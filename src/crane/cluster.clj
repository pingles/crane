(ns crane.cluster
 (:require [clojure.zip :as zip])
 (:use clojure.contrib.seq-utils)
 (:use clojure.contrib.shell-out)
 (:use clojure.contrib.duck-streams)
 (:use clojure.contrib.java-utils)
 (:use crane.ssh2)
 (:use crane.ec2)
 (:use crane.config)
 (:use clojure.xml)
 (:use crane.utils)
 (:import java.io.File)
 (:import java.util.ArrayList))

(defn parse-str-xml [s] (parse (new org.xml.sax.InputSource
                               (new java.io.StringReader s))))
 (defn cluster-jt-name [cluster-name]
   (str cluster-name "-jobtracker"))

(defn cluster-nn-name [cluster-name]
  (str cluster-name "-namenode"))

(defn find-master
"find master finds the master for a given cluster-name. 
if the cluster is named foo, then the master is named foo-jobtracker. 
be advised that find master returns nil if the master has been reserved but is not in running state yet."
  [ec2 cluster-name]
  (first
   (running-instances ec2 (cluster-jt-name cluster-name))))

(defn find-namenode
  [ec2 cluster-name]
  (first
   (running-instances ec2 (cluster-nn-name cluster-name))))

(defn master-already-running? [ec2 cluster]
 (if (find-master ec2 cluster)
      true
      false))

(defn cluster-running? [ec2 cluster n]
  (and (master-already-running? ec2 cluster)
       (already-running? ec2 cluster n)))

(defn cluster-instance-ids [ec2 cluster]
  (instance-ids 
   (concat
    (find-reservations ec2 cluster)
    (find-reservations ec2 (cluster-jt-name cluster))
    (find-reservations ec2 (cluster-nn-name cluster)))))

(defn stop-cluster 
  "terminates the master and all slaves."
  [ec2 cluster]
  (terminate-instances ec2 (cluster-instance-ids ec2 cluster)))

(defn job-tracker-url [{host :public}]
  (str "http://" host ":50030"))

(defn name-node-url [{host :public}]
  (str "http://" host ":50070"))

(defn namenode
  [ec cluster-name]
  (name-node-url
   (attributes
    (find-namenode ec cluster-name))))

(defn tracker
"gets the url for job tracker."
  [ec cluster-name]
  (job-tracker-url
    (attributes
      (find-master ec cluster-name))))

(defn cluster-instance-ids [ec2 cluster]
  (instance-ids
   (concat
    (find-reservations ec2 cluster)
    (find-reservations ec2 (cluster-jt-name cluster)))))

(defn create-mapred-site [template-file tracker-hostport]
  (let [template-xml (zip/xml-zip (parse template-file))
        insertxml (parse-str-xml (str "<property><name>mapred.job.tracker</name><value>" tracker-hostport "</value></property>"))
        ]
        (render-xml (-> template-xml (zip/insert-child insertxml) zip/root))))

(defn create-core-site [template-file namenode-hostport]
  (let [template-xml (zip/xml-zip (parse template-file))
        insertxml (parse-str-xml (str "<property><name>fs.default.name</name><value>" "hdfs://" namenode-hostport "</value></property>"))
        ]
    (render-xml (-> template-xml (zip/insert-child insertxml) zip/root))))

(defn launch-jobtracker-machine [ec2 config]
  (let
    [cluster-name (:group config)
     master-conf (merge config {:group (cluster-jt-name cluster-name)})]
    (ec2-instance ec2 master-conf)))

(defn launch-namenode-machine
  [ec2 config]
  (let
    [cluster-name (:group config)
    master-conf (merge config {:group (cluster-nn-name cluster-name)})]
    (ec2-instance ec2 master-conf)))

(defn launch-slave-machines
  "launch n hadoop slaves as specified by :instances in conf
"
  [ec2 conf]
  (ec2-instances ec2 conf))

(defn get-slaves-str
  [slaves]
  (let
    [slave-ips (map #(:private (attributes %)) slaves)]
    (apply str (interleave slave-ips (repeat "\n")))
    ))

(defn hadoop-machine-session
  [instance config]
  (let
    [creds (creds (:creds config))]
    (block-until-connected
  (session
  (:private-key-path creds) (:hadoopuser config)
  (:public (attributes instance))))
    )
  )

;;TODO remove config files and launch cmds if not necessary eg: some people don't use hdfs

(defn hadoop-conf [config]
           {:slaves-file (str (:hadooppath config) "/conf/slaves")
            :coresite-file (str (:hadooppath config) "/conf/core-site.xml")
            :hdfssite-file (str (:hadooppath config) "/conf/hdfs-site.xml")
            :mapredsite-file (str (:hadooppath config) "/conf/mapred-site.xml")
            :namenode-cmd (str "cd " (:hadooppath config) " && bin/hadoop namenode -format && bin/start-dfs.sh")
            :jobtracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start jobtracker")
            :tasktracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start tasktracker")})

(defn launch-cluster [ec2 config]
  "Assumes you have all settings configured in your mapred-site except for jobtracker url
 
You need to set up an image that contains hadoop installed and all necessary permissions set up
Need to set:
:hadoopuser to the user that will run hadoop
:mapredsite to path to mapred-site template, and
:hadooppath to path of hadoop on your image

"
  (let
    [cluster-name (:group config)
     namenode (launch-namenode-machine ec2 config)     
     jobtracker (launch-jobtracker-machine ec2 config)
     slaves (launch-slave-machines ec2 config)
     slaves-str (get-slaves-str slaves)
     conf-map (hadoop-conf config)
     mapred-site (create-mapred-site (:mapredsite config) (str (.getPrivateDnsName jobtracker)
                                                               ":9000"))
     core-site (create-core-site (:coresite config) (str (.getPrivateDnsName namenode)))
     hdfs-site (slurp (:hdfssite config))
     namenode-session (hadoop-machine-session namenode config)
     master-session (hadoop-machine-session jobtracker config)
     slave-sessions (map #(hadoop-machine-session % config) slaves)]
    ;; TODO put dest path for push file src and dest.
    
    (prn master-session)
    (prn namenode-session)
    (prn slave-sessions)
    
    (prn "pushing files to master")
    (push master-session [(:source (:push test-conf))
                          (:dest (:push test-conf))])
    (prn "scp hdfs-site")
    (dorun
     (pmap #(scp % hdfs-site (:hdfssite-file conf-map))
           (flatten [namenode-session master-session slave-sessions])))
    (prn "scp slaves-file")
    (dorun
     (pmap #(scp % slaves-str (:slaves-file conf-map))
           [master-session namenode-session]))
    (prn mapred-site)
    (dorun
     (pmap #(scp % mapred-site (:mapredsite-file conf-map))
           (flatten (cons [namenode-session master-session] slave-sessions))))
    (prn "scp core-site")
    (dorun
     (pmap #(scp % core-site (:coresite-file conf-map))
           (flatten (cons [namenode-session master-session] slave-sessions))))
    (prn "starting services... ")
    (sh! (shell-channel namenode-session) (:namenode-cmd conf-map))
    (sh! (shell-channel master-session) (:jobtracker-cmd conf-map))
    (dorun
     (pmap #(sh! (shell-channel %) (:tasktracker-cmd conf-map)) slave-sessions))
    {:ssh master-session :repl 'foo}))

;;TODO parralelize launching instances?
;; clean up massive let binding, remove duplicate code
;; pull add slaves out of cluster.clj?

(defn master-ips
"returns list of namenode and jobtracker external ip addresses"
  [ec2 config]
  (list (:public (attributes (find-master  ec2 (:group config))))
        (:public (attributes (find-namenode  ec2 (:group config))))))

(defn nn-private
"returns namenode private address as string"  
  [ec2 config]
  (:private (attributes (find-namenode ec2 (:group config)))))

(defn jt-private
"returns jobtracker private address as string"  
  [ec2 config]
  (str (:private (attributes (find-master ec2 (:group config)))) ":9000"))

(defn master-sessions
"bring up ssh sessions to master ips.  For slave file append"
  [addresses config]
  (let [cred (creds (:creds config))]
  (map
   #(block-until-connected (session (:private-key-path cred)
                                    (:hadoopuser config) %))
   addresses)))

(defn slaves-cmd
"command to redirect a list of slaves to the conf/slaves file on masters"
  [slaves-str config]
  (str "echo -e '" slaves-str "' > " (:hadooppath config) "/conf/slaves")) 

(defn start-daemons-cmd 
"returns string that will start dfs and tasktracker daemons"
  [config]
  (str "cd " 
       (:hadooppath config)
       " && bin/hadoop-daemon.sh start datanode"
       " && bin/hadoop-daemon.sh start tasktracker"))

;;workflow for adding nodes
;; launch slaves in  slaves group
;; append slaves file on jobtracker and namenode with new slaves
;; scp config files to new slaves
;; start services on slave nodes

(defn add-nodes
  "adds num of nodes to already running cluster, in (:group config)
Requires the same arguments "
  [ec2 num config]
  (let 
      [conf (merge config
                   {:instances (+ (count (running-instances ec2 (:group config)))
                                  num)})
     new-nodes (launch-slave-machines ec2 conf)
     nodes-str (get-slaves-str new-nodes)
     mapred-site (create-mapred-site (:mapredsite config) (jt-private ec2 config))
     core-site (create-core-site (:coresite config) (nn-private ec2 config))
     hdfs-site (slurp (:hdfssite config))
     coresite-file (str (:hadooppath config) "/conf/core-site.xml")
     hdfssite-file (str (:hadooppath config) "/conf/hdfs-site.xml")
     mapredsite-file (str (:hadooppath config) "/conf/mapred-site.xml")
     masters-sess (master-sessions (master-ips ec2 config) config)
     nodes-sessions (map
                     #(hadoop-machine-session % config)
                     new-nodes)]
  (dorun (map
           #(scp % mapred-site mapredsite-file)
           nodes-sessions))
  (dorun (map
           #(scp % core-site coresite-file)
           nodes-sessions))
  (dorun (map
           #(scp % hdfs-site hdfssite-file)
           nodes-sessions))  
  (dorun (pmap #(sh! (shell-channel %) (slaves-cmd nodes-str config)) masters-sess))
  (dorun (pmap #(sh! (shell-channel %) (start-daemons-cmd config)) nodes-sessions))))
