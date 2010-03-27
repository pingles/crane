(ns
 #^{:doc
    "crane.cluster doc
-all 'ec2' arguments require an instance of Jec2 class.
-'config' arguments are a map of configuration keys vals #^{}
-'cluster-name' is (:group config) #^String
-currently required options in config {} are:

:image         ;;your aws ec2 image 
:instance-type ;;desired instance type
:group         ;;cluster group; will be the group with slaves
:instances     ;;number of slaves, does not include jobtracker, namenode
:creds         ;;path to dir with file containing  aws creds;
                 creds.clj should be a map, with vals being aws creds
:jar-files     ;;jar files to push to master, must have socket repl jar
                 and project jar w/ dependencies
                 [[from to] [from to]]
:hadooppath    ;;path to hadoop dir on remote machine
:hadoopuser    ;;hadoop user on remote machine
:mapredsite    ;;path to mapred-site.xml template
:coresite      ;;path to core-site.xml template
:hdfssite      ;;path to hdfs-site.xml template

Optional keys in config {}

:zone          ;;ec2 cluster location
:push          ;;vector of strings, [[from to] [from to]]"}
 
 crane.cluster
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
 (:use crane.remote-repl)
 (:import java.io.File)
 (:import java.util.ArrayList)
 (:import java.net.Socket))

(defn parse-str-xml [s]
  (parse (new org.xml.sax.InputSource
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

(defn find-namenode [ec2 cluster-name]
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

(defn namenode [ec cluster-name]
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

(defn launch-namenode-machine [ec2 config]
  (let
    [cluster-name (:group config)
    master-conf (merge config {:group (cluster-nn-name cluster-name)})]
    (ec2-instance ec2 master-conf)))

(defn launch-slave-machines
"launch n hadoop slaves as specified by :instances in conf"
  [ec2 conf]
  (ec2-instances ec2 conf))

(defn get-slaves-str [slaves]
  (let
    [slave-ips (map #(:private (attributes %)) slaves)]
    (apply str (interleave slave-ips (repeat "\n")))))

(defn hadoop-machine-session
"returns connected session to instance"
  [instance config]
  (let
    [creds (creds (:creds config))]
    (block-until-connected
  (session
   (:private-key-path creds)
   (:hadoopuser config)
   (:public (attributes instance))))))

(defn make-classpath [config]
  (apply str (:hadooppath config) "/conf:"
             (interpose ":" (map
                             (fn [[x y]] y)
                             (:jar-files config)))))

(defn socket-repl
"Socket class, connected to master(jobtracker) remote repl"
  [#^String master]
  (Socket. master 8080))

;;TODO remove config files and launch cmds if not necessary eg: some people don't use hdfs

(defn repl-cmd
  [config sess]
  (future [] (sh! (exec-channel sess)
                  (str "java -Xmx4096m -cp "
                  (make-classpath config)
                  " swank.swank"))))

(defn hadoop-conf
"creates configuration, and remote shell-cmd map."
  [config]
  {:slaves-file (str (:hadooppath config) "/conf/slaves")
   :coresite-file (str (:hadooppath config) "/conf/core-site.xml")
   :hdfssite-file (str (:hadooppath config) "/conf/hdfs-site.xml")
   :mapredsite-file (str (:hadooppath config) "/conf/mapred-site.xml")
   :namenode-cmd (str "cd " (:hadooppath config) " && bin/hadoop namenode -format && bin/start-dfs.sh")
   :jobtracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start jobtracker")
   :tasktracker-cmd (str "cd " (:hadooppath config) " && bin/hadoop-daemon.sh start tasktracker")
   :hdfs-site (slurp (:hdfssite config))})

(defn master-session
  [ec2 config]
  (hadoop-machine-session
   (find-master ec2 (:group config))
   config))

(defn name-session [ec2 config]
 (hadoop-machine-session
  (find-namenode ec2 (:group config))
  config))

(defn slave-sessions [ec2 config]
  (map #(hadoop-machine-session % config)
       (find-instances
         running?
         (find-reservations ec2 (:group config)))))

(defn all-sessions [ec2 config]
  (conj (slave-sessions ec2 config)
        (master-session ec2 config)
        (name-session ec2 config)))

(defn push-mapred [ec2 config]
  (let [mapred-file (create-mapred-site (:mapredsite config)
                                        (str (.getPrivateDnsName
                                              (find-master ec2
                                               (:group config)))
                                         ":9000"))]
    (map
     #(push % mapred-file (:mapredsite-file (hadoop-conf config)))
     (all-sessions ec2 config))))

(defn push-coresite [ec2 config]
  (let [coresite-file (create-core-site (:coresite config)
                                        (str (.getPrivateDnsName
                                              (find-namenode ec2
                                               (:group config)))))]
    (map
     #(push % coresite-file (:coresite-file (hadoop-conf config)))
     (all-sessions ec2 config))))

(defn push-hdfs [ec2 config]
  (let [hdfs-file (:hdfs-site (hadoop-conf config))]
    (map
     #(push % hdfs-file (:hdfssite-file (hadoop-conf config)))
     (all-sessions ec2 config))))

(defn push-slaves [ec2 config]
  (let [slaves-file (get-slaves-str
                     (running-instances ec2 (:group config)))
        sessions (list (master-session ec2 config)
                       (name-session ec2 config))]
    (map
     #(push % slaves-file (:slaves-file (hadoop-conf config)))
     sessions)))

(defn push-files [ec2 config]
  (let [sess (master-session ec2 config)
        files (map
                (fn [[x y]] [(file x) y])
                (concat (:jar-files config) 
                        (:push config)))]
    (map
      (fn [[x y]] (push sess x y))
      files)))

(defn push-all [ec2 config]
  (map
    #(apply % [ec2 config])
    [push-mapred 
     push-coresite 
     push-hdfs 
     push-slaves 
     push-files ]))

(defn start-services [ec2 config]
  (let [cmds (hadoop-conf config)]
    (sh! (shell-channel
          (name-session ec2 config))
         (:namenode-cmd cmds))
    (sh! (shell-channel
          (master-session ec2 config))
          (:jobtracker-cmd cmds))
    (dorun (pmap
            #(sh! (shell-channel %) (:tasktracker-cmd cmds))
            (slave-sessions ec2 config)))
    (repl-cmd config (master-session ec2 config))))

(defn launch-machines [ec2 config]
  (doall (pmap
    #(% ec2 config)
    [launch-jobtracker-machine
     launch-namenode-machine
     launch-slave-machines]))
  (Thread/sleep 1000))

(defn launch-cluster [ec2 config]
  (doall (map #(% ec2 config)
  [launch-machines
  push-all
  start-services])))

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
"returns string that will start dfs and tasktracker daemons
when used in a shell channel"
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
     conf-map (hadoop-conf config)
     masters-sess (master-sessions (master-ips ec2 config) config)
     nodes-sessions (map
                      #(hadoop-machine-session % config)
                      new-nodes)]
  (dorun (map
           #(scp % mapred-site (:mapredsite-file conf-map))
           nodes-sessions))
  (dorun (map
           #(scp % core-site (:coresite-file conf-map))
           nodes-sessions))
  (dorun (map
           #(scp % (:hdfs-site conf-map) (:hdfssite-file conf-map))
           nodes-sessions))
  (dorun (pmap
           #(sh! (shell-channel %) (slaves-cmd nodes-str config))
           masters-sess))
  (dorun (pmap
           #(sh! (shell-channel %) (start-daemons-cmd config))
           nodes-sessions))))
