<(ns
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
 (:use clojure.contrib.shell-out)
 (:use clojure.contrib.duck-streams)
 (:use clojure.contrib.java-utils)
 (:use clj-ssh.ssh)
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
    (with-ssh-agent []
      (add-identity (:private-key-path creds))
      (let [inst (:public (attributes instance))
            session (session inst :username "root" :strict-host-key-checking :no)]
        (while (not (connected? session))
          (try (connect session)
               (catch com.jcraft.jsch.JSchException e
                 (println (str "waiting for hadoop-machine-session: " inst))
                 (Thread/sleep 1000)
                 (hadoop-machine-session instance config))))))))

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

;; push fn which takes either a [session] or a collection of
;; [session-from-to] tuples

(defn push
  ([session from to] (push [[session from to]]))
  ([session-from-to]
     (doall (pmap (fn [[session from to]]
                    (sftp session :put from to))))))


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
    (ssh (name-session ec2 config :in (:namenode-cmd cmds))
    (ssh (master-session ec2 config) :in (:jobtracker-cmd cmds))
    (dorun (pmap
            #(ssh % :in (:tasktracker-cmd cmds)) (slave-sessions ec2 config))))))

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