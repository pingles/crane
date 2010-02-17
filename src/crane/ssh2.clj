(ns
  #^{:doc
  "Wraps the ssh2 implimentation from jsch.
  http://www.jcraft.com/jsch/examples/
  
  exec:
  http://www.jcraft.com/jsch/examples/Exec.java
  http://code.google.com/p/securechannelfacade/
  http://seancode.blogspot.com/2008/02/jsch-scp-file-in-java.html
  
  shell:
  http://www.jcraft.com/jsch/examples/Shell.java
  http://blog.james-carr.org/2006/07/11/ssh-over-java/
  http://www.mailinglistarchive.com/jsch-users@lists.sourceforge.net/msg00069.html
  http://www.mailinglistarchive.com/jsch-users@lists.sourceforge.net/msg00062.html

  sftp:
  http://www.jcraft.com/jsch/examples/ScpTo.java
  http://show.docjava.com/book/cgij/jdoc/net/ssh/jsch/ChannelSftp.html

  Note for shell out that wildcards and such are not expanded by the
  shell-out/sh because it just uses runtime-exec. In order to run a shell to
  evaluate args before runing the programs, use sh as follows:
  http://www.coderanch.com/t/423573/Java-General/java/Passing-wilcard-Runtime-exec-command#1870031"}
  crane.ssh2
  (:use (clojure.contrib duck-streams shell-out))
  (:import (java.io BufferedReader InputStreamReader PipedOutputStream
                    PipedInputStream ByteArrayInputStream File)
           (com.jcraft.jsch JSch Session
                            Channel ChannelShell ChannelExec ChannelSftp)))

(defmacro with-connection [[conn-sym conn-form] & body]
  `(let [~conn-sym ~conn-form]
     (try
       (or (.isConnected ~conn-sym) (.connect ~conn-sym))
       ~@body
       (finally
         (.disconnect ~conn-sym)))))

(defn block-until-connected [#^Session session]
  (while (not (.isConnected session))
    (try (.connect session)
      (catch com.jcraft.jsch.JSchException e
        (println  "Waiting for ssh to come up" e)
        (Thread/sleep 10000)
        (block-until-connected session))))
  (println  "ssh is up.")
  session)

(defn session
  "Start a new jsch session and connect."
  [#^String private-key #^String username #^String hostname]
  (let [jsch    (doto (JSch.) (.addIdentity private-key))
        session (.getSession jsch username hostname 22)
        config  (doto (java.util.Properties.)
                  (.setProperty "StrictHostKeyChecking" "no"))]
    (doto session
      (.setConfig config))))

(defn sftp-channel [#^Session session]
  (.openChannel session "sftp"))

(defn exec-channel [#^Session session]
  (.openChannel session "exec"))

(defn shell-channel [#^Session session]
  (.openChannel session "shell"))

(defn to-stream [source]
  (if (string? source)
    (java.io.ByteArrayInputStream. (.getBytes #^String source "UTF-8"))
    (java.io.FileInputStream. #^File source)))

;;TODO: should we just change these all to have a ! at the end to match that
;;      remote execution api?
;;TODO: how to merge scp and put?
(defn scp
  "Copy a file to a remote dir and new filename."
  [#^Session session source #^String destination]
  (with-connection [channel (sftp-channel session)]
    (.put channel (to-stream source) destination)))

(defn put
  "Copy a file to a remote dir."
  [#^Session session #^File source #^String destination]
  (with-connection [channel (sftp-channel session)]
    (.cd channel destination)
    (.put channel (to-stream source) (.getName source))))

(defn chmod
  "chmod permissions on a remote dir."
  [#^Session session #^Integer permissions #^String remote-path]
  (with-connection [channel (sftp-channel session)]
    (.chmod channel permissions remote-path)))

(defn ls
  "ls a remote dir."
  [#^Session session #^String remote-path]
  (with-connection [channel (sftp-channel session)]
    (.ls channel remote-path)))

(def timeout 5000)

;;TODO: worth using a uuid?
(def terminator "zDONEz")
(defn terminated [c] (str c "; echo" terminator "\n"))

(defmulti read-command-output class)

(defmethod read-command-output ChannelExec
  [channel]
  (let [in       (.getInputStream channel)
        bos      (java.io.ByteArrayOutputStream.)
        end-time (+ (System/currentTimeMillis) timeout)]
    (while  (and (< (System/currentTimeMillis) end-time)
                 (.isConnected channel))
       (copy in bos))
  (str bos)))

;;TODO: still need to remove garbage at the end after the trailing \n
;; "read up to and excluding the terminator, and drop the terminator.
;;  you will see the terminator twice, once showing the command executed at the
;;  prompt, and once after the output from the command. thus, the output string
;;  is contained between the terminator markers."
(defmethod read-command-output ChannelShell
  [channel]
  (let [in      (reader (.getInputStream channel))
        builder (StringBuilder.)]
    ((fn build [marked]
       (let [l (.readLine in)]
         (doto builder (.append l) (.append "\n"))
         (if (.contains l terminator)
           (if marked
             (let [s   (str builder)
                   out (.substring s (+ (.indexOf s terminator)
                                        (.length (str terminator "\n"))))]
               (.substring out 0 (+ (.indexOf out terminator))))
             (build true))
           (build marked)))) false)))

(defmulti write-command (fn [ch cmd] (class ch)))

(defmethod write-command ChannelExec
  [channel command]
  (.setCommand channel command)
  (.setInputStream channel nil))

(defmethod write-command ChannelShell
  [channel command]
  (.setInputStream
    channel
    (ByteArrayInputStream.
      (.getBytes (terminated command)))))

;;TODO: this might turn into a multimethod if some cases (like exec when u dont
;;      need to the oepn the channel beforehand since it is always transient, 
;;      they might want to take session rather than channel as first praram.
;;TODO: stuff to make persistent shell chennel work:
;;-can't close every time.
;;-need to hold on to the same input channel?        
;;  -in that case read-command-output needs to be taking an input stream rather
;;   than a channel?
(defn sh!
  "Execute a shell command on a remote process. The way the command is written
   to the stream is determined by whether it is an exec or shell channel, but
   the way the output is read is consistent."
  [#^Channel channel #^String command]
  (write-command channel command)
  (with-connection [_ channel]
    (read-command-output channel)))

;;TODO: efficient parallel transfer of many small files, e.g. via zip?
(defn push
  "Takes a coll of from->to paths, turns them into tuples, and scps from->to.
   The transfers are executed in parallel via pmap, which will create agents
   that may need to be shutdown."
  [sess paths]
   (doall (pmap (fn [[from to]] (scp sess from to)) (partition 2 paths))))
