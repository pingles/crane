(ns crane.ssh2-test
  (:use clojure.test
        crane.ssh2)
  (:require (clojure.contrib [duck-streams :as ds] [java-utils :as ju]))
  (:import (com.jcraft.jsch Session)))


(def key-path (str (System/getProperty "user.home") "/.ssh/id_rsa"))
(def user     (System/getProperty "user.name"))
(def host     "localhost")

(def tmp-dir-path (str (System/getProperty "java.io.tmpdir") "/crane-test"))

(defmacro with-tmp-dir [tmp-dir-path & body]
  `(let [tmp-dir# (ju/file ~tmp-dir-path)]
     (or (.exists tmp-dir#) (.mkdir tmp-dir#))
     (try
       ~@body
       (finally
         (ju/delete-file-recursively tmp-dir# true)))))

(defn- local-session []
  (session key-path user host))

(deftest test-session
  (is (instance? Session (session key-path user host))))

(deftest test-to-stream
  (is (= "foo" (ds/slurp* (to-stream "foo"))))
  (is (= (ds/slurp* "README") (ds/slurp* (to-stream (ju/file "README"))))))

(deftest test-scp
  (let [from-path (str tmp-dir-path "/from")
        to-path   (str tmp-dir-path "/to")]
    (with-tmp-dir tmp-dir-path
      (ds/write-lines from-path ["content"])
      (with-connection [sess (local-session)]
        (scp sess from-path to-path))
      (is (.exists (ju/file to-path))))))

(deftest test-put
  (let [tmp-inner-dir-path (str tmp-dir-path "/dir")
        from-path          (str tmp-dir-path "/from")
        to-path            (str tmp-inner-dir-path "/from")]
    (with-tmp-dir tmp-dir-path
      (with-tmp-dir tmp-inner-dir-path
        (ds/write-lines from-path ["content"])
        (with-connection [sess (local-session)]
          (put sess (ju/file from-path) tmp-inner-dir-path))
        (is (.exists (ju/file to-path)))))))

; should actually check permissions [mrm]
(deftest test-chmod
  (let [touch-file-path (str tmp-dir-path "/foo")]
    (with-tmp-dir tmp-dir-path
      (ds/write-lines touch-file-path ["content"])
      (with-connection [sess (local-session)]
        (chmod sess 777 touch-file-path)))))

(deftest test-ls
  (let [touch-file-path (str tmp-dir-path "/foo")]
    (with-tmp-dir tmp-dir-path
      (ds/write-lines touch-file-path ["content"])
      (with-connection [sess (local-session)]
        (is (= ["." ".." "foo"]
               (map #(.getFilename %) (ls sess tmp-dir-path))))))))

(deftest test-sh!-exec
  (with-tmp-dir tmp-dir-path
    (let [touch-file (str tmp-dir-path "/foo")]
      (with-connection [sess (local-session)]
        (is (= "" (sh! (exec-channel sess) (str "touch " touch-file))))
        (is (= (str "foo\n")
               (sh! (exec-channel sess) (str "ls " tmp-dir-path))))))))

(deftest test-sh!-shell
  (with-tmp-dir tmp-dir-path
    (let [touch-file-path (str tmp-dir-path "/foo")]
      (with-connection [sess (local-session)]
        (sh! (shell-channel sess) (str "touch " touch-file-path))
        (is (= (str "foo\n")
               (sh! (exec-channel sess) (str "ls " tmp-dir-path))))))))
