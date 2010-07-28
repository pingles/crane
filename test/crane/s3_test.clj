(ns crane.s3-test
  (:use clojure.test
	crane.config
	crane.s3))

(def creds-home "/home/bradford/Dropbox/aws/")
(def test-bucket "learner")

(deftest write-and-read-clj
  (let [_ (System/setProperty "org.xml.sax.driver" "org.xmlpull.v1.sax2.Driver")
	c (s3-connection (creds creds-home))
	clj {:a 1}]
    (println (buckets c))))
    ;; 	_ (s3-byte-put c test-bucket "test" clj)
    ;; 	read-clj (s3-byte-get c test-bucket "test")]
    ;; (is (= read-clj clj))))
	