(ns 
#^{:doc 
"
a lib for interacting with s3.
"}
crane.s3
  (:use clj-serializer.core)
  (:import (clj_serializer Serializer)
           (java.io DataOutputStream ByteArrayOutputStream
                    DataInputStream ByteArrayInputStream))
  (:require [clojure.contrib.duck-streams :as ds])
  (:import java.io.File)
  (:import org.jets3t.service.S3Service)
  (:import org.jets3t.service.impl.rest.httpclient.RestS3Service)
  (:import org.jets3t.service.model.S3Object)
  (:import org.jets3t.service.security.AWSCredentials)
  (:import org.jets3t.service.utils.ServiceUtils))

(defn s3-connection 
  ([{access-key :key secret-key :secretkey}] 
     (s3-connection access-key secret-key))
  ([k sk] (RestS3Service. (AWSCredentials. k sk))))

(defn buckets [s3] (.listAllBuckets s3))

(defn objects
"
http://jets3t.s3.amazonaws.com/api/index.html

list the objects in a bucket:
s3 bucket -> objects

list the objects in a bucket at a key:
s3 bucket key -> objects

example: (pprint 
          (objects 
           (s3-connection flightcaster-creds) 
            \"somebucket\" \"some-folder\"))
"
  ([s3 bucket-name] 
     (.listObjects s3 (.getBucket s3 bucket-name)))
  ([s3 bucket-root rest] 
     (.listObjects s3 (.getBucket s3 bucket-root) rest nil)))

(defn folder? [o]
  (or (> (.indexOf o "$folder$") 0)
      (> (.indexOf o "logs") 0)))

(defn without-folders [c]
  (filter #(not (folder? (.getKey %))) c))

(defn create-bucket [s3 bucket-name]
  (.createBucket s3 bucket-name))

(defn mkdir [path] (.mkdir (File. path)))

(defn put-file
  ([s3 bucket-name file]
     (let [bucket (.getBucket s3 bucket-name)
	   s3-object (S3Object. bucket file)]
       (.putObject s3 bucket s3-object)))
  ([connection bucket-name key file]
     (let [bucket (.getBucket connection bucket-name)
	   s3-object (S3Object. bucket file)]
       (.setKey s3-object key)
       (.putObject connection bucket s3-object))))

(defn files [dir]
  (for [file (file-seq (File. dir))
	      :when (.isFile file)]
	     file))

(defn put-dir
"create a new bucket b. copy everything from dir d to bucket b."
[s3 d b]
    (create-bucket s3 b)
    (dorun 
      (for [f (files d)]
	     (put-file s3 b f))))

(defn put-str
 [s3 bucket-name key data]
  (let [bucket (.getBucket s3 bucket-name)
        s3-object (S3Object. bucket key data)]
    (.putObject s3 bucket s3-object)))

(defn put-clj [s3 bucket-name key clj]
  (let [bucket (.getBucket s3 bucket-name)  
        s3-object (S3Object. bucket key)
	_ (.setDataInputStream s3-object (ByteArrayInputStream. (serialize clj)))]
    (.putObject s3 bucket s3-object)))

(defn obj-to-str [obj]
  (ServiceUtils/readInputStreamToString (.getDataInputStream obj) "UTF-8"))

(defn get-str [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)]
    (obj-to-str obj)))

(defn s3-deserialize [is eof-val]
  (let [dis (DataInputStream. is)]
    (try 
     (Serializer/deserialize dis eof-val)
  (finally 
   (.close dis)))))

(defn get-clj [s3 bucket-name key]
  (let [bucket (.getBucket s3 bucket-name)
        obj (.getObject s3 bucket key)]
    (s3-deserialize
      (.getWrappedInputStream 
      (.getDataInputStream obj))
     (Object.))))

(defn get-dir
"read the object(s) at the root/rest s3 uri into memory using a s3/get-foo fn."
 [s3 root-bucket rest rdr]
  (let [files (without-folders (objects s3 root-bucket rest))]
    (map #(rdr s3 root-bucket (.getKey %)) files)))

