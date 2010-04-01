(ns crane.partfiles
 (:use crane.s3)
 (:use clojure.contrib.str-utils)
 (:use clojure.contrib.duck-streams))

;;TODO: move to cascading-clojrue once the aws stuff becomes open source.

(defn next-split [s] (.indexOf s "\t"))
(defn first-word [s]
  (let [i (next-split s)]
    (if (= i -1) s
	(.substring s 0 i))))

;;use java stream tokenizer or something?
(defn rest-words [s]
  (let [i (next-split s)]
    (if (= i -1) nil
	(.substring s (+ i 1)))))

(def partfile-reader (comp read-string rest-words))
"
 (s3->clj s3 root-bucket rest partfile-reader))
"

(defn multiline-partfile-reader [f]
  (map 
   partfile-reader 
   (line-seq (reader f))))

(defn from-lines
"
 (s3->clj s3 root-bucket rest from-lines))
"
[#^String s]
  (map 
   read-string
   (.split s "\n")))

(defn partfile-dirs->clj [s3 n root-bucket rest]
  (for [x (range 1 (+ 1 n))]
    (partfiles->clj s3 root-bucket (str-join "/" [rest x]))))