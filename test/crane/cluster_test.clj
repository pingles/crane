(ns crane.cluster-test
  (:use clojure.test
        crane.cluster))

(def conf-map
     {:hadooppath "/foo/bar"
      :jar-files ["/foo/bar.jar" "/bar/bizzle.jar"
                  "/foo/baz.jar" "/bar/bat.jar"]})

(deftest test-make-classpath
  (is (= "/foo/bar/conf:/bar/bizzle.jar:/bar/bat.jar"
         (make-classpath conf-map))))