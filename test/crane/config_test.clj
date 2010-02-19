(ns crane.config-test
  (:use clojure.test
        crane.config))

(deftest test-replace-all
  (is (= "foo baz"
	       (replace-all "foo bar" [["bar" "baz"]])))
  (is (= "foo baz bag"
	       (replace-all "foo bar biz" [["bar" "baz"] ["biz" "bag"]]))))