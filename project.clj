(defproject crane "1.0-SNAPSHOT"
  :description "heavy lifting for ec2"
  :dependencies [[org.clojure/clojure "1.2.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.2.0-SNAPSHOT"]
                 [net.java.dev.jets3t/jets3t "0.7.4"]
                 [com.google.code.typica/typica "1.5.2a"]
                 [jline "0.9.94"]
                 [remote-repl "0.0.1-SNAPSHOT"]
		 [clj-ssh "0.2.0-SNAPSHOT"]
		 [clj-serializer "0.1.0"]]
  :dev-dependencies [[swank-clojure "1.2.1"]
		     [lein-clojars "0.5.0"]])