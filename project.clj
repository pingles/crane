(defproject crane "1.0-SNAPSHOT"
  :description "heavy lifting for ec2"
  :dependencies [[org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                 [net.java.dev.jets3t/jets3t "0.7.1"]
                 [com.google.code.typica/typica "1.5.2a"]
                 [org.jclouds/jclouds-blobstore "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-compute "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-azure "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-atmos "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-aws "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-rackspace "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-terremark "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-hostingdotcom "1.0-SNAPSHOT"]
                 [org.jclouds/jclouds-rimuhosting "1.0-SNAPSHOT"]
                 [com.jcraft/jsch "0.1.42"]
                 [jline "0.9.94"]]
  :dev-dependencies [[org.clojure/swank-clojure "1.0"]])
