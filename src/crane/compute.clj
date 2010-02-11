(ns
#^{:doc
"
a lib for interacting with jclouds ComputeService.

Current supported services are:
   [ec2, rimuhosting, terremark, vcloud, hostingdotcom]

Here's an example of getting some compute configuration from rackspace:

  (ns crane.jclouds
    (:use crane.compute
          clojure.contrib.pprint))

  (def user \"username\")
  (def password \"password\")
  (def compute-name \"cloudservers\")

  (def compute (compute-context compute-name user password))

  (pprint (locations compute))
  (pprint (images compute))
  (pprint (nodes compute))
  (pprint (sizes compute))

"}
  crane.compute

  (:use clojure.contrib.duck-streams)
  (:import java.io.File
           org.jclouds.compute.domain.OsFamily
           org.jclouds.domain.Location
           org.jclouds.compute.ComputeService
           org.jclouds.compute.ComputeServiceContext
           org.jclouds.compute.ComputeServiceContextFactory
           org.jclouds.logging.log4j.config.Log4JLoggingModule
           org.jclouds.ssh.jsch.config.JschSshClientModule
           org.jclouds.enterprise.config.EnterpriseConfigurationModule
           org.jclouds.compute.domain.Template
           org.jclouds.compute.domain.TemplateBuilder
           org.jclouds.compute.domain.ComputeMetadata
           org.jclouds.compute.domain.Size
           org.jclouds.compute.domain.Image))

(def module-lookup
     {:log4j org.jclouds.logging.log4j.config.Log4JLoggingModule
      :ssh org.jclouds.ssh.jsch.config.JschSshClientModule
      :enterprise org.jclouds.enterprise.config.EnterpriseConfigurationModule})

(defn modules
  "Build a list of modules suitable for passing to compute-context"
  [& modules]
  (.build #^com.google.common.collect.ImmutableSet$Builder
	  (reduce #(.add #^com.google.common.collect.ImmutableSet$Builder %1
			 (.newInstance #^Class (%2 module-lookup)))
		  (#^com.google.common.collect.ImmutableSet$Builder
		   com.google.common.collect.ImmutableSet/builder)
		  modules)))

(defn compute-context
  ([s a k]
     (compute-context s a k (modules :log4j :ssh :enterprise)))
  ([#^String s #^String a #^String k #^com.google.common.collect.ImmutableSet m]
     (.createContext (new ComputeServiceContextFactory) s a k m)))

(defn locations [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getLocations (.getComputeService compute)))

(defn nodes
  ([#^org.jclouds.compute.ComputeServiceContext compute]
    (.getNodes (.getComputeService compute) ))
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
    (.getNodesWithTag (.getComputeService compute) tag )))

(defn images [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getImages (.getComputeService compute)))

(defn sizes [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getSizes (.getComputeService compute)))

(defn default-template [#^org.jclouds.compute.ComputeServiceContext compute]
  (.. compute (getComputeService) (templateBuilder)
    (osFamily OsFamily/UBUNTU)
    smallest
    (options (org.jclouds.compute.options.TemplateOptions$Builder/authorizePublicKey (slurp (str (. System getProperty "user.home") "/.ssh/id_rsa.pub"))))
    build))

(defn run-nodes
  ([#^org.jclouds.compute.ComputeServiceContext compute tag count]
    (.runNodesWithTag (.getComputeService compute) tag count (default-template compute)))
  ([#^org.jclouds.compute.ComputeServiceContext compute tag count template]
    (.runNodesWithTag (.getComputeService compute) tag count template)))

(defn run-node
  ([#^org.jclouds.compute.ComputeServiceContext compute tag]
    (run-nodes compute tag 1))
  ([#^org.jclouds.compute.ComputeServiceContext compute tag template]
    (run-nodes compute tag 1 template)))

(defn node-details [#^org.jclouds.compute.ComputeServiceContext compute node]
  (.getNodeMetadata (.getComputeService compute) node ))

(defn reboot-nodes
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
    (.rebootNodesWithTag (.getComputeService compute) tag )))

(defn reboot-node
  ([#^org.jclouds.compute.ComputeServiceContext compute #^org.jclouds.compute.domain.ComputeMetadata node]
    (.rebootNode (.getComputeService compute) node )))

(defn destroy-nodes
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
    (.destroyNodesWithTag (.getComputeService compute) tag )))

(defn destroy-node
  ([#^org.jclouds.compute.ComputeServiceContext compute #^org.jclouds.compute.domain.ComputeMetadata node]
    (.destroyNode (.getComputeService compute) node )))

