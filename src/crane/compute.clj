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
           org.jclouds.domain.Location
           (org.jclouds.compute ComputeService
                                ComputeServiceContext
                                ComputeServiceContextFactory)
           (org.jclouds.compute.domain Template
                                       TemplateBuilder
                                       ComputeMetadata
                                       Size
                                       OsFamily
                                       Image)))

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
  "Create a logged in context."
  ([s a k]
     (compute-context s a k (modules :log4j :ssh :enterprise)))
  ([#^String s #^String a #^String k #^com.google.common.collect.ImmutableSet m]
     (.createContext (new ComputeServiceContextFactory) s a k m)))

(defn locations
  "Retrieve the available compute locations for the compute context."
  [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getLocations (.getComputeService compute)))

(defn node-list [node-set]
  (map #(.getValue %) node-set))

(defn nodes
  "Retrieve the existing nodes for the compute context."
  ([#^org.jclouds.compute.ComputeServiceContext compute]
     (node-list (.getNodes (.getComputeService compute))))
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
     (node-list (.getNodesWithTag (.getComputeService compute) tag))))

(defn images
  "Retrieve the available images for the compute context."
  [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getImages (.getComputeService compute)))

(defn sizes
  "Retrieve the available node sizes for the compute context."
  [#^org.jclouds.compute.ComputeServiceContext compute]
  (.getSizes (.getComputeService compute)))

(defn default-template [#^org.jclouds.compute.ComputeServiceContext compute]
  (.. compute (getComputeService) (templateBuilder)
    (osFamily OsFamily/UBUNTU)
    smallest
    (options
     (org.jclouds.compute.options.TemplateOptions$Builder/authorizePublicKey
      (slurp (str (. System getProperty "user.home") "/.ssh/id_rsa.pub"))))
    build))

(defn run-nodes
  "Create the specified number of nodes using the default or specified
   template."
  ([#^org.jclouds.compute.ComputeServiceContext compute tag count]
    (.runNodesWithTag
     (.getComputeService compute) tag count (default-template compute)))
  ([#^org.jclouds.compute.ComputeServiceContext compute tag count template]
    (.runNodesWithTag
     (.getComputeService compute) tag count template)))

(defn run-node
  "Create the specified number of nodes using the default context."
  ([#^org.jclouds.compute.ComputeServiceContext compute tag]
    (run-nodes compute tag 1))
  ([#^org.jclouds.compute.ComputeServiceContext compute tag template]
    (run-nodes compute tag 1 template)))

(defn node-details
  "Retrieve the node metadata"
  [#^org.jclouds.compute.ComputeServiceContext compute node]
  (.getNodeMetadata (.getComputeService compute) node ))

(defn reboot-nodes
  "Reboot all the nodes with the given tag."
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
    (.rebootNodesWithTag (.getComputeService compute) tag )))

(defn reboot-node
  "Reboot a given node."
  ([#^org.jclouds.compute.ComputeServiceContext compute
    #^org.jclouds.compute.domain.ComputeMetadata node]
    (.rebootNode (.getComputeService compute) node )))

(defn destroy-nodes
  "Destroy all the nodes with the given tag."
  ([#^org.jclouds.compute.ComputeServiceContext compute #^String tag]
    (.destroyNodesWithTag (.getComputeService compute) tag )))

(defn destroy-node
  "Destroy a given node."
  ([#^org.jclouds.compute.ComputeServiceContext compute
    #^org.jclouds.compute.domain.ComputeMetadata node]
    (.destroyNode (.getComputeService compute) node )))

(defmacro state-predicate [node state]
  `(= (.getState ~node) (. org.jclouds.compute.domain.NodeState ~state)))

(defn pending?
  "Predicate for the node being in transition"
  [node]
  (state-predicate node PENDING))

(defn running?
  "Predicate for the node being available for requests."
  [node]
  (state-predicate node RUNNING))

(defn terminated?
  "Predicate for the node being halted."
  [node]
  (state-predicate node TERMINATED))

(defn suspended?
  "Predicate for the node being suspended."
  [node]
  (state-predicate node SUSPENDED))

(defn error-state?
  "Predicate for the node being in an error state."
  [node]
  (state-predicate node ERROR))

(defn unknown-state?
  "Predicate for the node being in an unknown state."
  [node]
  (state-predicate node UNKNOWN))

