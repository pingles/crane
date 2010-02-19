(ns
    #^{:doc "
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
           (org.jclouds.compute.domain Template TemplateBuilder ComputeMetadata
                                       NodeMetadata Size OsFamily Image)
           (com.google.common.collect ImmutableSet)))

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
                  (com.google.common.collect.ImmutableSet/builder)
                  modules)))

(defn compute-context
  "Create a logged in context."
  ([s a k]
     (compute-context s a k (modules :log4j :ssh :enterprise)))
  ([#^String s #^String a #^String k #^ImmutableSet m]
     (.createContext (new ComputeServiceContextFactory) s a k m)))

(defn- seq-from-immutable-set [#^ImmutableSet set]
  (map #(.getValue %) set))

(defn locations
  "Retrieve the available compute locations for the compute context."
  [#^ComputeServiceContext compute]
  (seq-from-immutable-set (.getLocations (.getComputeService compute))))

(defn nodes
  "Retrieve the existing nodes for the compute context."
  ([#^ComputeServiceContext compute]
     (seq-from-immutable-set (.getNodes (.getComputeService compute))))
  ([#^ComputeServiceContext compute #^String tag]
     (seq-from-immutable-set (.getNodesWithTag (.getComputeService compute) tag))))

(defn images
  "Retrieve the available images for the compute context."
  [#^ComputeServiceContext compute]
  (seq-from-immutable-set (.getImages (.getComputeService compute))))

(defn sizes
  "Retrieve the available node sizes for the compute context."
  [#^ComputeServiceContext compute]
  (seq-from-immutable-set (.getSizes (.getComputeService compute))))

(defn default-template [#^ComputeServiceContext compute]
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
  ([compute tag count]
     (run-nodes compute tag count (default-template compute)))
  ([#^ComputeServiceContext compute tag count template]
     (seq-from-immutable-set
      (.runNodesWithTag
       (.getComputeService compute) tag count template))))

(defn run-node
  "Create a node using the default or specified template."
  ([compute tag]
    (run-nodes compute tag 1 (default-template compute)))
  ([compute tag template]
     (run-nodes compute tag 1 template)))

(defn #^NodeMetadata node-details
  "Retrieve the node metadata."
  [#^ComputeServiceContext compute node]
  (.getNodeMetadata (.getComputeService compute) node ))

(defn reboot-nodes
  "Reboot all the nodes with the given tag."
  ([#^ComputeServiceContext compute #^String tag]
    (.rebootNodesWithTag (.getComputeService compute) tag )))

(defn reboot-node
  "Reboot a given node."
  ([#^ComputeServiceContext compute
    #^ComputeMetadata node]
    (.rebootNode (.getComputeService compute) node )))

(defn destroy-nodes
  "Destroy all the nodes with the given tag."
  ([#^ComputeServiceContext compute #^String tag]
    (.destroyNodesWithTag (.getComputeService compute) tag )))

(defn destroy-node
  "Destroy a given node."
  ([#^ComputeServiceContext compute
    #^ComputeMetadata node]
    (.destroyNode (.getComputeService compute) node )))

(defmacro state-predicate [node state]
  `(= (.getState ~node)
      (. org.jclouds.compute.domain.NodeState ~state)))

(defn pending?
  "Predicate for the node being in transition"
  [#^NodeMetadata node]
  (state-predicate node PENDING))

(defn running?
  "Predicate for the node being available for requests."
  [#^NodeMetadata node]
  (state-predicate node RUNNING))

(defn terminated?
  "Predicate for the node being halted."
  [#^NodeMetadata node]
  (state-predicate node TERMINATED))

(defn suspended?
  "Predicate for the node being suspended."
  [#^NodeMetadata node]
  (state-predicate node SUSPENDED))

(defn error-state?
  "Predicate for the node being in an error state."
  [#^NodeMetadata node]
  (state-predicate node ERROR))

(defn unknown-state?
  "Predicate for the node being in an unknown state."
  [#^NodeMetadata node]
  (state-predicate node UNKNOWN))

