(ns
#^{:doc
"
a lib for interacting with jclouds ComputeService. 

Current supported services are:
   [ec2, rimuhosting, terremark, vcloud, hostingdotcom]

Here's an example of getting some compute configuration from rackspace:

(ns crane.jclouds
  (:use crane.compute)
  (:use clojure.contrib.pprint)
)

 (def user  "rackspace_username")
 (def password "rackspace_password")
 (def compute-name "cloudservers")

 (def compute (compute-context compute-name user password))

 (pprint (locations compute))
 (pprint (images compute))
 (pprint (nodes compute))
 (pprint (sizes compute))
 
"}
crane.compute

  (:use clojure.contrib.duck-streams)
  (:import java.io.File)
  (:import org.jclouds.domain.Location)
  (:import org.jclouds.compute.ComputeService)
  (:import org.jclouds.compute.ComputeServiceContext)
  (:import org.jclouds.compute.ComputeServiceContextFactory)
  (:import org.jclouds.compute.domain.Template)
  (:import org.jclouds.compute.domain.TemplateBuilder)
  (:import org.jclouds.compute.domain.ComputeMetadata)
  (:import org.jclouds.compute.domain.Size)
  (:import org.jclouds.compute.domain.Image))
 
(defn compute-context

  ([{service :service account :account key :key}]
     (compute-context service account key))
  ([s a k] (.createContext (new ComputeServiceContextFactory) s a k ))
  ([s a k m] (.createContext (new ComputeServiceContextFactory) s a k m)))

(defn locations

"
http://code.google.com/p/jclouds
 
get the nodes in a service:
compute -> locations

example: (pprint
(locations
(compute-context service flightcaster-creds))
"
  ([compute]
     (.getLocations (.getComputeService compute)))
)

(defn nodes

"
http://code.google.com/p/jclouds
 
get the nodes in a service:
compute -> nodes

example: (pprint
(nodes
(compute-context service flightcaster-creds))
"
  ([compute]
     (.getNodes (.getComputeService compute)))
)

(defn images

"
http://code.google.com/p/jclouds

get the images in a service:
compute -> images

example: (pprint
(images
(compute-context service flightcaster-creds)))
"
  ([compute]
     (.getImages (.getComputeService compute)))
)

(defn sizes

"
http://code.google.com/p/jclouds

get the sizes in a service:
compute -> sizes

example: (pprint
(sizes
(compute-context service flightcaster-creds)))
"
  ([compute]
     (.getSizes (.getComputeService compute)))
)

(defn run-nodes

"
http://code.google.com/p/jclouds
 
create and run nodes in a service:
compute tag count template -> node

the node will be running when this completes and contain ssh credentials, which may be a password or a private key.  Note that for many clouds, this is the only opportunity to.get the ssh credentials.

example: (pprint
(run-nodes
(compute-context service flightcaster-creds) tag count template))
"
  ([compute tag count template]
     (.runNodesWithTag (.getComputeService compute) tag count template))
)

(defn node-details

"
http://code.google.com/p/jclouds
 
get more info on a node:
compute node -> node

example: (pprint
(node-details
(compute-context service flightcaster-creds) node ))
"
  ([compute node]
     (.getNodeMetadata  (.getComputeService compute) node ))
)

(defn destroy-node

"
http://code.google.com/p/jclouds
 
destroys a node in a service:
compute node -> nil

example:
(destroy-node
(compute-context service flightcaster-creds) node )
"
  ([compute node]
     (.destroyNode (.getComputeService compute) node ))
)
