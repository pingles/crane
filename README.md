# crane
## Deployment, Configuration, Cloud Services, and Distributed Computing Infrastructure in Clojure

Crane wraps amazon libs typica and jets3t for compute via ec2, blog storage via s3, queue service via sqs, and it uses clj-ssh for ssh.

Crane has remote repl and cluster capabilities, as well as deployment capaibilities for distributes systems like hadoop, as well as for web services stacks.

## How to start a hadoop ec2 cluster using crane.

Workflow:
-def conf map using conf fn
-def new Jec2 class, stores creds
-launch hadoop cluster

Configuration maps:

creds.clj

{:key "AWS-KEY"
 :secretkey "AWS-SECRET-KEY"
 :private-key-path "/path/to/private-key"
 :key-name "key-name"}

conf.clj

{:image "ami-"
 :instance-type :m1.large
 :group "ec2-group"
 :instances int
 :instances int
 :creds "/path/to/creds.clj-dir"
 :push ["/source/path" "/dest/path/file"
        "/another/source/path" "/another/dest/path/file"]
 :hadooppath "/path/to/hadoop"          ;;used for remote pathing
 :hadoopuser "hadoop-user"              ;;remote hadoop user
 :mapredsite "/path/to/local/mapred"    ;;used as local templates
 :coresite "/path/to/local/core-site"
 :hdfssite "/path/to/local/hdfssite"}

Example:

;;read in config "aws.clj"
crane.cluster> (def my-conf (conf "/path/to/conf.clj-dir"))

;;create new Jec2 class
crane.cluster> (def ec (ec2 (creds "/path/to/creds.clj-dir")))

;;start cluster 
(launch-hadoop-cluster ec my-conf)

To build:

1. Download and install leiningen http://github.com/technomancy/leiningen
2. $ lein deps
4. $ lein install


## crane is part of clj-sys http://github.com/clj-sys

- Conciseness, but not at the expense of expressiveness, explicitness, abstraction, and composability.

- Simple and explict functional sytle over macros and elaborate DSLs.

- Functional parameterization over vars and binding.

- Libraries over frameworks.

- Build in layers.

- Write tests.

- Copyright (c) Bradford Cross and Jared Strate released under the MIT License (http://www.opensource.org/licenses/mit-license.php).