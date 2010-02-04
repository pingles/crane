#!/usr/bin/env bash

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size.
################################################################################

################################################################################
# Initialize variables
################################################################################

MAX_MAP_TASKS=1
MAX_REDUCE_TASKS=1
CHILD_OPTS=-Xmx1536m
CHILD_ULIMIT=2097152

MAPRED_LOCAL_DIR=/mnt/hadoop/mapred/local
DFS_NAME_DIR=/mnt/hadoop/hdfs/name
FS_CHECKPOINT_DIR=/mnt/hadoop/hdfs/secondary
DFS_DATA_DIR=/mnt/hadoop/hdfs/data
FIRST_MOUNT=/mnt
DFS_REPLICATION=3 

MASTER_HOST=%MASTER_HOST%  

cat >  /etc/sysctl.conf <<END
# Workaround for TCP Window Scaling bugs in other ppl's equipment:

net.ipv4.tcp_wmem = 4096 16384 512000
net.ipv4.tcp_rmem = 4096 87380 512000
END

SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`

AWS_ACCESS_KEY_ID=%AWS_ACCESS_KEY_ID%
AWS_SECRET_ACCESS_KEY=%AWS_SECRET_ACCESS_KEY%

HADOOP_HOME=`ls -d /usr/local/hadoop-*`

################################################################################
# Hadoop configuration
# Modify this section to customize your Hadoop cluster.
################################################################################

cat > $HADOOP_HOME/conf/hadoop-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>

<property>
  <name>dfs.block.size</name>
  <value>134217728</value>
  <final>true</final>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>$DFS_DATA_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.du.reserved</name>
  <value>1073741824</value>
  <final>true</final>
</property>
<property>
  <name>dfs.datanode.handler.count</name>
  <value>3</value>
  <final>true</final>
</property>
<property>
  <name>dfs.name.dir</name>
  <value>$DFS_NAME_DIR</value>
  <final>true</final>
</property>
<property>
  <name>dfs.namenode.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>dfs.permissions</name>
  <value>true</value>
  <final>true</final>
</property>
<property>
  <name>dfs.replication</name>
  <value>$DFS_REPLICATION</value>
</property>
<property>
  <name>fs.checkpoint.dir</name>
  <value>$FS_CHECKPOINT_DIR</value>
  <final>true</final>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST/</value>
</property>
<property>
  <name>fs.trash.interval</name>
  <value>1440</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/tmp/hadoop-\${user.name}</value>
  <final>true</final>
</property>
<property>
  <name>io.file.buffer.size</name>
  <value>65536</value>
</property>
<property>
  <name>mapred.child.java.opts</name>
  <value>$CHILD_OPTS</value>
</property>
<property>
  <name>mapred.child.ulimit</name>
  <value>$CHILD_ULIMIT</value>
  <final>true</final>
</property>
<property>
  <name>mapred.job.tracker</name>
  <value>$MASTER_HOST:8021</value>
</property>
<property>
  <name>mapred.job.tracker.handler.count</name>
  <value>5</value>
  <final>true</final>
</property>
<property>
  <name>mapred.local.dir</name>
  <value>$MAPRED_LOCAL_DIR</value>
  <final>true</final>
</property>
<property>
  <name>mapred.map.tasks.speculative.execution</name>
  <value>true</value>
</property>
<property>
  <name>mapred.reduce.parallel.copies</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks</name>
  <value>10</value>
</property>
<property>
  <name>mapred.reduce.tasks.speculative.execution</name>
  <value>false</value>
</property>
<property>
  <name>mapred.submit.replication</name>
  <value>10</value>
</property>
<property>
  <name>mapred.system.dir</name>
  <value>/hadoop/system/mapred</value>
</property>
<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>$MAX_MAP_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>$MAX_REDUCE_TASKS</value>
  <final>true</final>
</property>
<property>
  <name>tasktracker.http.threads</name>
  <value>46</value>
  <final>true</final>
</property>
<property>
  <name>mapred.output.compression.type</name>
  <value>BLOCK</value>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.default</name>
  <value>org.apache.hadoop.net.StandardSocketFactory</value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.ClientProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>hadoop.rpc.socket.factory.class.JobSubmissionProtocol</name>
  <value></value>
  <final>true</final>
</property>
<property>
  <name>io.compression.codecs</name>
  <value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec</value>
</property>
<property>
  <name>fs.s3.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>
<property>
  <name>fs.s3.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>
<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>%AWS_ACCESS_KEY_ID%</value>
</property>
<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>%AWS_SECRET_ACCESS_KEY%</value>
</property>

<property>
  <name>mapred.task.timeout</name>
  <value>0</value>
  <description>The number of milliseconds before a task will be
  terminated if it neither reads an input, writes an output, nor
  updates its status string.  We use zero for no timeout.
  </description>
</property>

</configuration>
EOF

################################################################################
# final settings
################################################################################
mkdir -p /mnt/hadoop/logs

# not set on boot
export USER="root"

# Run this script on next boot
rm -f /var/ec2/ec2-run-user-data.*
EOF
