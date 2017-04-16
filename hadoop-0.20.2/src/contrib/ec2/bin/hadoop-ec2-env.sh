# Set environment variables for running Hadoop on Amazon EC2 here. All are required.

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Your Amazon Account Number.
AWS_ACCOUNT_ID=

# Your Amazon AWS access key.
AWS_ACCESS_KEY_ID=

# Your Amazon AWS secret access key.
AWS_SECRET_ACCESS_KEY=

# Location of EC2 keys.
# The default setting is probably OK if you set up EC2 following the Amazon Getting Started guide.
EC2_KEYDIR=`dirname "$EC2_PRIVATE_KEY"`

# The EC2 key name used to launch instances.
# The default is the value used in the Amazon Getting Started guide.
KEY_NAME=gsg-keypair

# Where your EC2 private key is stored (created when following the Amazon Getting Started guide).
# You need to change this if you don't store this with your other EC2 keys.
PRIVATE_KEY_PATH=`echo "$EC2_KEYDIR"/"id_rsa-$KEY_NAME"`

# SSH options used when connecting to EC2 instances.
SSH_OPTS=`echo -i "$PRIVATE_KEY_PATH" -o StrictHostKeyChecking=no -o ServerAliveInterval=30`

# The version of Hadoop to use.
HADOOP_VERSION=0.19.0

# The Amazon S3 bucket where the Hadoop AMI is stored.
# The default value is for public images, so can be left if you are using running a public image.
# Change this value only if you are creating your own (private) AMI
# so you can store it in a bucket you own.
S3_BUCKET=hadoop-images

# Enable public access to JobTracker and TaskTracker web interfaces
ENABLE_WEB_PORTS=true

# The script to run on instance boot.
USER_DATA_FILE=hadoop-ec2-init-remote.sh

# The EC2 instance type: m1.small, m1.large, m1.xlarge
INSTANCE_TYPE="m1.small"
#INSTANCE_TYPE="m1.large"
#INSTANCE_TYPE="m1.xlarge"
#INSTANCE_TYPE="c1.medium"
#INSTANCE_TYPE="c1.xlarge"

# The EC2 group master name. CLUSTER is set by calling scripts
CLUSTER_MASTER=$CLUSTER-master

# Cached values for a given cluster
MASTER_PRIVATE_IP_PATH=~/.hadooop-private-$CLUSTER_MASTER
MASTER_IP_PATH=~/.hadooop-$CLUSTER_MASTER
MASTER_ZONE_PATH=~/.hadooop-zone-$CLUSTER_MASTER

#
# The following variables are only used when creating an AMI.
#

# The version number of the installed JDK.
JAVA_VERSION=1.6.0_07

# SUPPORTED_ARCHITECTURES = ['i386', 'x86_64']
# The download URL for the Sun JDK. Visit http://java.sun.com/javase/downloads/index.jsp and get the URL for the "Linux self-extracting file".
if [ "$INSTANCE_TYPE" == "m1.small" -o "$INSTANCE_TYPE" == "c1.medium" ]; then
  ARCH='i386'
  BASE_AMI_IMAGE="ami-2b5fba42"  # ec2-public-images/fedora-8-i386-base-v1.07.manifest.xml
  JAVA_BINARY_URL=''
else
  ARCH='x86_64'
  BASE_AMI_IMAGE="ami-2a5fba43"  # ec2-public-images/fedora-8-x86_64-base-v1.07.manifest.xml
  JAVA_BINARY_URL=''
fi

if [ "$INSTANCE_TYPE" == "c1.medium" ]; then
  AMI_KERNEL=aki-9b00e5f2 # ec2-public-images/vmlinuz-2.6.18-xenU-ec2-v1.0.i386.aki.manifest.xml
fi

if [ "$INSTANCE_TYPE" == "c1.xlarge" ]; then
  AMI_KERNEL=aki-9800e5f1 # ec2-public-images/vmlinuz-2.6.18-xenU-ec2-v1.0.x86_64.aki.manifest.xml
fi

if [ "$AMI_KERNEL" != "" ]; then
  KERNEL_ARG="--kernel ${AMI_KERNEL}"
fi
