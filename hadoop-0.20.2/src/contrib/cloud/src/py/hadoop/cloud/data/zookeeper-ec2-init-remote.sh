#!/bin/bash -x

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

################################################################################
# Script that is run on each EC2 instance on boot. It is passed in the EC2 user
# data, so should not exceed 16K in size after gzip compression.
#
# This script is executed by /etc/init.d/ec2-run-user-data, and output is
# logged to /var/log/messages.
################################################################################

################################################################################
# Initialize variables
################################################################################

# Substitute environment variables passed by the client
export %ENV%

ZK_VERSION=${ZK_VERSION:-3.2.2}
ZOOKEEPER_HOME=/usr/local/zookeeper-$ZK_VERSION
ZK_CONF_DIR=/etc/zookeeper/conf

function register_auto_shutdown() {
  if [ ! -z "$AUTO_SHUTDOWN" ]; then
    shutdown -h +$AUTO_SHUTDOWN >/dev/null &
  fi
}

# Install a list of packages on debian or redhat as appropriate
function install_packages() {
  if which dpkg &> /dev/null; then
    apt-get update
    apt-get -y install $@
  elif which rpm &> /dev/null; then
    yum install -y $@
  else
    echo "No package manager found."
  fi
}

# Install any user packages specified in the USER_PACKAGES environment variable
function install_user_packages() {
  if [ ! -z "$USER_PACKAGES" ]; then
    install_packages $USER_PACKAGES
  fi
}

function install_zookeeper() {
  zk_tar_url=http://www.apache.org/dist/hadoop/zookeeper/zookeeper-$ZK_VERSION/zookeeper-$ZK_VERSION.tar.gz
  zk_tar_file=`basename $zk_tar_url`
  zk_tar_md5_file=`basename $zk_tar_url.md5`

  curl="curl --retry 3 --silent --show-error --fail"
  for i in `seq 1 3`;
  do
    $curl -O $zk_tar_url
    $curl -O $zk_tar_url.md5
    if md5sum -c $zk_tar_md5_file; then
      break;
    else
      rm -f $zk_tar_file $zk_tar_md5_file
    fi
  done

  if [ ! -e $zk_tar_file ]; then
    echo "Failed to download $zk_tar_url. Aborting."
    exit 1
  fi

  tar zxf $zk_tar_file -C /usr/local
  rm -f $zk_tar_file $zk_tar_md5_file

  echo "export ZOOKEEPER_HOME=$ZOOKEEPER_HOME" >> ~root/.bashrc
  echo 'export PATH=$JAVA_HOME/bin:$ZOOKEEPER_HOME/bin:$PATH' >> ~root/.bashrc
}

function configure_zookeeper() {
  mkdir -p /mnt/zookeeper/logs
  ln -s /mnt/zookeeper/logs /var/log/zookeeper
  mkdir -p /var/log/zookeeper/txlog
  mkdir -p $ZK_CONF_DIR
  cp $ZOOKEEPER_HOME/conf/log4j.properties $ZK_CONF_DIR

  sed -i -e "s|log4j.rootLogger=INFO, CONSOLE|log4j.rootLogger=INFO, ROLLINGFILE|" \
         -e "s|log4j.appender.ROLLINGFILE.File=zookeeper.log|log4j.appender.ROLLINGFILE.File=/var/log/zookeeper/zookeeper.log|" \
      $ZK_CONF_DIR/log4j.properties
      
  # Ensure ZooKeeper starts on boot
  cat > /etc/rc.local <<EOF
ZOOCFGDIR=$ZK_CONF_DIR $ZOOKEEPER_HOME/bin/zkServer.sh start > /dev/null 2>&1 &
EOF

}

register_auto_shutdown
install_user_packages
install_zookeeper
configure_zookeeper
