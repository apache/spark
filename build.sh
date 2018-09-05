#!/bin/bash
#
# Copyright (c) 2018 Baidu.com, Inc, All Rights Reserved
#
# Author: zhuliangchang
# Date: 2018/3/26
# Brief:
#   build maven project: baidu/inf-spark/spark-source by build_submitter.py
# Globals:
#   CUR_PATH
# Arguments:
#   None
# Return:
#   None
declare -r CUR_PATH=`pwd`
##########################################
# Brief:
#   download scala & zinc tools 
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
##########################################
function download_tools() {
    wget -P build http://nmg01-spark-master01.nmg01.baidu.com:8090/tmp/zinc-0.3.11.tgz 
    wget -P build http://nmg01-spark-master01.nmg01.baidu.com:8090/tmp/scala-2.11.8.tgz
}
##########################################
# Brief:
#   set compile environment for maven
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
##########################################
function setup_compile_env() {
    export M2_HOME=${BUILD_KIT_PATH}/maven/apache-maven-3.3.9
    export JAVA_HOME=${BUILD_KIT_PATH}/java/jdk-1.8-8u20
    export PATH=${M2_HOME}/bin:${JAVA_HOME}/bin:${PATH}
    cp settings.xml  ~/.m2/
}
##########################################
# Brief:
#   compile spark source
# Globals:
#   None
# Arguments:
#   spark-version  
# Returns:
#   None
##########################################
function compile() {
    if [ $# != 1 ]; then
       echo "need provide spark-version"
       exit 1 
    fi
    SPARK_VERSION=$1
    #echo "mvn versions:set -DnewVersion=${SPARK_VERSION} $(get_profiles)"
    #mvn versions:set -DnewVersion=${SPARK_VERSION} $(get_profiles)
    sh dev/make-distribution.sh --tgz $(get_profiles)
}
##########################################
# Brief:
#   get profile for maven when compile
# Goals:
#   None
# Arguments:
#   None
# Returns:
#   profiles as string
##########################################
function get_profiles() {
    echo "-Phadoop-2.7 -Pyarn -Phive -Phive-thriftserver"
}
##########################################
# TODO: (zhuliangchang@baidu.com):
# Brief:
#   deploy jars to baidu private maven repository
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
##########################################
function main() {
  if [ $# != 1 ]; then
     echo "Need supply spark version!!!"
     exit 1
  fi
  echo "SPARK-VERSION: $1"
  download_tools
  setup_compile_env 
  compile $1
  mkdir output 
}
main "$@"
