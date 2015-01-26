#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

MAVEN_SETTINGS_DIR=~/.m2/settings.xml
MAVEN_LOCAL_REPO=~/.m2/repository

# Read user maven configurations if there are any. Update path to local repository if needed.
if [ -e "$MAVEN_SETTINGS_DIR" ]; then
  searchAlternateRepo=$(grep "<localRepository>" $MAVEN_SETTINGS_DIR)
  if [ ${#searchAlternateRepo[@]} == 1 ]; then
    removePrefix=${searchAlternateRepo#<localRepository>}
    MAVEN_LOCAL_REPO=${removePrefix%</localRepository>}
  fi
fi

# Given the path for a maven coordinate, returns the artifactId and the version
computeArtifactName() {
  coordinatePath=$1
  stripPrefix=${coordinatePath#*/}
  echo ${stripPrefix%.*}
}

# Given a maven coordinate, returns the path to the corresponding jar in the local Maven repo.
# Currently requires the coordinate to be in the form `groupId:artifactId:version`
computeLocalPath() {
  coordinate=$1
  split=(${coordinate//:/ })
  if [ ${#split[@]} != 3 ]; then
    echo "Provided Maven Coordinates must be in the form 'groupId:artifactId:version'."
    echo "The coordinate provided is: coordinate"
    exit 1
  fi
  groupId=${split[0]//.//}
  artifactId=${split[1]}
  version=${split[2]}
  echo "$MAVEN_LOCAL_REPO/$groupId/$artifactId/$version/$artifactId-$version.jar"
}

CUR_DIR=$PWD

# Removes dependency on Spark (if there is one)
removeSparkDependency() {
  artifactName=$1
  echo "$artifactName" >> log.txt
  # Create empty pom file for the maven plugin to use
  > pom.xml
  inSpark=false
  while read -r line; do
    if [[ $line == *"<groupId>org.apache.spark"* ]]; then
      inSpark=true
    fi
    if [[ $inSpark == true ]] && [[ $line == *"</dependency>"* ]]; then
      echo "<scope>provided</scope>" >> pom.xml
      inSpark=false
    fi
    echo $line >> pom.xml
  done < "/$artifactName.pom"
  # bash skips the last line for some reason
  echo $line >> pom.xml
}

# Recursive function that gets the first level of dependencies of each maven coordinate.
# We use a recursive function so that if any of the transitive dependencies are Spark, we don't
# include anything related to it in the classpath.
addDependenciesToClasspath() {
  pathOfArtifact=$1
  if [ ${#pathOfArtifact} -gt 0 ]; then
    artifactName=$(computeArtifactName $pathOfArtifact)
    cd ${pathOfArtifact%/*}
    mavenPath=$pathOfArtifact
    > cp.txt
    removeSparkDependency $artifactName
    mvn dependency:build-classpath -Dmdep.outputFile=cp.txt -DexcludeScope=provided -DexcludeTransitive=true
    depClasspath=`cat cp.txt`
    depList=(${depClasspath//:/ })
    for dep in "${depList[@]}"; do
      mavenPath="$mavenPath:$(addDependenciesToClasspath $dep)"
    done
    echo $mavenPath
  fi
}

# The path to jars in the local maven repo that will be appended to the classpath
mavenClasspath=""
if [ ! -z "SPARK_SUBMIT_MAVEN_COORDINATES" ]; then
  coordinateList=(${SPARK_SUBMIT_MAVEN_COORDINATES//,/ })
  for i in "${coordinateList[@]}"; do
    localPath=$(computeLocalPath "$i")
    # if jar doesn't exist, download it and all it's dependencies (except Spark)
    if [ ! -e "$localPath.jar" ]; then
      mvn dependency:get -Dartifact=$i -DremoteRepositories=$SPARK_SUBMIT_MAVEN_REPOS -Dtransitive=false
    fi
    # add all dependencies of this jar to the classpath
    mavenClasspath="$mavenClasspath:$(addDependenciesToClasspath $localPath)"
  done
fi

cd $CUR_DIR

echo ${mavenClasspath#:}
