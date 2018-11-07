#!/bin/bash

scalaVersion="2.11"
version="2.3.2"

mvn deploy:deploy-file -Dfile=./target/spark-parent_"$scalaVersion"-"$version"-SNAPSHOT-tests.jar -DrepositoryId=snapshots -Durl=https://artifactory.eng.toasttab.com/artifactory/libs-snapshot-local -DgroupId=org.apache.spark -DartifactId="$moduleName"_"$scalaVersion" -Dversion="$version"-SNAPSHOT


for moduleName in `cat spark-module-list.txt`
do
   if [ -z $moduleName ] ; then
      echo" No module provided"
   else
      echo "Module name : "$moduleName
      mvn deploy:deploy-file -Dfile=./assembly/target/scala-"$scalaVersion"/jars/"$moduleName"_"$scalaVersion"-"$version"-SNAPSHOT.jar -DrepositoryId=snapshots -Durl=https://artifactory.eng.toasttab.com/artifactory/libs-snapshot-local -DgroupId=org.apache.spark -DartifactId="$moduleName"_"$scalaVersion" -Dversion="$version"-SNAPSHOT
     if [ $? -ne 0 ] ; then
        echo "Failed to deploy module : "$moduleName
        break
     fi
   fi
done
