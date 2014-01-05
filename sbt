#!/bin/bash
# This script launches sbt for this project. If present it uses the system 
# version of sbt. If there is no system version of sbt it attempts to download
# sbt locally.
SBT_VERSION=0.12.4
URL1=http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch.jar
URL2=http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch.jar
JAR=.sbtlib/sbt-launch-${SBT_VERSION}.jar

printf "Checking for system sbt ["
if hash sbt 2>/dev/null; then 
    printf "FOUND]\n"
    # Use System SBT
    sbt $@
else
    printf "NOT FOUND]\n"
    # Download sbt or use allready downloaded
    if [ ! -d .sbtlib ]; then
	mkdir .sbtlib
    fi
    if [ ! -f ${JAR} ]; then
	# Download
	printf "Attempting to fetch sbt\n"
	if hash curl 2>/dev/null; then
	    curl --progress-bar ${URL1} > ${JAR} || curl --progress-bar ${URL2} > ${JAR}
	elif hash wget 2>/dev/null; then
	    wget --progress=bar ${URL1} -O ${JAR} || wget --progress=bar ${URL2} -O ${JAR}
	else
	    printf "You do not have curl or wget installed, please install sbt manually from http://www.scala-sbt.org/\n"
	    exit
	fi
    fi
    if [ ! -f ${JAR} ]; then
	# We failed to download
	printf "Our attempt to download sbt locally to {$JAR} failed. Please install sbt manually from http://www.scala-sbt.org/\n"
	exit
    fi
    printf "Launching sbt from .sbtlib\n"
    java \
	-Duser.timezone=UTC \
	-Djava.awt.headless=true \
	-Dfile.encoding=UTF-8 \
	-XX:MaxPermSize=256m \
	-Xmx1g \
	-noverify \
	-jar ${JAR} \
	"$@"
fi
