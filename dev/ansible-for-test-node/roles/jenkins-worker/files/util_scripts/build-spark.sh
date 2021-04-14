#!/bin/bash

cd $1

export JAVA_HOME=/usr/java/latest
export DATE=`date "+%Y%m%d"`
export REVISION=`git rev-parse --short HEAD`

export AMPLAB_JENKINS=1
export PATH="/home/anaconda/envs/py36/bin:$PATH"

# Prepend JAVA_HOME/bin to fix issue where Zinc's embedded SBT incremental compiler seems to
# ignore our JAVA_HOME and use the system javac instead.
export PATH="$JAVA_HOME/bin:$PATH:/usr/local/bin"

# Generate random point for Zinc
export ZINC_PORT
ZINC_PORT=$(python -S -c "import random; print(random.randrange(3030,4030))")

./dev/make-distribution.sh --name ${DATE}-${REVISION} --r --pip --tgz -DzincPort=${ZINC_PORT} \
     -Phadoop-2.7 -Pkubernetes -Pkinesis-asl -Phive -Phive-thriftserver
PATH="$PATH:/usr/sbin"
/home/jenkins/bin/kill_zinc_nailgun.py --zinc-port ${ZINC_PORT}
