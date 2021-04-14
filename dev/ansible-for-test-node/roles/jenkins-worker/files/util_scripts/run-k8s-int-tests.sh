#!/bin/bash
set -x

cd $1

DIR=$(pwd)

export PATH="$PATH:/home/anaconda/envs/py36/bin"
export JAVA_HOME=/usr/java/latest
export PATH="$JAVA_HOME/bin:$PATH:/usr/local/bin"
export ZINC_PORT
PVC_TMP_DIR=$(mktemp -d)
export PVC_TESTS_HOST_PATH=$PVC_TMP_DIR
export PVC_TESTS_VM_PATH=$PVC_TMP_DIR

${HOME}/bin/session_lock_resource.py minikube

# Start minikube
/usr/bin/minikube --vm-driver=kvm2 start --memory 6000 --cpus 8
/usr/bin/minikube mount ${PVC_TESTS_HOST_PATH}:${PVC_TESTS_VM_PATH} --9p-version=9p2000.L --gid=0 --uid=185 &
MOUNT_PID=$(jobs -rp)

kubectl create clusterrolebinding serviceaccounts-cluster-admin --clusterrole=cluster-admin --group=system:serviceaccounts || true

resource-managers/kubernetes/integration-tests/dev/dev-run-integration-tests.sh \
  --spark-tgz $DIR/spark-*.tgz

kill -9 $MOUNT_PID
/usr/bin/minikube stop
rm -rf $PVC_TMP_DIR
