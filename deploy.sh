#!/bin/sh

set -o nounset
set -o errexit

ref_name="spark-2.1.1-bin-hadoop2.7"

pkg="$(ls spark*.tgz)"
name="${pkg%.tgz}"

rm -r $HOME/$name*
cp -v $pkg $HOME
cd $HOME && \
  tar -xzf $pkg && \
  cd -
cp -v \
  $HOME/$ref_name/conf/spark-env.sh \
  $HOME/$ref_name/conf/slaves \
  $HOME/$name/conf/


rm -r $HOME/tmp/v1/$name*
cp -v $pkg $HOME/tmp/v1

for host in ivt-spark1 ivt-spark2
do
  ssh $host "rm -r $name*"
  scp $pkg $host:
  ssh $host "tar -xzf $pkg"
  ssh $host "cp -v $ref_name/conf/spark-env.sh $name/conf/"
done
