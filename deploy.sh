#!/usr/bin/env bash

nodes=(tao-lab-2 tao-lab-ubuntu-3 li-precision-t3610 binbin-precision-t3610 godeep chow)


for i in "${nodes[@]}"
do
	des="shuo@$i:$2"
	scp -r "$1" "$des"
done
