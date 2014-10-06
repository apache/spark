#!/bin/bash
BASEDIR=$(dirname $0)
export PYTHONPATH=$BASEDIR/Flux
python core/bin/flux.py $*
