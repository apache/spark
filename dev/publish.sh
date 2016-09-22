#!/usr/bin/env bash
set -euo pipefail

FLAGS="-Psparkr -Phive -Phive-thriftserver -Pyarn -Pmesos"
case $CIRCLE_NODE_INDEX in
0)
  ./build/sbt -Phadoop-2.7 -Pmesos -Pkinesis-asl -Pyarn -Phive-thriftserver -Phive publish
  ;;
1)
  ./dev/make-distribution.sh --name without-hadoop --tgz "-Psparkr -Phadoop-provided -Pyarn -Pmesos" \
      2>&1 >  binary-release-without-hadoop.log
  ;;
2)
  ./dev/make-distribution.sh --name hadoop2.7 --tgz "-Phadoop2.7 $FLAGS" \
      2>&1 >  binary-release-hadoop2.7.log
  ;;
esac
