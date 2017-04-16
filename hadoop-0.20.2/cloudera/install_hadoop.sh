#!/bin/bash -x
# Copyright 2009 Cloudera, inc.

set -ex

usage() {
  echo "
usage: $0 <options>
  Required not-so-options:
     --cloudera-source-dir=DIR   path to cloudera distribution files
     --build-dir=DIR             path to hive/build/dist
     --prefix=PREFIX             path to install into

  Optional options:
     --native-build-string       eg Linux-amd-64 (optional - no native installed if not set)
     ... [ see source for more similar options ]
  "
  exit 1
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'cloudera-source-dir:' \
  -l 'prefix:' \
  -l 'build-dir:' \
  -l 'native-build-string:' \
  -l 'installed-lib-dir:' \
  -l 'lib-dir:' \
  -l 'system-lib-dir:' \
  -l 'src-dir:' \
  -l 'etc-dir:' \
  -l 'doc-dir:' \
  -l 'man-dir:' \
  -l 'example-dir:' \
  -l 'apache-branch:' \
  -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"
while true ; do
    case "$1" in
        --cloudera-source-dir)
        CLOUDERA_SOURCE_DIR=$2 ; shift 2
        ;;
        --prefix)
        PREFIX=$2 ; shift 2
        ;;
        --lib-dir)
        LIB_DIR=$2 ; shift 2
        ;;
        --system-lib-dir)
        SYSTEM_LIB_DIR=$2 ; shift 2
        ;;
        --build-dir)
        BUILD_DIR=$2 ; shift 2
        ;;
        --native-build-string)
        NATIVE_BUILD_STRING=$2 ; shift 2
        ;;
        --doc-dir)
        DOC_DIR=$2 ; shift 2
        ;;
        --etc-dir)
        ETC_DIR=$2 ; shift 2
        ;;
        --installed-lib-dir)
        INSTALLED_LIB_DIR=$2 ; shift 2
        ;;
        --man-dir)
        MAN_DIR=$2 ; shift 2
        ;;
        --example-dir)
        EXAMPLE_DIR=$2 ; shift 2
        ;;
        --apache-branch)
        APACHE_BRANCH=$2 ; shift 2
        ;;
        --src-dir)
        SRC_DIR=$2 ; shift 2
        ;;
        --)
        shift ; break
        ;;
        *)
        echo "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
done

for var in CLOUDERA_SOURCE_DIR PREFIX BUILD_DIR APACHE_BRANCH; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

LIB_DIR=${LIB_DIR:-$PREFIX/usr/lib/hadoop-$APACHE_BRANCH}
SYSTEM_LIB_DIR=${SYSTEM_LIB_DIR:-/usr/lib}
BIN_DIR=${BIN_DIR:-$PREFIX/usr/bin}
DOC_DIR=${DOC_DIR:-$PREFIX/usr/share/doc/hadoop-$APACHE_BRANCH}
MAN_DIR=${MAN_DIR:-$PREFIX/usr/man}
EXAMPLE_DIR=${EXAMPLE_DIR:-$DOC_DIR/examples}
SRC_DIR=${SRC_DIR:-$PREFIX/usr/src/hadoop-$APACHE_BRANCH}
ETC_DIR=${ETC_DIR:-$PREFIX/etc/hadoop-$APACHE_BRANCH}

INSTALLED_LIB_DIR=${INSTALLED_LIB_DIR:-/usr/lib/hadoop-$APACHE_BRANCH}

# TODO(todd) right now we're using bin-package, so we don't copy
# src/ into the dist. otherwise this would be BUILD_DIR/src
HADOOP_SRC_DIR=$BUILD_DIR/../../src

mkdir -p $LIB_DIR
(cd $BUILD_DIR && tar cf - .) | (cd $LIB_DIR && tar xf - )

# Create symlinks to preserve old jar names
# Also create symlinks of versioned jars to jars without version names, which other
# packages can depend on
(cd $LIB_DIR &&
for j in hadoop-*.jar; do
  if [[ $j =~ hadoop-([a-zA-Z]+)-(.*).jar ]]; then
    name=${BASH_REMATCH[1]}
    ver=${BASH_REMATCH[2]}
    ln -s hadoop-$name-$ver.jar hadoop-$ver-$name.jar
    ln -s hadoop-$name-$ver.jar hadoop-$name.jar
  fi
done)

# Take out things we've installed elsewhere
for x in docs lib/native c++ src conf usr/bin/fuse_dfs contrib/fuse ; do
  rm -rf $LIB_DIR/$x 
done

# Make bin wrappers
mkdir -p $BIN_DIR

for bin_wrapper in hadoop ; do
  wrapper=$BIN_DIR/$bin_wrapper-$APACHE_BRANCH
  cat > $wrapper <<EOF
#!/bin/sh

export HADOOP_HOME=$INSTALLED_LIB_DIR
exec $INSTALLED_LIB_DIR/bin/$bin_wrapper "\$@"
EOF
  chmod 755 $wrapper
done

# Fix some bad permissions in HOD
chmod 755 $LIB_DIR/contrib/hod/support/checklimits.sh || /bin/true
chmod 644 $LIB_DIR/contrib/hod/bin/VERSION || /bin/true

# Link examples to /usr/share
mkdir -p $EXAMPLE_DIR
for x in $LIB_DIR/*examples*jar ; do
  INSTALL_LOC=`echo $x | sed -e "s,$LIB_DIR,$INSTALLED_LIB_DIR,"`
  ln -sf $INSTALL_LOC $EXAMPLE_DIR/
done
# And copy the source
mkdir -p $EXAMPLE_DIR/src
cp -a $HADOOP_SRC_DIR/examples/* $EXAMPLE_DIR/src

# Install docs
mkdir -p $DOC_DIR
cp -r ${BUILD_DIR}/../../docs/* $DOC_DIR

# Install source
mkdir -p $SRC_DIR
rm -f ${HADOOP_SRC_DIR}/contrib/fuse-dfs/src/*.o 
rm -f ${HADOOP_SRC_DIR}/contrib/fuse-dfs/src/fuse_dfs
rm -f ${HADOOP_SRC_DIR}/contrib/fuse-dfs/fuse_dfs
rm -rf ${HADOOP_SRC_DIR}/contrib/hod/


cp -a ${HADOOP_SRC_DIR}/* $SRC_DIR/
mv -f $LIB_DIR/cloudera-pom.xml $LIB_DIR/cloudera $SRC_DIR

# Make the empty config
install -d -m 0755 $ETC_DIR/conf.empty
(cd ${BUILD_DIR}/conf && tar cf - .) | (cd $ETC_DIR/conf.empty && tar xf -)

# Link the HADOOP_HOME conf, log and pid dir to installed locations
rm -rf $LIB_DIR/conf
ln -s ${ETC_DIR#$PREFIX}/conf $LIB_DIR/conf
rm -rf $LIB_DIR/logs
ln -s /var/log/hadoop-$APACHE_BRANCH $LIB_DIR/logs
rm -rf $LIB_DIR/pids
ln -s /var/run/hadoop-$APACHE_BRANCH $LIB_DIR/pids

# Make the pseudo-distributed config
for conf in conf.pseudo ; do
  install -d -m 0755 $ETC_DIR/$conf
  # Install the default configurations 
  (cd ${BUILD_DIR}/conf && tar -cf - .) | (cd $ETC_DIR/$conf && tar -xf -)
  chmod -R 0644 $ETC_DIR/$conf/*

  # Overlay the -site files
  (cd ${BUILD_DIR}/../../example-confs/$conf && tar -cf - .) | (cd $ETC_DIR/$conf && tar -xf -)
done

# man pages
mkdir -p $MAN_DIR/man1
cp ${CLOUDERA_SOURCE_DIR}/hadoop-$APACHE_BRANCH.1.gz $MAN_DIR/man1/

############################################################
# ARCH DEPENDENT STUFF
############################################################

if [ ! -z "$NATIVE_BUILD_STRING" ]; then
  cp ${CLOUDERA_SOURCE_DIR}/hadoop-fuse-dfs.1.gz $MAN_DIR/man1/
  # Fuse 
  mkdir -p $LIB_DIR/bin
  mv  ${BUILD_DIR}/contrib/fuse-dfs/* $LIB_DIR/bin
  rmdir ${BUILD_DIR}/contrib/fuse-dfs 

  fuse_wrapper=${BIN_DIR}/hadoop-fuse-dfs
  cat > $fuse_wrapper << EOF
#!/bin/bash

/sbin/modprobe fuse

export HADOOP_HOME=$INSTALLED_LIB_DIR

if [ -f /etc/default/hadoop-0.20-fuse ] 
  then . /etc/default/hadoop-0.20-fuse
fi

if [ -f \$HADOOP_HOME/bin/hadoop-config.sh ] 
  then . \$HADOOP_HOME/bin/hadoop-config.sh
fi

if [ "\${LD_LIBRARY_PATH}" = "" ]; then
  export LD_LIBRARY_PATH=/usr/lib
  for f in \`find \${JAVA_HOME}/jre/lib -name client -prune -o -name libjvm.so -exec dirname {} \;\`; do
    export LD_LIBRARY_PATH=\$f:\${LD_LIBRARY_PATH}
  done
fi

for i in \${HADOOP_HOME}/*.jar \${HADOOP_HOME}/lib/*.jar
  do CLASSPATH+=\$i:
done

export PATH=\$PATH:\${HADOOP_HOME}/bin/

env CLASSPATH=\$CLASSPATH \${HADOOP_HOME}/bin/fuse_dfs \$@
EOF

  chmod 755 $fuse_wrapper

  # Native compression libs
  mkdir -p $LIB_DIR/lib/native/
  cp -r ${BUILD_DIR}/lib/native/${NATIVE_BUILD_STRING} $LIB_DIR/lib/native/

  # Pipes
  mkdir -p $PREFIX/$SYSTEM_LIB_DIR $PREFIX/usr/include
  cp ${BUILD_DIR}/c++/${NATIVE_BUILD_STRING}/lib/libhadooppipes.a \
      ${BUILD_DIR}/c++/${NATIVE_BUILD_STRING}/lib/libhadooputils.a \
      $PREFIX/$SYSTEM_LIB_DIR
  cp -r ${BUILD_DIR}/c++/${NATIVE_BUILD_STRING}/include/hadoop $PREFIX/usr/include/

  # libhdfs
  cp ${BUILD_DIR}/c++/${NATIVE_BUILD_STRING}/lib/libhdfs.so.0.0.0 $PREFIX/$SYSTEM_LIB_DIR
  ln -sf libhdfs.so.0.0.0 $PREFIX/$SYSTEM_LIB_DIR/libhdfs.so.0

  # libhdfs-dev - hadoop doesn't realy install these things in nice places :(
  mkdir -p $PREFIX/usr/share/doc/libhdfs0-dev/examples

  cp ${HADOOP_SRC_DIR}/c++/libhdfs/hdfs.h $PREFIX/usr/include/
  cp ${HADOOP_SRC_DIR}/c++/libhdfs/hdfs_*.c $PREFIX/usr/share/doc/libhdfs0-dev/examples

  #    This is somewhat unintuitive, but the -dev package has this symlink (see Debian Library Packaging Guide)
  ln -sf libhdfs.so.0.0.0 $PREFIX/$SYSTEM_LIB_DIR/libhdfs.so
  sed -e "s|^libdir='.*'|libdir=\"$SYSTEM_LIB_DIR\"|" \
      ${BUILD_DIR}/c++/${NATIVE_BUILD_STRING}/lib/libhdfs.la > $PREFIX/$SYSTEM_LIB_DIR/libhdfs.la
fi
