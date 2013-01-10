#!/bin/bash

# Resolve links
this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# Convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# Define JAVA_HOME if it is not already set
if [ -z "${JAVA_HOME+x}" ]; then
        JAVA_HOME=/usr/lib/jvm/java-6-sun/
fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME+x}" ]; then
        HOSTNAME=`hostname`
fi

# Define the main directory of the CDFS installation
CDFS_ROOT_DIR=`dirname "$this"`/..
CDFS_CONF_DIR=$CDFS_ROOT_DIR/conf
CDFS_BIN_DIR=$CDFS_ROOT_DIR/bin
CDFS_LIB_DIR=$CDFS_ROOT_DIR/lib
CDFS_LOG_DIR=$CDFS_ROOT_DIR/log

# Arguments for the JVM. 
JVM_ARGS="-Djava.net.preferIPv4Stack=true"

# Default classpath 
CLASSPATH=$( echo $MEMNGT_LIB_DIR/*.jar . | sed 's/ /:/g' )
