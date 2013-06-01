#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cdfs-config.sh

$JAVA_HOME/bin/java -classpath $CLASSPATH edu.berkeley.icsi.cdfs.wlgen.WorkloadGenerator -c $CDFS_CONF_DIR $@
