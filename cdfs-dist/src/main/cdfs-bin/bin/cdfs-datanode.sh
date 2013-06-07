#!/bin/bash

STARTSTOP=$1

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cdfs-config.sh

if [ "$CDFS_PID_DIR" = "" ]; then
	CDFS_PID_DIR=/tmp
fi

if [ "$CDFS_IDENT_STRING" = "" ]; then
	CDFS_IDENT_STRING="$USER"
fi

out=$CDFS_LOG_DIR/cdfs-$CDFS_IDENT_STRING-datanode-$HOSTNAME.out
log=$CDFS_LOG_DIR/cdfs-$CDFS_IDENT_STRING-datanode-$HOSTNAME.log
pid=$CDFS_PID_DIR/cdfs-$CDFS_IDENT_STRING-datanode.pid
log_setting="-Dlog.file="$log" -Dlog4j.configuration=file://"$CDFS_CONF_DIR"/log4j.properties"

JVM_ARGS="$JVM_ARGS -XX:+UseParNewGC -XX:NewRatio=8 -XX:PretenureSizeThreshold=64m -Xms"$DATANODE_HEAP_SIZE"m -Xmx"$DATANODE_HEAP_SIZE"m"

case $STARTSTOP in

	(start)
		mkdir -p "$CDFS_PID_DIR"
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo CDFS datanode running as process `cat $pid` on host $HOSTNAME.  Stop it first.
				exit 1
     			fi
		fi

		echo Starting CDFS datanode on host $HOSTNAME
		$JAVA_HOME/bin/java $JVM_ARGS $log_setting -classpath $CLASSPATH edu.berkeley.icsi.cdfs.datanode.DataNode -confDir $CDFS_CONF_DIR > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

	(stop)
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Stopping CDFS datanode on host $HOSTNAME
				kill `cat $pid`
			else
				echo No CDFS datanode to stop on host $HOSTNAME
			fi
		else
			echo No CDFS datanode to stop on host $HOSTNAME
		fi
	;;

	(*)
		echo Please specify start or stop
	;;

esac
