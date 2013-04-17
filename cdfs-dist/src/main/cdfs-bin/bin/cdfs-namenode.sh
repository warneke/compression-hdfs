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

out=$CDFS_LOG_DIR/cdfs-$CDFS_IDENT_STRING-namenode-$HOSTNAME.out
pid=$CDFS_PID_DIR/cdfs-$CDFS_IDENT_STRING-namenode.pid

JVM_ARGS="$JVM_ARGS -Xms"$NAMNODE_HEAP_SIZE"m -Xmx"$NAMENODE_HEAP_SIZE"m"

case $STARTSTOP in

	(start)
		mkdir -p "$CDFS_PID_DIR"
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo CDFS namenode running as process `cat $pid` on host $HOSTNAME.  Stop it first.
				exit 1
     			fi
		fi

		echo Starting CDFS namenode on host $HOSTNAME
		$JAVA_HOME/bin/java $JVM_ARGS -classpath $CLASSPATH edu.berkeley.icsi.cdfs.namenode.NameNode > "$out" 2>&1 < /dev/null &
		echo $! > $pid
	;;

	(stop)
		if [ -f $pid ]; then
			if kill -0 `cat $pid` > /dev/null 2>&1; then
				echo Stopping CDFS namenode on host $HOSTNAME
				kill `cat $pid`
			else
				echo No CDFS namenode to stop on host $HOSTNAME
			fi
		else
			echo No CDFS namenode to stop on host $HOSTNAME
		fi
	;;

	(*)
		echo Please specify start or stop
	;;

esac
