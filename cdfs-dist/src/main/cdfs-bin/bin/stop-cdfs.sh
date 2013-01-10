#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/cdfs-config.sh

HOSTLIST="${CDFS_CONF_DIR}/slaves"

if [ ! -f $HOSTLIST ]; then
	echo $HOSTLIST is not a valid slave list
	exit 1
fi


$CDFS_BIN_DIR/cdfs-namenode.sh stop

while read line
do
	HOST=$line
	ssh -n $HOST -- "nohup /bin/bash $CDFS_BIN_DIR/cdfs-datanode.sh stop &" &
done < $HOSTLIST
wait
