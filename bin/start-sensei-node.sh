#!/usr/bin/env bash

#usage="Usage: start-sensei-node.sh <id> <port> <partitions> <conf-dir>"

# if no args specified, show usage
#if [ $# -le 3 ]; then
#  echo $usage
#  exit 1
#fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

OS=`uname`
IP="" # store IP
case $OS in
   Linux) 
        if [ -f /sbin/ifconfig ]
        then
            IP=`/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}' | head -n 1`
        else
        # some linux use /bin/ifconfig (e.g. gentoo)
            IP=`/bin/ifconfig | grep 'inet ' | grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $2}' | head -n 1`
        fi
        ;;
   FreeBSD|OpenBSD|Darwin) IP=`ifconfig | grep -E '^en[0-9]:' -A 4 | grep -E 'inet.[0-9]' | grep -v '127.0.0.1' | awk '{ print $2}' | head -n 1` ;;
   SunOS) IP=`ifconfig -a | grep inet | grep -v '127.0.0.1' | awk '{ print $2} ' | head -n 1` ;;
   *) IP="Unknown";;
esac


lib=$bin/../sensei-core/target/lib
dist=$bin/../sensei-core/target
resources=$bin/../resources
logs=$bin/../logs

if [[ ! -d $logs ]]; then
  echo "Log file does not exists, creating one..."
  mkdir $logs
fi

# HEAP_OPTS="-Xmx4096m -Xms2048m -XX:NewSize=1024m" # -d64 for 64-bit awesomeness
HEAP_OPTS="-Xmx1g -Xms1g -XX:NewSize=256m"
# HEAP_OPTS="-Xmx1024m -Xms512m -XX:NewSize=128m"
# HEAP_OPTS="-Xmx512m -Xms256m -XX:NewSize=64m"
# GC_OPTS="-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
#JAVA_DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,address=1044,server=y,suspend=n"
#GC_OPTS="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC"
JAVA_OPTS="-server -d64"
JMX_OPTS="-Djava.rmi.server.hostname=$IP -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=18889 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

MAIN_CLASS="com.senseidb.search.node.SenseiServer"


CLASSPATH=$resources/:$lib/*:$dist/*:$1/ext/*

PIDFILE=/tmp/sensei-search-node.pid

if [ -f $PIDFILE ]; then
  echo "File $PIDFILE exists shutdown may not be proper"
  echo "Please check PID" `cat $PIDFILE`
  echo "Make sure the node is shutdown and the file" $PIDFILE "is removed before stating the node"
 else
  
   java $JAVA_OPTS $JMX_OPTS $HEAP_OPTS $GC_OPTS $JAVA_DEBUG -classpath $CLASSPATH  -Dlog.home=$logs $MAIN_CLASS $1  &
  echo $! > ${PIDFILE}
  echo "Sensei node started successfully! Logs are at $logs"
 fi
