#!/bin/sh
APPLICATION="anamnesis-datanode"
VERSION="0.1-SNAPSHOT"

JAVA_OPTS="-Xmx12G -Djava.library.path=$BASEDIR/../build/generated"

CLASSPATH=""
BASEDIR=$(dirname $0)
HOSTNAME=$(hostname)
if [ -f $BASEDIR/build/libs/$APPLICATION-$VERSION.jar ]; then
    CLASSPATH="$BASEDIR/build/libs/$APPLICATION-$VERSION.jar"
else
    echo "unable to find $APPLICATION-$VERSION.jar."
    exit 1
fi

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.anamnesis.datanode.Main $@ > $BASEDIR/$HOSTNAME.log 2>&1 &
echo $! > $BASEDIR/$HOSTNAME.pid
