#!/bin/bash
APPLICATION="anamnesis-namenode"
VERSION="0.1-SNAPSHOT"

CLASSPATH=""
BASEDIR=$(dirname $0)
HOSTNAME=$(hostname)
if [ -f $BASEDIR/build/libs/$APPLICATION-$VERSION.jar ]; then
    CLASSPATH="$BASEDIR/build/libs/$APPLICATION-$VERSION.jar"
else
    echo "unable to find $APPLICATION-$VERSION.jar."
    exit 1
fi

JAVA_OPTS="-Xmx2G"

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.anamnesis.namenode.Main $@
