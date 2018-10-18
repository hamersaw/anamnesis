#!/bin/bash
APPLICATION="anamnesis-datanode"
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

JAVA_OPTS="-Xmx12G -Djava.library.path=$BASEDIR/../build/generated"

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.anamnesis.datanode.Main $@
