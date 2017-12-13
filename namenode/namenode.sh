#!/bin/bash
BASEDIR=$(dirname $0)

JAVA_OPTS="-Xmx3G"

CLASSPATH=""
if [ -f $BASEDIR/build/libs/anamnesis-namenode.jar ]
then
    CLASSPATH="$BASEDIR/build/libs/anamnesis-namenode.jar"
else
    echo "unable to find anamnesis-namenode.jar. please compile with 'gradle build'."
    exit 1
fi

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.anamnesis.namenode.Main $@
