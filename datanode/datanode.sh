#!/bin/bash
BASEDIR=$(dirname $0)

JAVA_OPTS="-Xmx3G -Djava.library.path=../build/generated"

CLASSPATH=""
if [ -f $BASEDIR/build/libs/anamnesis-datanode.jar ]
then
    CLASSPATH="$BASEDIR/build/libs/anamnesis-datanode.jar"
else
    echo "unable to find anamnesis-datanode.jar. please compile with 'gradle build'."
    exit 1
fi

java -cp $CLASSPATH $JAVA_OPTS com.bushpath.anamnesis.datanode.Main $@
