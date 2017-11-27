#!/bin/bash
java -cp "build/libs/anamnesis-datanode-deps.jar:build/libs/anamnesis-datanode.jar" -Djava.library.path="../build/generated" com.bushpath.anamnesis.datanode.Main $@
