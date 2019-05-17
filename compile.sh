#!/bin/bash
CLASSPATH=.
for LIBS in `ls ./lib`
do
    CLASSPATH=$CLASSPATH:./lib/$LIBS
done
javac -nowarn -d . -cp $CLASSPATH src/RDS.java 
