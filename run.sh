#!/bin/bash

CLASSPATH=.
for LIBS in `ls ./lib`
do
    CLASSPATH=$CLASSPATH:./lib/$LIBS
done
# echo "Going to run with CP = " $CLASSPATH
java -cp $CLASSPATH RDS

