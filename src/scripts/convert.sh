#!/bin/sh

#
# usage: convert <source_configuration> <target_configuration>
#

#
#Installation Directory
#
dir=`dirname $0`
if [ "$dir" != "." ]
then
  INST=`dirname $dir`
else
  pwd | grep -e 'bin$' > /dev/null
  if [ $? = 0 ]
  then
    INST=".."
  else
    INST=`dirname $dir`
  fi
fi

INST=${INST:-.}

#
#Alternatively specify the installation dir here
#
#INST=

JAVA=java

JARS=${INST}/lib/*.jar
CP=
for JAR in $JARS ; do 
    CP=$CP:$JAR
done

#memory for the VM
MEM=-Xmx128m

$JAVA $MEM -cp $CP de.fzj.unicore.persistence.util.Convert $*

