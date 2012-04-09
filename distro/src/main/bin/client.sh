#!/usr/bin/env bash

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

home=`dirname $this`/..

source ${home}/bin/oozie-sys.sh

names=`find $home/oozie-server/webapps/oozie/WEB-INF/lib -name "*.jar"`


CP=""
for name in $names
do
  CP=${CP}:$name
done

HADOOP_IN_PATH=`which hadoop 2>/dev/null`
if [ -f ${HADOOP_IN_PATH} ]; then
  HADOOP_DIR=`dirname "$HADOOP_IN_PATH"`/..
fi
# HADOOP_HOME env variable overrides hadoop in the path
HADOOP_HOME=${HADOOP_HOME:-$HADOOP_DIR}
if [ "$HADOOP_HOME" != "" ]; then
  CP=$CP:"$HADOOP_HOME/conf"
fi

if [[ -n $1 ]] ; then
  target=$1
else
  target=${OOZIE_BASE_URL}
fi

java -cp $CP org.apache.oozie.SimpleClient $target

