#!/usr/bin/env bash

this="${BASH_SOURCE-$0}"
bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$bin/$script"

home=`dirname $this`/..

names=`find $home/oozie-server/webapps/oozie/WEB-INF/lib -name "*.jar"`

CP=""
for name in $names
do
  CP=${CP}:$name
done

if [[ -n $1 ]] ; then
  target=$1
else
  target=http://localhost:11000/oozie
fi

java -cp $CP org.apache.oozie.SimpleClient $target

