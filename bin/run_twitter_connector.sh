#!/bin/bash

# Copyright 2017 Fondazione Ugo Bordoni
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
#     
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

fullPath () {
	t='TEMP=`cd $TEMP; pwd`'
	for d in $*; do
		eval `echo $t | sed 's/TEMP/'$d'/g'`
	done
}


if [ ! -n "$TWITTERCONNECTOR_HOME" ]
then
	#find out where this script is running
	TEMPVAR=`dirname $0`
	#make the path abolute
	fullPath TEMPVAR
	#qosmo folder is folder above
	TWITTERCONNECTOR_HOME=`dirname $TEMPVAR`
	echo "Setting TWITTERCONNECTOR_HOME to $TWITTERCONNECTOR_HOME"
fi

if [ ! -n "$TWITTERCONNECTOR_LIB" ];
then
	TWITTERCONNECTOR_LIB=$TWITTERCONNECTOR_HOME/lib/
fi

for jar in $TWITTERCONNECTOR_LIB/*.jar; do
	if [ ! -n "$CLASSPATH" ]
	then
		CLASSPATH=$jar
	else
		CLASSPATH=$CLASSPATH:$jar
	fi
done

CLASSPATH=$CLASSPATH:$TWITTERCONNECTOR_HOME/etc

if [ ! -n "$TWITTERCONNECTOR_HEAP_MEM" ];
then
    TWITTERCONNECTOR_HEAP_MEM=1024M
fi

java -Xmx$TWITTERCONNECTOR_HEAP_MEM \
	 -cp $CLASSPATH \
     it.fub.bigdataplatform.connectors.twitter.StreamingConnector $@
