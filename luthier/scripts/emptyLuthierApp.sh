#!/bin/bash
# get path to this file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# clean last build's artifacts
cd $DIR/../../
mvn clean

# prepares json files
cd $DIR/../src/main/lua/
lua config.lua
# this is equivalent to lua config.lua app ../../../target/classes/

# install fili using mvn
cd $DIR/../../
mvn install -DskipTests -Dcheckstyle.skip
# runs luthier wiki example on port 9012
mvn -pl luthier exec:java -Dbard__fili_port=9012 -Dbard__druid_coord=http://localhost:8081/druid/coordinator/v1 -Dbard__druid_broker=http://localhost:8082/druid/v2