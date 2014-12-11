#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#!/bin/sh
jps | awk '/Drillbit/ {print $1}' | xargs kill -9
DRILL_BIN_DIR=/opt/drill/apache-drill-0.7.0-incubating-SNAPSHOT
$DRILL_BIN_DIR/bin/drillbit.sh start
sleep 30
mkdir -p build && cd build && cmake .. && make
./querySubmitter connectStr="zk=127.0.0.1:2181/drill/drillbits1" type=sql api=async query='select * from cp.`employee.json` limit 1' queryTimeout=1 logLevel=trace
#./querySubmitter connectStr="zk=127.0.0.1:2181/drill/drillbits1" type=sql api=async query='select * from sys.drillbits; select * from cp.`employee.json` limit 1' queryTimeout=1
