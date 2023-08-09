#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

USERPROFILE=/database/config/db2inst1/sqllib/userprofile
echo "export DB2_KRB5_PRINCIPAL=db2/__IP_ADDRESS_REPLACE_ME__@EXAMPLE.COM" >> $USERPROFILE
echo "export KRB5_KTNAME=/var/custom/db2.keytab" >> $USERPROFILE
# This trick is needed because DB2 forwards environment variables automatically only if it's starting with DB2.
su - db2inst1 -c "db2set DB2ENVLIST=KRB5_KTNAME"

su - db2inst1 -c "db2 UPDATE DBM CFG USING SRVCON_GSSPLUGIN_LIST IBMkrb5 IMMEDIATE"
su - db2inst1 -c "db2 UPDATE DBM CFG USING SRVCON_AUTH KERBEROS IMMEDIATE"

su - db2inst1 -c "db2stop force; db2start"
