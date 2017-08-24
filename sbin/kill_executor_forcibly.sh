#!/usr/bin/env bash

#
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

# Force kill the CoarseGrainedExecutorBackend process which is likely be orphaned.


print_log()
{
    echo "$*" >> "$LOG_FILE"
}

get_propval()
{
    if [ -f "$1" ]; then
        echo `sed -n 's/^[ \t]*'$2'=\(.*\)/\1/p' $1`
    else
        echo ""
    fi
}

get_executor_pid()
{
    local line_value=$1
    executor_pid=`echo $line_value | awk '{print $2}'`
}

get_executor_appid()
{
    local line_value=$1
    app_id=`echo $line_value | awk -F "--app-id" '{print $2}'| awk '{print $1}'`
}

get_executor_id()
{
    local line_value=$1
    executor_id=`echo $line_value | awk -F "--executor-id" '{print $2}'| awk '{print $1}'`
}

get_master_curl()
{
    local host=$1
    curl_value=`curl -s http://${host}:${SPARK_MASTER_WEBUI_PORT}`
}

get_master_status()
{ 
    echo "$curl_value" | grep "Status:"| awk -F "/strong>" '{print $2}'| awk -F "</" '{print $1}'| tr -d " "
}

get_spark_masters()
{
    local spark_master1=`echo "${SPARK_MASTER}" | awk -F "://" '{print $2}' | awk -F "," '{print $1}' | awk -F ":" '{print $1}'`
    local spark_master2=`echo "${SPARK_MASTER}" | awk -F "://" '{print $2}' | awk -F "," '{print $2}' | awk -F ":" '{print $1}'`
    
    echo "${spark_master1} ${spark_master2}"
}

get_active_master_curl()
{
    local spark_masters=$(get_spark_masters)
    
    for host in ${spark_masters[@]}
    do
        get_master_curl $host
        if [ "`get_master_status`" == "ALIVE" ]; then
            active_master=$host
            curl_value_alive="$curl_value"
            return
        fi
    done 
}

get_app_state()
{
    echo $curl_value_alive | sed -n 's/\(.*\)Running Applications\(.*\)Completed Applications\(.*\)/\2/p' | grep "$app_id" > /dev/null
    
    if [ $? -eq 0 ];then
        app_state="RUNNING"
    else
        app_state="COMPLETED"
    fi
}

kill_executor()
{
    local arr_length=${#executor_pid_array[*]}
    
    if [ $arr_length -le 0 ]; then
        return
    fi
    
    # sleep 10 seconds, wait for app to kill the executor
    sleep 10
    
    for((i=0;i<$arr_length;i++))
    do
        local pid=${executor_pid_array[$i]}
        ps -ef | grep "CoarseGrainedExecutorBackend" | grep -v grep | awk '{print $2}' | grep -w "$pid" > /dev/null
        if [ $? -eq 0 ];then
            my_kill_print $i
            kill -9 $pid
        fi
    done
}

my_kill_print()
{
    local index=$1
    
    if [ -f "$LOG_FILE" ]; then
        local logSize=`ls -l "$LOG_FILE" | awk '{print $5}'`
        if [ $logSize -gt $LOG_FILE_SIZE ]; then
            > "$LOG_FILE"
        fi
    fi
    
    print_log "========================================"
    print_log "Date: `date`"
    print_log "Active Master: ${active_master_array[$index]}"
    print_log "Application Id: ${app_id_array[$index]}"
    print_log "Executor Id: ${executor_id_array[$index]}"
    print_log "Application State: ${app_state_array[$index]}"
    print_log "Kill Executor: ${executor_pid_array[$index]}"
}

main()
{
    # 将命令的结果传给一个变量
    OUTFILE=`ps -ef|grep CoarseGrainedExecutorBackend | grep -v grep`
    
    sleep 3
    
    while read line
    do
        # get pid of Executor
        get_executor_pid "$line"
        
        # get app-id of Executor
        get_executor_appid "$line"
        
        # get executor-id of Executor
        get_executor_id "$line"
        
        if [ -z "$app_id" ]; then
            continue
        fi
        
        # get curl value of active master
        get_active_master_curl
        
        if [ -z "${curl_value_alive}" ]; then
            break
        fi
        
        # get state of the application on active spark master
        get_app_state $app_id
        
        if [ "$app_state" == "COMPLETED" ]; then
            # 放入数组等待强杀
            active_master_array=(${active_master_array[*]} $active_master)
            app_id_array=(${app_id_array[*]} $app_id)
            app_state_array=(${app_state_array[*]} $app_state)
            executor_pid_array=(${executor_pid_array[*]} $executor_pid)
            executor_id_array=(${executor_id_array[*]} $executor_id)
        fi
    done <<EOF
    
    # 将该变量作为该循环的HERE文档输入。
    $OUTFILE
EOF
    
    kill_executor
    
}


if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

LOG_FILE="$SPARK_HOME"/kill_executor_forcibly.log
LOG_FILE_SIZE=104857600

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"


if [ "$SPARK_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_MASTER_WEBUI_PORT=8080
fi

SPARK_MASTER=`get_propval ${SPARK_CONF_DIR}/spark-defaults.conf "spark.master"`

if [[ "$SPARK_MASTER" != "spark://"* ]]; then
    print_log "spark.master is not start with 'spark://'."
    exit 1
fi

curl_value=""
curl_value_alive=""
active_master=""
executor_pid=""
app_id=""
executor_id=""
app_state=""

unset active_master_array
unset executor_pid_array
unset app_id_array
unset executor_id_array
unset app_state_array

main
