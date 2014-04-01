#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Schedule FailMon execution for nodes of file hosts.list, according to
# the properties file conf/global.config.

import time
import ConfigParser
import subprocess
import threading
import random

jobs = []
username = "user"
connections = 10
failmonDir = ""
maxFiles = 100

# This class represents a thread that connects to a set of cluster
# nodes to locally execute monitoring jobs. These jobs are specified
# as a shell command in the constructor.
class sshThread (threading.Thread):

    def __init__(self, threadname, username, command, failmonDir):
        threading.Thread.__init__(self)
        self.name = threadname
        self.username = username
        self.command = command
        self.failmonDir = failmonDir
        self.hosts = []

    def addHost(self, host):
        self.hosts.append(host)
        
    def run (self):
        for host in self.hosts:
            toRun = ["ssh", self.username + "@" + host, "cd " + self.failmonDir + " ; " + self.command]
            print "Thread", self.name, "invoking command on", host, ":\t", toRun, "...",
            subprocess.check_call(toRun)
            print "Done!"

# This class represents a monitoring job. The param member is a string
# that can be passed in the '--only' list of jobs given to the Java
# class org.apache.hadoop.contrib.failmon.RunOnce for execution on a
# node.
class Job:
    def __init__(self, param, interval):
        self.param = param
        self.interval = interval
        self.counter = interval
        return

    def reset(self):
        self.counter = self.interval

# This function reads the configuration file to get the values of the
# configuration parameters.
def getJobs(file):
    global username
    global connections
    global jobs
    global failmonDir
    global maxFiles
    
    conf = ConfigParser.SafeConfigParser()
    conf.read(file)

    username = conf.get("Default", "ssh.username")
    connections = int(conf.get("Default", "max.connections"))
    failmonDir = conf.get("Default", "failmon.dir")
    maxFiles = conf.get("Default", "hdfs.files.max")
    
    # Hadoop Log
    interval = int(conf.get("Default", "log.hadoop.interval"))

    if interval != 0:
        jobs.append(Job("hadoopLog", interval))

    # System Log
    interval = int(conf.get("Default", "log.system.interval"))

    if interval != 0:
        jobs.append(Job("systemLog", interval))

    # NICs
    interval = int(conf.get("Default", "nics.interval"))

    if interval != 0:
        jobs.append(Job("nics", interval))

    # CPU
    interval = int(conf.get("Default", "cpu.interval"))

    if interval != 0:
        jobs.append(Job("cpu", interval))

    # CPU
    interval = int(conf.get("Default", "disks.interval"))

    if interval != 0:
        jobs.append(Job("disks", interval))

    # sensors
    interval = int(conf.get("Default", "sensors.interval"))

    if interval != 0:
        jobs.append(Job("sensors", interval))

    # upload
    interval = int(conf.get("Default", "upload.interval"))

    if interval != 0:
        jobs.append(Job("upload", interval))

    return


# Compute the gcd (Greatest Common Divisor) of two integerss
def GCD(a, b):
    assert isinstance(a, int)
    assert isinstance(b, int)

    while a:
        a, b = b%a, a

    return b

# Compute the gcd (Greatest Common Divisor) of a list of integers
def listGCD(joblist):
    assert isinstance(joblist, list)

    if (len(joblist) == 1):
        return joblist[0].interval

    g = GCD(joblist[0].interval, joblist[1].interval)

    for i in range (2, len(joblist)):
        g = GCD(g, joblist[i].interval)
        
    return g

# Merge all failmon files created on the HDFS into a single file
def mergeFiles():
    global username
    global failmonDir
    hostList = []
    hosts = open('./conf/hosts.list', 'r')
    for host in hosts:
        hostList.append(host.strip().rstrip())
    randomHost = random.sample(hostList, 1)
    mergeCommand = "bin/failmon.sh --mergeFiles"
    toRun = ["ssh", username + "@" + randomHost[0], "cd " + failmonDir + " ; " + mergeCommand]
    print "Invoking command on", randomHost, ":\t", mergeCommand, "...",
    subprocess.check_call(toRun)
    print "Done!"
    return

# The actual scheduling is done here
def main():
    getJobs("./conf/global.config")

    for job in jobs:
        print "Configuration: ", job.param, "every", job.interval, "seconds"
        
    globalInterval = listGCD(jobs)
        
    while True :
        time.sleep(globalInterval)
        params = []
        
        for job in jobs:
            job.counter -= globalInterval
            
            if (job.counter <= 0):
                params.append(job.param)
                job.reset()
                
        if (len(params) == 0):
            continue;
                    
        onlyStr = "--only " + params[0]
        for i in range(1, len(params)):
            onlyStr += ',' + params[i] 
                
        command = "bin/failmon.sh " + onlyStr

        # execute on all nodes
        hosts = open('./conf/hosts.list', 'r')
        threadList = []
        # create a thread for every connection
        for i in range(0, connections):
            threadList.append(sshThread(i, username, command, failmonDir))

        # assign some hosts/connections hosts to every thread
        cur = 0;
        for host in hosts:
            threadList[cur].addHost(host.strip().rstrip())
            cur += 1
            if (cur == len(threadList)):
                cur = 0    

        for ready in threadList:
            ready.start()

        for ssht in threading.enumerate():
            if ssht != threading.currentThread():
                ssht.join()

        # if an upload has been done, then maybe we need to merge the
        # HDFS files
        if "upload" in params:
            mergeFiles()

    return


if __name__ == '__main__':
    main()

