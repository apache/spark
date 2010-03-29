#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
import os, pprint, re, time

from hodlib.Common.threads import simpleCommand
from hodlib.Common.util import args_to_string
from hodlib.Common.logger import hodDummyLogger

reQstatLine = re.compile("^\s*(\w+)\s*=\s*(.*)\s*$")

class torqueInterface:
  def __init__(self, torqueDir, environment, log=None):
    self.__qsub = os.path.join(torqueDir, 'bin', 'qsub')
    self.__qdel = os.path.join(torqueDir, 'bin', 'qdel')
    self.__qstat = os.path.join(torqueDir, 'bin', 'qstat')
    self.__pbsNodes = os.path.join(torqueDir, 'bin', 'pbsnodes')
    self.__pbsdsh = os.path.join(torqueDir, 'bin', 'pbsdsh')
    self.__qalter = os.path.join(torqueDir, 'bin', 'qalter')
    self.__env = environment
    
    self.__log = log
    if not self.__log:
      self.__log = hodDummyLogger()
        
  def qsub(self, argList, stdinList):
    jobID = False
    exitCode = 0

    qsubCommand = "%s %s" % (self.__qsub, args_to_string(argList))
    
    self.__log.debug("qsub -> %s" % qsubCommand)
    
    qsubProcess = simpleCommand('qsub', qsubCommand, env=self.__env)
    qsubProcess.start()
    
    while qsubProcess.stdin == None:
      time.sleep(.2)

    try:
      for line in stdinList:
        self.__log.debug("qsub stdin: %s" % line)
        print >>qsubProcess.stdin, line
      qsubProcess.stdin.close()
    except IOError, i:
      # If torque's qsub is given invalid params, it fails & returns immediately
      # Check for such errors here
      # Wait for command execution to finish
      qsubProcess.wait()
      qsubProcess.join()
      output = qsubProcess.output()
      if output!=[]:
        self.__log.critical("qsub Failure : %s " % output[0].strip())
        self.__log.critical("qsub Command : %s" % qsubCommand)
      return None, qsubProcess.exit_code()

    qsubProcess.wait()
    qsubProcess.join()
    
    exitCode = qsubProcess.exit_code()
    if exitCode == 0:
      buffer = qsubProcess.output()
      jobID = buffer[0].rstrip('\n')
      self.__log.debug("qsub jobid: %s" % jobID)
    else:
      self.__log.critical("qsub error: %s" % qsubProcess.exit_status_string())    
    
    return jobID, exitCode
  
  def qstat(self, jobID):
    qstatInfo = None  
    
    qstatCommand = "%s -f -1 %s" % (self.__qstat, jobID)
    
    self.__log.debug(qstatCommand)

    qstatProcess = simpleCommand('qstat', qstatCommand, env=self.__env)
    qstatProcess.start()
    qstatProcess.wait()
    qstatProcess.join()
    
    exitCode = qstatProcess.exit_code()
    if exitCode > 0:
      self.__log.warn('qstat error: %s' % qstatProcess.exit_status_string())
    else:
      qstatInfo = {}
      for line in qstatProcess.output():
        line = line.rstrip()
        if line.find('=') != -1:
          qstatMatch = reQstatLine.match(line)
          if qstatMatch:
            key = qstatMatch.group(1)
            value = qstatMatch.group(2)
            qstatInfo[key] = value
          
      if 'exec_host' in qstatInfo:
        list = qstatInfo['exec_host'].split('+')
        addrList = []
        
        for item in list:
          [head, end] = item.split('/', 1)
          addrList.append(head)
        
        qstatInfo['exec_host'] = addrList
        
    return qstatInfo, exitCode
  
  def pbs_nodes(self, argString):
    pass
  
  def qdel(self, jobId, force=False):
    exitCode = 0
    qdel = self.__qdel
    if force:
      qdel = "%s -p %s" % (qdel, jobId)
    else:
      qdel = "%s %s" % (qdel, jobId) 

    self.__log.debug(qdel)

    qdelProcess = simpleCommand('qdel', qdel, env=self.__env)
    qdelProcess.start()
    qdelProcess.wait()
    qdelProcess.join()      
      
    exitCode = qdelProcess.exit_code()
    
    return exitCode
  
  def pbsdsh(self, arguments):
    status = None
    
    pbsdshCommand = "%s %s" % (self.__pbsdsh, args_to_string(arguments))
    
    self.__log.debug("pbsdsh command: %s" % pbsdshCommand)
    
    pbsdsh = simpleCommand('pbsdsh', pbsdshCommand, env=self.__env)
    pbsdsh.start()   

    for i in range(0, 30):
      status = pbsdsh.exit_code()
      if status:
        self.__log.error("pbsdsh failed: %s" % pbsdsh.exit_status_string())
        break  
    
    if not status: status = 0
      
    return status  

  def qalter(self, fieldName, fieldValue, jobId):
    """Update the job field with fieldName with the fieldValue.
       The fieldValue must be modifiable after the job is submitted."""

    # E.g. to alter comment: qalter -W notes='value` jobId
    qalterCmd = '%s -W %s=\"%s\" %s' % (self.__qalter, fieldName, fieldValue, jobId) 
    self.__log.debug("qalter command: %s" % qalterCmd)
    qalterProcess = simpleCommand('qalter', qalterCmd, env=self.__env)
    qalterProcess.start()
    qalterProcess.wait()
    qalterProcess.join()
    exitCode = qalterProcess.exit_code()

    return exitCode
