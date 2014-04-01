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
import os, re, time
from hodlib.Common.threads import loop, func
from hodlib.Common.threads import simpleCommand
from hodlib.Common.util import get_exception_string, hadoopVersion

class HadoopJobStatus:
  """This class represents the status of a single Hadoop job"""
  
  def __init__(self, jobId, status):
    self.__jobId = jobId
    self.__status = status

  def getJobId(self):
    return self.__jobId

  def getStatus(self):
    return self.__status

class HadoopClientException(Exception):
  """This class represents an exception that is raised when we fail in
     running the job client."""
  
  def __init__(self, errorCode):
    self.errorCode = errorCode
  
class JobTrackerMonitor:
  """This class monitors the JobTracker of an allocated cluster
     periodically to detect whether it is idle. If it is found
     to be idle for more than a configured limit, it calls back
     registered handlers who can act upon the idle cluster."""

  def __init__(self, log, idleJTHandler, interval, limit,
                      hadoopDir, javaHome, servInfoProvider):
    self.__log = log
    self.__idlenessLimit = limit
    self.__idleJobTrackerHandler = idleJTHandler
    self.__hadoopDir = hadoopDir
    hadoopPath = os.path.join(self.__hadoopDir, "bin", "hadoop")
    #hadoop directory can be from pkgs or a temp location like tarball. Verify once.
    if not os.path.exists(hadoopPath):
      raise Exception('Invalid Hadoop path specified: %s' % hadoopPath)
    self.__javaHome = javaHome
    # Note that when this object is created, we don't yet know the JT URL.
    # The service info provider will be polled until we get the URL.
    self.__serviceInfoProvider = servInfoProvider
    self.__jobCountRegExp = re.compile("([0-9]+) jobs currently running.*")
    self.__jobStatusRegExp = re.compile("(\S+)\s+(\d)\s+\d+\s+\S+$")
    self.__firstIdleTime = 0
    self.__hadoop15Version = { 'major' : '0', 'minor' : '15' }
    #Assumption: we are not going to support versions older than 0.15 for Idle Job tracker.
    if not self.__isCompatibleHadoopVersion(self.__hadoop15Version):
      raise Exception('Incompatible Hadoop Version: Cannot check status')
    self.__stopFlag = False
    self.__jtURLFinderThread = func(name='JTURLFinderThread', functionRef=self.getJobTrackerURL)
    self.__jtMonitorThread = loop(name='JTMonitorThread', functionRef=self.monitorJobTracker,
                                  sleep=interval)
    self.__jobTrackerURL = None

  def start(self):
    """This method starts a thread that will determine the JobTracker URL"""
    self.__jtURLFinderThread.start()

  def stop(self):
    self.__log.debug('Joining the monitoring thread.')
    self.__stopFlag = True
    if self.__jtMonitorThread.isAlive():
      self.__jtMonitorThread.join()
    self.__log.debug('Joined the monitoring thread.')

  def getJobTrackerURL(self):
    """This method periodically checks the service info provider for the JT URL"""
    self.__jobTrackerURL = self.__serviceInfoProvider.getServiceAddr('mapred')
    while not self.__stopFlag and not self.__isValidJobTrackerURL():
      time.sleep(10)
      if not self.__stopFlag:
        self.__jobTrackerURL = self.__serviceInfoProvider.getServiceAddr('mapred')
      else:
        break

    if self.__isValidJobTrackerURL():
      self.__log.debug('Got URL %s. Starting monitoring' % self.__jobTrackerURL)
      self.__jtMonitorThread.start()

  def monitorJobTracker(self):
    """This method is periodically called to monitor the JobTracker of the cluster."""
    try:
      if self.__isIdle():
        if self.__idleJobTrackerHandler:
          self.__log.info('Detected cluster as idle. Calling registered callback handler.')
          self.__idleJobTrackerHandler.handleIdleJobTracker()
    except:
      self.__log.debug('Exception while monitoring job tracker. %s' % get_exception_string())

  def getJobsStatus(self):
    """This method should return the status of all jobs that are run on the HOD allocated
       hadoop cluster"""
    jobStatusList = []
    try:
      hadoop16Version = { 'major' : '0', 'minor' : '16' }
      if self.__isCompatibleHadoopVersion(hadoop16Version):
        jtStatusCommand = self.__initStatusCommand(option='-list all')
        jtStatusCommand.start()
        jtStatusCommand.wait()
        jtStatusCommand.join()
        if jtStatusCommand.exit_code() == 0:
          for line in jtStatusCommand.output():
            jobStatus = self.__extractJobStatus(line)
            if jobStatus is not None:
              jobStatusList.append(jobStatus)
    except:
      self.__log.debug('Exception while getting job statuses. %s' % get_exception_string())
    return jobStatusList

  def __isValidJobTrackerURL(self):
    """This method checks that the passed in URL is not one of the special case strings
       returned by the getServiceAddr API"""
    return ((self.__jobTrackerURL != None) and (self.__jobTrackerURL != 'not found') \
              and (not self.__jobTrackerURL.startswith('Error')))

  def __extractJobStatus(self, line):
    """This method parses an output line from the job status command and creates
       the JobStatus object if there is a match"""
    jobStatus = None
    line = line.strip()
    jsMatch = self.__jobStatusRegExp.match(line)
    if jsMatch:
      jobStatus = HadoopJobStatus(jsMatch.group(1), int(jsMatch.group(2)))
    return jobStatus

  def __isIdle(self):
    """This method checks if the JobTracker is idle beyond a certain limit."""
    jobCount = 0
    err = False

    try:
      jobCount = self.__getJobCount()
    except HadoopClientException, hce:
      self.__log.debug('HadoopClientException handled in getting job count. \
                                      Error code: %s' % hce.errorCode)
      err = True

    if (jobCount==0) or err:
      if self.__firstIdleTime == 0:
        #detecting idleness for the first time
        self.__firstIdleTime = time.time()
      else:
        if ((time.time()-self.__firstIdleTime) >= self.__idlenessLimit):
          self.__log.info('Idleness limit crossed for cluster')
          return True
    else:
      # reset idleness time
      self.__firstIdleTime = 0
      
    return False

  def __getJobCount(self):
    """This method executes the hadoop job -list command and parses the output to detect
       the number of running jobs."""

    # We assume here that the poll interval is small enough to detect running jobs. 
    # If jobs start and stop within the poll interval, the cluster would be incorrectly 
    # treated as idle. Hadoop 2266 will provide a better mechanism than this.
    jobs = -1
    jtStatusCommand = self.__initStatusCommand()
    jtStatusCommand.start()
    jtStatusCommand.wait()
    jtStatusCommand.join()
    if jtStatusCommand.exit_code() == 0:
      for line in jtStatusCommand.output():
        match = self.__jobCountRegExp.match(line)
        if match:
          jobs = int(match.group(1))
    elif jtStatusCommand.exit_code() == 1:
      # for now, exit code 1 comes for any exception raised by JobClient. If hadoop gets
      # to differentiate and give more granular exit codes, we can check for those errors
      # corresponding to network errors etc.
      raise HadoopClientException(jtStatusCommand.exit_code())
    return jobs

  def __isCompatibleHadoopVersion(self, expectedVersion):
    """This method determines whether the version of hadoop being used is one that 
       is higher than the expectedVersion.
       This can be used for checking if a particular feature is available or not"""
    ver = hadoopVersion(self.__hadoopDir, self.__javaHome, self.__log)
    ret = False
  
    if (ver['major']!=None) and (int(ver['major']) >= int(expectedVersion['major'])) \
      and (ver['minor']!=None) and (int(ver['minor']) >= int(expectedVersion['minor'])):
      ret = True
    return ret

  def __initStatusCommand(self, option="-list"):
    """This method initializes the command to run to check the JT status"""
    cmd = None
    hadoopPath = os.path.join(self.__hadoopDir, 'bin', 'hadoop')
    cmdStr = "%s job -jt %s" % (hadoopPath, self.__jobTrackerURL)
    cmdStr = "%s %s" % (cmdStr, option)
    self.__log.debug('cmd str %s' % cmdStr)
    env = os.environ
    env['JAVA_HOME'] = self.__javaHome
    cmd = simpleCommand('HadoopStatus', cmdStr, env)
    return cmd
   
