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
import errno, sys, os, traceback, stat, socket, re, warnings, signal

from hodlib.Common.tcp import tcpSocket, tcpError 
from hodlib.Common.threads import simpleCommand

setUGV   = { 'S_ISUID' : 2, 'S_ISGID' : 1, 'S_ISVTX' : 0 }
reEscapeSeq = r"\\(.)?"
reEscapeSeq = re.compile(reEscapeSeq)

HOD_INTERRUPTED_CODE = 127
HOD_INTERRUPTED_MESG = "Hod interrupted. Cleaning up and exiting"
TORQUE_USER_LIMITS_COMMENT_FIELD = "User-limits exceeded. " + \
        "Requested:([0-9]*) Used:([0-9]*) MaxLimit:([0-9]*)"
TORQUE_USER_LIMITS_EXCEEDED_MSG = "Requested number of nodes exceeded " + \
                                  "maximum user limits. "

class AlarmException(Exception):
    def __init__(self, msg=''):
        self.message = msg
        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

def isProcessRunning(pid):
    '''Check if a process is running, by sending it a 0 signal, and checking for errors'''
    # This method is documented in some email threads on the python mailing list.
    # For e.g.: http://mail.python.org/pipermail/python-list/2002-May/144522.html
    try:
      os.kill(pid, 0)
      return True
    except OSError, err:
      return err.errno == errno.EPERM

def untar(file, targetDir):
    status = False
    command = 'tar -C %s -zxf %s' % (targetDir, file)
    commandObj = simpleCommand('untar', command)
    commandObj.start()
    commandObj.wait()
    commandObj.join()
    if commandObj.exit_code() == 0:
        status = True
        
    return status

def tar(tarFile, tarDirectory, tarList):
    currentDir = os.getcwd()
    os.chdir(tarDirectory)
    status = False
    command = 'tar -czf %s ' % (tarFile)

    for file in tarList:
        command = "%s%s " % (command, file)
    
    commandObj = simpleCommand('tar', command)
    commandObj.start()
    commandObj.wait()
    commandObj.join()
    if commandObj.exit_code() == 0:
        status = True
    else:
        status = commandObj.exit_status_string()
    
    os.chdir(currentDir)
        
    return status
  
def to_http_url(list):
    """convert [hostname, port]  to a http url""" 
    str = ''
    str = "http://%s:%s" % (list[0], list[1])
    
    return str

def get_exception_string():
    (type, value, tb) = sys.exc_info()
    exceptList = traceback.format_exception(type, value, tb)
    exceptString = ''
    for line in exceptList:
        exceptString = "%s%s" % (exceptString, line)
    
    return exceptString
  
def get_exception_error_string():
  (type, value, tb) = sys.exc_info()
  if value:
    exceptString = "%s %s" % (type, value)
  else:
    exceptString = type
    
  return exceptString

def check_timestamp(timeStamp):
    """ Checks the validity of a timeStamp.

        timeStamp - (YYYY-MM-DD HH:MM:SS in UTC)

        returns True or False
    """
    isValid = True

    try:
        timeStruct = time.strptime(timeStamp, "%Y-%m-%d %H:%M:%S")
    except:
        isValid = False

    return isValid

def sig_wrapper(sigNum, handler, *args):
  if args:
      handler(args)
  else:
      handler()
      
def get_perms(filename):
    mode = stat.S_IMODE(os.stat(filename)[stat.ST_MODE])
    permsString = ''
    permSet = 0
    place = 2
    for who in "USR", "GRP", "OTH":
        for what in "R", "W", "X":
            if mode & getattr(stat,"S_I"+what+who):
                permSet = permSet + 2**place
            place = place - 1

        permsString = "%s%s" % (permsString, permSet)
        permSet = 0
        place = 2

    permSet = 0
    for permFlag in setUGV.keys():
        if mode & getattr(stat, permFlag):
            permSet = permSet + 2**setUGV[permFlag]

    permsString = "%s%s" % (permSet, permsString)

    return permsString

def local_fqdn():
    """Return a system's true FQDN rather than any aliases, which are
       occasionally returned by socket.gethostname."""

    fqdn = None
    me = os.uname()[1]
    nameInfo=socket.gethostbyname_ex(me)
    nameInfo[1].append(nameInfo[0])
    for name in nameInfo[1]:
        if name.count(".") and name.startswith(me):
            fqdn = name
    if fqdn == None:
        fqdn = me
    return(fqdn)
  
def need_to_allocate(allocated, config, command):
    status = True
    
    if allocated.isSet():
        status = False
    elif re.search("\s*dfs.*$", command) and \
        config['gridservice-hdfs']['external']:    
        status = False
    elif config['gridservice-mapred']['external']:    
        status = False
        
    return status
  
def filter_warnings():
    warnings.filterwarnings('ignore',
        message=".*?'with' will become a reserved keyword.*")
    
def args_to_string(list):
  """return a string argument space seperated"""
  arg = ''
  for item in list:
    arg = "%s%s " % (arg, item)
  return arg[:-1]

def replace_escapes(object):
  """ replace any escaped character. e.g \, with , \= with = and so on """
  # here object is either a config object or a options object
  for section in object._mySections:
    for option in object._configDef[section].keys():
      if object[section].has_key(option):
        if object._configDef[section][option]['type'] == 'keyval':
          keyValDict = object[section][option]
          object[section][option] = {}
          for (key,value) in keyValDict.iteritems():
            match = reEscapeSeq.search(value)
            if match:
              value = reEscapeSeq.sub(r"\1", value)
            object[section][option][key] = value

def hadoopVersion(hadoopDir, java_home, log):
  # Determine the version of hadoop being used by executing the 
  # hadoop version command. Code earlier in idleTracker.py
  hadoopVersion = { 'major' : None, 'minor' : None }
  hadoopPath = os.path.join(hadoopDir, 'bin', 'hadoop')
  cmd = "%s version" % hadoopPath
  log.debug('Executing command %s to find hadoop version' % cmd)
  env = os.environ
  env['JAVA_HOME'] = java_home
  hadoopVerCmd = simpleCommand('HadoopVersion', cmd, env)
  hadoopVerCmd.start()
  hadoopVerCmd.wait()
  hadoopVerCmd.join()
  if hadoopVerCmd.exit_code() == 0:
    verLine = hadoopVerCmd.output()[0]
    log.debug('Version from hadoop command: %s' % verLine)
    hadoopVerRegExp = re.compile("Hadoop ([0-9]+)\.([0-9]+).*")
    verMatch = hadoopVerRegExp.match(verLine)
    if verMatch != None:
      hadoopVersion['major'] = verMatch.group(1)
      hadoopVersion['minor'] = verMatch.group(2)
  return hadoopVersion


def get_cluster_status(hdfsAddress, mapredAddress):
  """Determine the status of the cluster based on socket availability
     of HDFS and Map/Reduce."""
  status = 0

  mapredSocket = tcpSocket(mapredAddress)
  try:
    mapredSocket.open()
    mapredSocket.close()
  except tcpError:
    status = 14

  hdfsSocket = tcpSocket(hdfsAddress)
  try:
    hdfsSocket.open()
    hdfsSocket.close()
  except tcpError:
    if status > 0:
      status = 10
    else:
      status = 13

  return status

def parseEquals(list):
  # takes in a list of keyval pairs e.g ['a=b','c=d'] and returns a
  # dict e.g {'a'='b','c'='d'}. Used in GridService/{mapred.py/hdfs.py} and 
  # HodRing/hodring.py. No need for specially treating escaped =. as in \=,
  # since all keys are generated by hod and don't contain such anomalies
  dict = {}
  for elems in list:
    splits = elems.split('=')
    dict[splits[0]] = splits[1]
  return dict

def getMapredSystemDirectory(mrSysDirRoot, userid, jobid):
  return os.path.join(mrSysDirRoot, userid, 'mapredsystem', jobid)

class HodInterrupt:
  def __init__(self):
    self.HodInterruptFlag = False
    self.log = None

  def set_log(self, log):
    self.log = log

  def init_signals(self):

    def sigStop(sigNum, handler):
      sig_wrapper(sigNum, self.setFlag)

    signal.signal(signal.SIGTERM, sigStop) # 15 : software termination signal
    signal.signal(signal.SIGQUIT, sigStop) # 3  : Quit program
    signal.signal(signal.SIGINT, sigStop)  # 2 ^C : Interrupt program

    def sig_wrapper(sigNum, handler, *args):
      self.log.critical("Caught signal %s." % sigNum )

      if args:
          handler(args)
      else:
          handler()

  def setFlag(self, val = True):
    self.HodInterruptFlag = val

  def isSet(self):
    return self.HodInterruptFlag

class HodInterruptException(Exception):
  def __init__(self, value = ""):
    self.value = value
    
  def __str__(self):
    return repr(self.value)

hodInterrupt = HodInterrupt()
