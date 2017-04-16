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
# -*- python -*-

import sys, os, getpass, pprint, re, cPickle, random, shutil, time, errno

import hodlib.Common.logger

from hodlib.ServiceRegistry.serviceRegistry import svcrgy
from hodlib.Common.xmlrpc import hodXRClient
from hodlib.Common.util import to_http_url, get_exception_string
from hodlib.Common.util import get_exception_error_string
from hodlib.Common.util import hodInterrupt, HodInterruptException
from hodlib.Common.util import HOD_INTERRUPTED_CODE

from hodlib.Common.nodepoolutil import NodePoolUtil
from hodlib.Hod.hadoop import hadoopCluster, hadoopScript

CLUSTER_DATA_FILE = 'clusters'
INVALID_STATE_FILE_MSGS = \
              [

                "Requested operation cannot be performed. Cannot read %s: " + \
                "Permission denied.",

                "Requested operation cannot be performed. " + \
                "Cannot write to %s: Permission denied.",

                "Requested operation cannot be performed. " + \
                "Cannot read/write to %s: Permission denied.",

                "Cannot update %s: Permission denied. " + \
                "Cluster is deallocated, but info and list " + \
                "operations might show incorrect information.",

              ]

class hodState:
  def __init__(self, store):
    self.__store = store
    self.__stateFile = None
    self.__init_store()
    self.__STORE_EXT = ".state"
   
  def __init_store(self):
    if not os.path.exists(self.__store):
      os.mkdir(self.__store)
  
  def __set_state_file(self, id=None):
    if id:
      self.__stateFile = os.path.join(self.__store, "%s%s" % (id, 
                                      self.__STORE_EXT))
    else:
      for item in os.listdir(self.__store):
        if item.endswith(self.__STORE_EXT):  
          self.__stateFile = os.path.join(self.__store, item)          

  def get_state_file(self):
    return self.__stateFile
          
  def checkStateFile(self, id=None, modes=(os.R_OK,)):
    # is state file exists/readable/writable/both?
    self.__set_state_file(id)

    # return true if file doesn't exist, because HOD CAN create
    # state file and so WILL have permissions to read and/or write
    try:
      os.stat(self.__stateFile)
    except OSError, err:
      if err.errno == errno.ENOENT: # error 2 (no such file)
        return True

    # file exists
    ret = True
    for mode in modes:
      ret = ret and os.access(self.__stateFile, mode)
    return ret

  def read(self, id=None):
    info = {}
    
    self.__set_state_file(id)
  
    if self.__stateFile:
      if os.path.isfile(self.__stateFile):
        stateFile = open(self.__stateFile, 'r')
        try:
          info = cPickle.load(stateFile)
        except EOFError:
          pass
        
        stateFile.close()
    
    return info
           
  def write(self, id, info):
    self.__set_state_file(id)
    if not os.path.exists(self.__stateFile):
      self.clear(id)
 
    stateFile = open(self.__stateFile, 'w')
    cPickle.dump(info, stateFile)
    stateFile.close()
  
  def clear(self, id=None):
    self.__set_state_file(id)
    if self.__stateFile and os.path.exists(self.__stateFile):
      os.remove(self.__stateFile)
    else:
      for item in os.listdir(self.__store):
        if item.endswith(self.__STORE_EXT):
          os.remove(item)
        
class hodRunner:

  def __init__(self, cfg, log=None, cluster=None):
    self.__hodhelp = hodHelp()
    self.__ops = self.__hodhelp.ops
    self.__cfg = cfg  
    self.__npd = self.__cfg['nodepooldesc']
    self.__opCode = 0
    self.__user = getpass.getuser()
    self.__registry = None
    self.__baseLogger = None
    # Allowing to pass in log object to help testing - a stub can be passed in
    if log is None:
      self.__setup_logger()
    else:
      self.__log = log
    
    self.__userState = hodState(self.__cfg['hod']['user_state']) 
    
    self.__clusterState = None
    self.__clusterStateInfo = { 'env' : None, 'hdfs' : None, 'mapred' : None }
    
    # Allowing to pass in log object to help testing - a stib can be passed in
    if cluster is None:
      self.__cluster = hadoopCluster(self.__cfg, self.__log)
    else:
      self.__cluster = cluster
  
  def __setup_logger(self):
    self.__baseLogger = hodlib.Common.logger.hodLog('hod')
    self.__log = self.__baseLogger.add_logger(self.__user )
 
    if self.__cfg['hod']['stream']:
      self.__baseLogger.add_stream(level=self.__cfg['hod']['debug'], 
                            addToLoggerNames=(self.__user ,))
  
    if self.__cfg['hod'].has_key('syslog-address'):
      self.__baseLogger.add_syslog(self.__cfg['hod']['syslog-address'], 
                                   level=self.__cfg['hod']['debug'], 
                                   addToLoggerNames=(self.__user ,))

  def get_logger(self):
    return self.__log

  def __setup_cluster_logger(self, directory):
    self.__baseLogger.add_file(logDirectory=directory, level=4,
                          backupCount=self.__cfg['hod']['log-rollover-count'],
                          addToLoggerNames=(self.__user ,))

  def __setup_cluster_state(self, directory):
    self.__clusterState = hodState(directory)

  def __norm_cluster_dir(self, directory):
    directory = os.path.expanduser(directory)
    if not os.path.isabs(directory):
      directory = os.path.join(self.__cfg['hod']['original-dir'], directory)
    directory = os.path.abspath(directory)
    
    return directory
  
  def __setup_service_registry(self):
    cfg = self.__cfg['hod'].copy()
    cfg['debug'] = 0
    self.__registry = svcrgy(cfg, self.__log)
    self.__registry.start()
    self.__log.debug(self.__registry.getXMLRPCAddr())
    self.__cfg['hod']['xrs-address'] = self.__registry.getXMLRPCAddr()
    self.__cfg['ringmaster']['svcrgy-addr'] = self.__cfg['hod']['xrs-address']

  def __set_cluster_state_info(self, env, hdfs, mapred, ring, jobid, min, max):
    self.__clusterStateInfo['env'] = env
    self.__clusterStateInfo['hdfs'] = "http://%s" % hdfs
    self.__clusterStateInfo['mapred'] = "http://%s" % mapred
    self.__clusterStateInfo['ring'] = ring
    self.__clusterStateInfo['jobid'] = jobid
    self.__clusterStateInfo['min'] = min
    self.__clusterStateInfo['max'] = max
    
  def __set_user_state_info(self, info):
    userState = self.__userState.read(CLUSTER_DATA_FILE)
    for key in info.keys():
      userState[key] = info[key]
      
    self.__userState.write(CLUSTER_DATA_FILE, userState)  

  def __remove_cluster(self, clusterDir):
    clusterInfo = self.__userState.read(CLUSTER_DATA_FILE)
    if clusterDir in clusterInfo:
      del(clusterInfo[clusterDir])
      self.__userState.write(CLUSTER_DATA_FILE, clusterInfo)
      
  def __cleanup(self):
    if self.__registry: self.__registry.stop()
    
  def __check_operation(self, operation):    
    opList = operation.split()
    
    if not opList[0] in self.__ops:
      self.__log.critical("Invalid hod operation specified: %s" % operation)
      self._op_help(None)
      self.__opCode = 2
         
    return opList 
  
  def __adjustMasterFailureCountConfig(self, nodeCount):
    # This method adjusts the ringmaster.max-master-failures variable
    # to a value that is bounded by the a function of the number of
    # nodes.

    maxFailures = self.__cfg['ringmaster']['max-master-failures']
    # Count number of masters required - depends on which services
    # are external
    masters = 0
    if not self.__cfg['gridservice-hdfs']['external']:
      masters += 1
    if not self.__cfg['gridservice-mapred']['external']:
      masters += 1

    # So, if there are n nodes and m masters, we look atleast for
    # all masters to come up. Therefore, atleast m nodes should be
    # good, which means a maximum of n-m master nodes can fail.
    maxFailedNodes = nodeCount - masters

    # The configured max number of failures is now bounded by this
    # number.
    self.__cfg['ringmaster']['max-master-failures'] = \
                              min(maxFailures, maxFailedNodes)

  def _op_allocate(self, args):
    operation = "allocate"
    argLength = len(args)
    min = 0
    max = 0
    errorFlag = False
    errorMsgs = []

    if argLength == 3:
      nodes = args[2]
      clusterDir = self.__norm_cluster_dir(args[1])

      if not os.path.exists(clusterDir):
        try:
          os.makedirs(clusterDir)
        except OSError, err:
          errorFlag = True
          errorMsgs.append("Could not create cluster directory. %s" \
                            % (str(err)))
      elif not os.path.isdir(clusterDir):
        errorFlag = True
        errorMsgs.append( \
                    "Invalid cluster directory (--hod.clusterdir or -d) : " + \
                         clusterDir + " : Not a directory")
        
      if int(nodes) < 3 :
        errorFlag = True
        errorMsgs.append("Invalid nodecount (--hod.nodecount or -n) : " + \
                         "Must be >= 3. Given nodes: %s" % nodes)
      if errorFlag:
        for msg in errorMsgs:
          self.__log.critical(msg)
        self.__opCode = 3
        return

      if not self.__userState.checkStateFile(CLUSTER_DATA_FILE, \
                                              (os.R_OK, os.W_OK)):
        self.__log.critical(INVALID_STATE_FILE_MSGS[2] % \
                         self.__userState.get_state_file())
        self.__opCode = 1
        return

      clusterList = self.__userState.read(CLUSTER_DATA_FILE)
      if clusterDir in clusterList.keys():
        self.__setup_cluster_state(clusterDir)
        clusterInfo = self.__clusterState.read()
        # Check if the job is not running. Only then can we safely
        # allocate another cluster. Otherwise the user would need
        # to deallocate and free up resources himself.
        if clusterInfo.has_key('jobid') and \
            self.__cluster.is_cluster_deallocated(clusterInfo['jobid']):
          self.__log.warn("Found a dead cluster at cluster directory '%s'. Deallocating it to allocate a new one." % (clusterDir))
          self.__remove_cluster(clusterDir)
          self.__clusterState.clear()
        else:
          self.__log.critical("Found a previously allocated cluster at cluster directory '%s'. HOD cannot determine if this cluster can be automatically deallocated. Deallocate the cluster if it is unused." % (clusterDir))
          self.__opCode = 12
          return
 
      self.__setup_cluster_logger(clusterDir)

      (status, message) = self.__cluster.is_valid_account()
      if status is not 0:
        if message:
          for line in message:
            self.__log.critical("verify-account output: %s" % line)
        self.__log.critical("Cluster cannot be allocated because account verification failed. " \
                              + "verify-account returned exit code: %s." % status)
        self.__opCode = 4
        return
      else:
        self.__log.debug("verify-account returned zero exit code.")
        if message:
          self.__log.debug("verify-account output: %s" % message)

      if re.match('\d+-\d+', nodes):
        (min, max) = nodes.split("-")
        min = int(min)
        max = int(max)
      else:
        try:
          nodes = int(nodes)
          min = nodes
          max = nodes
        except ValueError:
          print self.__hodhelp.help(operation)
          self.__log.critical(
          "%s operation requires a pos_int value for n(nodecount)." % 
          operation)
          self.__opCode = 3
        else:
          self.__setup_cluster_state(clusterDir)
          clusterInfo = self.__clusterState.read()
          self.__opCode = self.__cluster.check_cluster(clusterInfo)
          if self.__opCode == 0 or self.__opCode == 15:
            self.__setup_service_registry()   
            if hodInterrupt.isSet(): 
              self.__cleanup()
              raise HodInterruptException()
            self.__log.debug("Service Registry started.")

            self.__adjustMasterFailureCountConfig(nodes)
            
            try:
              allocateStatus = self.__cluster.allocate(clusterDir, min, max)    
            except HodInterruptException, h:
              self.__cleanup()
              raise h
            # Allocation has gone through.
            # Don't care about interrupts any more

            try:
              if allocateStatus == 0:
                self.__set_cluster_state_info(os.environ, 
                                              self.__cluster.hdfsInfo, 
                                              self.__cluster.mapredInfo, 
                                              self.__cluster.ringmasterXRS,
                                              self.__cluster.jobId,
                                              min, max)
                self.__setup_cluster_state(clusterDir)
                self.__clusterState.write(self.__cluster.jobId, 
                                          self.__clusterStateInfo)
                #  Do we need to check for interrupts here ??
  
                self.__set_user_state_info( 
                  { clusterDir : self.__cluster.jobId, } )
              self.__opCode = allocateStatus
            except Exception, e:
              # Some unknown problem.
              self.__cleanup()
              self.__cluster.deallocate(clusterDir, self.__clusterStateInfo)
              self.__opCode = 1
              raise Exception(e)
          elif self.__opCode == 12:
            self.__log.critical("Cluster %s already allocated." % clusterDir)
          elif self.__opCode == 10:
            self.__log.critical("dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                  clusterDir))
          elif self.__opCode == 13:
            self.__log.warn("hdfs dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                       clusterDir))
          elif self.__opCode == 14:
            self.__log.warn("mapred dead\t%s\t%s" % (clusterInfo['jobid'], 
                                                     clusterDir))   
          
          if self.__opCode > 0 and self.__opCode != 15:
            self.__log.critical("Cannot allocate cluster %s" % clusterDir)
    else:
      print self.__hodhelp.help(operation)
      self.__log.critical("%s operation requires two arguments. "  % operation
                        + "A cluster directory and a nodecount.")
      self.__opCode = 3
 
  def _is_cluster_allocated(self, clusterDir):
    if os.path.isdir(clusterDir):
      self.__setup_cluster_state(clusterDir)
      clusterInfo = self.__clusterState.read()
      if clusterInfo != {}:
        return True
    return False

  def _op_deallocate(self, args):
    operation = "deallocate"
    argLength = len(args)
    if argLength == 2:
      clusterDir = self.__norm_cluster_dir(args[1])
      if os.path.isdir(clusterDir):
        self.__setup_cluster_state(clusterDir)
        clusterInfo = self.__clusterState.read()
        if clusterInfo == {}:
          self.__handle_invalid_cluster_directory(clusterDir, cleanUp=True)
        else:
          self.__opCode = \
            self.__cluster.deallocate(clusterDir, clusterInfo)
          # irrespective of whether deallocate failed or not\
          # remove the cluster state.
          self.__clusterState.clear()
          if not self.__userState.checkStateFile(CLUSTER_DATA_FILE, (os.W_OK,)):
            self.__log.critical(INVALID_STATE_FILE_MSGS[3] % \
                               self.__userState.get_state_file())
            self.__opCode = 1
            return
          self.__remove_cluster(clusterDir)
      else:
        self.__handle_invalid_cluster_directory(clusterDir, cleanUp=True)
    else:
      print self.__hodhelp.help(operation)
      self.__log.critical("%s operation requires one argument. "  % operation
                        + "A cluster path.")
      self.__opCode = 3
            
  def _op_list(self, args):
    operation = 'list'
    clusterList = self.__userState.read(CLUSTER_DATA_FILE)
    for path in clusterList.keys():
      if not os.path.isdir(path):
        self.__log.info("cluster state unknown\t%s\t%s" % (clusterList[path], path))
        continue
      self.__setup_cluster_state(path)
      clusterInfo = self.__clusterState.read()
      if clusterInfo == {}:
        # something wrong with the cluster directory.
        self.__log.info("cluster state unknown\t%s\t%s" % (clusterList[path], path))
        continue
      clusterStatus = self.__cluster.check_cluster(clusterInfo)
      if clusterStatus == 12:
        self.__log.info("alive\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 10:
        self.__log.info("dead\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 13:
        self.__log.info("hdfs dead\t%s\t%s" % (clusterList[path], path))
      elif clusterStatus == 14:
        self.__log.info("mapred dead\t%s\t%s" % (clusterList[path], path))    
         
  def _op_info(self, args):
    operation = 'info'
    argLength = len(args)  
    if argLength == 2:
      clusterDir = self.__norm_cluster_dir(args[1])
      if os.path.isdir(clusterDir):
        self.__setup_cluster_state(clusterDir)
        clusterInfo = self.__clusterState.read()
        if clusterInfo == {}:
          # something wrong with the cluster directory.
          self.__handle_invalid_cluster_directory(clusterDir)
        else:
          clusterStatus = self.__cluster.check_cluster(clusterInfo)
          if clusterStatus == 12:
            self.__print_cluster_info(clusterInfo)
            self.__log.info("hadoop-site.xml at %s" % clusterDir)
          elif clusterStatus == 10:
            self.__log.critical("%s cluster is dead" % clusterDir)
          elif clusterStatus == 13:
            self.__log.warn("%s cluster hdfs is dead" % clusterDir)
          elif clusterStatus == 14:
            self.__log.warn("%s cluster mapred is dead" % clusterDir)

          if clusterStatus != 12:
            if clusterStatus == 15:
              self.__log.critical("Cluster %s not allocated." % clusterDir)
            else:
              self.__print_cluster_info(clusterInfo)
              self.__log.info("hadoop-site.xml at %s" % clusterDir)
            
            self.__opCode = clusterStatus
      else:
        self.__handle_invalid_cluster_directory(clusterDir)
    else:
      print self.__hodhelp.help(operation)
      self.__log.critical("%s operation requires one argument. "  % operation
                        + "A cluster path.")
      self.__opCode = 3      

  def __handle_invalid_cluster_directory(self, clusterDir, cleanUp=False):
    if not self.__userState.checkStateFile(CLUSTER_DATA_FILE, (os.R_OK,)):
      self.__log.critical(INVALID_STATE_FILE_MSGS[0] % \
                           self.__userState.get_state_file())
      self.__opCode = 1
      return

    clusterList = self.__userState.read(CLUSTER_DATA_FILE)
    if clusterDir in clusterList.keys():
      # previously allocated cluster.
      self.__log.critical("Cannot find information for cluster with id '%s' in previously allocated cluster directory '%s'." % (clusterList[clusterDir], clusterDir))
      if cleanUp:
        self.__cluster.delete_job(clusterList[clusterDir])
        self.__log.critical("Freeing resources allocated to the cluster.")
        if not self.__userState.checkStateFile(CLUSTER_DATA_FILE, (os.W_OK,)):
          self.__log.critical(INVALID_STATE_FILE_MSGS[1] % \
                              self.__userState.get_state_file())
          self.__opCode = 1
          return
        self.__remove_cluster(clusterDir)
      self.__opCode = 3
    else:
      if not os.path.exists(clusterDir):
        self.__log.critical(  \
                  "Invalid hod.clusterdir(--hod.clusterdir or -d). " + \
                  clusterDir + " : No such directory")
      elif not os.path.isdir(clusterDir):
        self.__log.critical( \
                  "Invalid hod.clusterdir(--hod.clusterdir or -d). " + \
                  clusterDir + " : Not a directory")
      else:
        self.__log.critical( \
                  "Invalid hod.clusterdir(--hod.clusterdir or -d). " + \
                  clusterDir + " : Not tied to any allocated cluster.")
      self.__opCode = 15
    
  def __print_cluster_info(self, clusterInfo):
    keys = clusterInfo.keys()

    _dict = { 
              'jobid' : 'Cluster Id', 'min' : 'Nodecount',
              'hdfs' : 'HDFS UI at' , 'mapred' : 'Mapred UI at'
            }

    for key in _dict.keys():
      if clusterInfo.has_key(key):
        self.__log.info("%s %s" % (_dict[key], clusterInfo[key]))

    if clusterInfo.has_key('ring'):
      self.__log.debug("%s\t%s" % ('Ringmaster at ', clusterInfo['ring']))
    
    if self.__cfg['hod']['debug'] == 4:
      for var in clusterInfo['env'].keys():
        self.__log.debug("%s = %s" % (var, clusterInfo['env'][var]))

  def _op_help(self, arg):
    if arg == None or arg.__len__() != 2:
      print "hod commands:\n"
      for op in self.__ops:
        print self.__hodhelp.help(op)
    else:
      if arg[1] not in self.__ops:
        print self.__hodhelp.help('help')
        self.__log.critical("Help requested for invalid operation : %s"%arg[1])
        self.__opCode = 3
      else: print self.__hodhelp.help(arg[1])

  def operation(self):  
    operation = self.__cfg['hod']['operation']
    try:
      opList = self.__check_operation(operation)
      if self.__opCode == 0:
        if not self.__userState.checkStateFile(CLUSTER_DATA_FILE, (os.R_OK,)):
           self.__log.critical(INVALID_STATE_FILE_MSGS[0] % \
                         self.__userState.get_state_file())
           self.__opCode = 1
           return self.__opCode
        getattr(self, "_op_%s" % opList[0])(opList)
    except HodInterruptException, h:
      self.__log.critical("op: %s failed because of a process interrupt." \
                                                                % operation)
      self.__opCode = HOD_INTERRUPTED_CODE
    except:
      self.__log.critical("op: %s failed: %s" % (operation,
                          get_exception_error_string()))
      self.__log.debug(get_exception_string())
    
    self.__cleanup()
    
    self.__log.debug("return code: %s" % self.__opCode)
    
    return self.__opCode
  
  def script(self):
    errorFlag = False
    errorMsgs = []
    scriptRet = 0 # return from the script, if run
    
    script = self.__cfg['hod']['script']
    nodes = self.__cfg['hod']['nodecount']
    clusterDir = self.__cfg['hod']['clusterdir']
    
    if not os.path.exists(script):
      errorFlag = True
      errorMsgs.append("Invalid script file (--hod.script or -s) : " + \
                       script + " : No such file")
    elif not os.path.isfile(script):
      errorFlag = True
      errorMsgs.append("Invalid script file (--hod.script or -s) : " + \
                       script + " : Not a file.")
    else:
      isExecutable = os.access(script, os.X_OK)
      if not isExecutable:
        errorFlag = True
        errorMsgs.append("Invalid script file (--hod.script or -s) : " + \
                         script + " : Not an executable.")

    if not os.path.exists(clusterDir):
      try:
        os.makedirs(clusterDir)
      except OSError, err:
        errorFlag = True
        errorMsgs.append("Could not create cluster directory. %s" % (str(err)))
    elif not os.path.isdir(clusterDir):
      errorFlag = True
      errorMsgs.append( \
                  "Invalid cluster directory (--hod.clusterdir or -d) : " + \
                       clusterDir + " : Not a directory")

    if int(self.__cfg['hod']['nodecount']) < 3 :
      errorFlag = True
      errorMsgs.append("Invalid nodecount (--hod.nodecount or -n) : " + \
                       "Must be >= 3. Given nodes: %s" % nodes)

    if errorFlag:
      for msg in errorMsgs:
        self.__log.critical(msg)
      self.handle_script_exit_code(scriptRet, clusterDir)
      sys.exit(3)

    try:
      self._op_allocate(('allocate', clusterDir, str(nodes)))
      if self.__opCode == 0:
        if self.__cfg['hod'].has_key('script-wait-time'):
          time.sleep(self.__cfg['hod']['script-wait-time'])
          self.__log.debug('Slept for %d time. Now going to run the script' % self.__cfg['hod']['script-wait-time'])
        if hodInterrupt.isSet():
          self.__log.debug('Hod interrupted - not executing script')
        else:
          scriptRunner = hadoopScript(clusterDir, 
                                  self.__cfg['hod']['original-dir'])
          self.__opCode = scriptRunner.run(script)
          scriptRet = self.__opCode
          self.__log.info("Exit code from running the script: %d" % self.__opCode)
      else:
        self.__log.critical("Error %d in allocating the cluster. Cannot run the script." % self.__opCode)

      if hodInterrupt.isSet():
        # Got interrupt while executing script. Unsetting it for deallocating
        hodInterrupt.setFlag(False)
      if self._is_cluster_allocated(clusterDir):
        self._op_deallocate(('deallocate', clusterDir))
    except HodInterruptException, h:
      self.__log.critical("Script failed because of a process interrupt.")
      self.__opCode = HOD_INTERRUPTED_CODE
    except:
      self.__log.critical("script: %s failed: %s" % (script,
                          get_exception_error_string()))
      self.__log.debug(get_exception_string())
    
    self.__cleanup()

    self.handle_script_exit_code(scriptRet, clusterDir)
    
    return self.__opCode

  def handle_script_exit_code(self, scriptRet, clusterDir):
    # We want to give importance to a failed script's exit code, and write out exit code to a file separately
    # so users can easily get it if required. This way they can differentiate between the script's exit code
    # and hod's exit code.
    if os.path.exists(clusterDir):
      exit_code_file_name = (os.path.join(clusterDir, 'script.exitcode'))
      if scriptRet != 0:
        exit_code_file = open(exit_code_file_name, 'w')
        print >>exit_code_file, scriptRet
        exit_code_file.close()
        self.__opCode = scriptRet
      else:
        #ensure script exit code file is not there:
        if (os.path.exists(exit_code_file_name)):
          os.remove(exit_code_file_name)

class hodHelp:
  def __init__(self):
    self.ops = ['allocate', 'deallocate', 'info', 'list','script',  'help']

    self.usage_strings = \
      {
        'allocate'   : 'hod allocate -d <clusterdir> -n <nodecount> [OPTIONS]',
        'deallocate' : 'hod deallocate -d <clusterdir> [OPTIONS]',
        'list'       : 'hod list [OPTIONS]',
        'info'       : 'hod info -d <clusterdir> [OPTIONS]',
        'script'     :
              'hod script -d <clusterdir> -n <nodecount> -s <script> [OPTIONS]',
        'help'       : 'hod help <OPERATION>',
        }

    self.description_strings = \
      {
       'allocate' : "Allocates a cluster of n nodes using the specified \n" + \
      "              cluster directory to store cluster state \n" + \
      "              information. The Hadoop site XML is also stored \n" + \
      "              in this location.\n",

       'deallocate' : "Deallocates a cluster using the specified \n" + \
      "             cluster directory.  This operation is also \n" + \
      "             required to clean up a dead cluster.\n",

       'list' : "List all clusters currently allocated by a user, \n" + \
      "              along with limited status information and the \n" + \
      "              cluster ID.\n",

       'info' : "Provide detailed information on an allocated cluster.\n",

       'script' : "Allocates a cluster of n nodes with the given \n" +\
           "              cluster directory, runs the specified script \n" + \
           "              using the allocated cluster, and then \n" + \
           "              deallocates the cluster.\n",
 
       'help' : "Print help for the operation and exit.\n" + \
                "Available operations : %s.\n" % self.ops,
       }

  def usage(self, op):
    return "Usage       : " + self.usage_strings[op] + "\n" + \
           "For full description: hod help " + op + ".\n"

  def help(self, op=None):
    if op is None:
      return "hod <operation> [ARGS] [OPTIONS]\n" + \
             "Available operations : %s\n" % self.ops + \
             "For help on a particular operation : hod help <operation>.\n" + \
             "For all options : hod help options."
    else:
      return "Usage       : " + self.usage_strings[op] + "\n" + \
             "Description : " + self.description_strings[op] + \
             "For all options : hod help options.\n"
