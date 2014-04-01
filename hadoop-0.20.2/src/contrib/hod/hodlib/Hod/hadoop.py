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
"""define WorkLoad as abstract interface for user job"""
# -*- python -*-

import os, time, sys, shutil, exceptions, re, threading, signal, urllib, pprint, math

from HTMLParser import HTMLParser

import xml.dom.minidom
import xml.dom.pulldom
from xml.dom import getDOMImplementation

from hodlib.Common.util import *
from hodlib.Common.xmlrpc import hodXRClient
from hodlib.Common.miniHTMLParser import miniHTMLParser
from hodlib.Common.nodepoolutil import NodePoolUtil
from hodlib.Common.tcp import tcpError, tcpSocket

reCommandDelimeterString = r"(?<!\\);"
reCommandDelimeter = re.compile(reCommandDelimeterString)

class hadoopConfig:
  def __create_xml_element(self, doc, name, value, description, final = False):
    prop = doc.createElement("property")
    nameP = doc.createElement("name")
    string = doc.createTextNode(name)
    nameP.appendChild(string)
    valueP = doc.createElement("value")
    string = doc.createTextNode(value)
    valueP.appendChild(string)
    if final:
      finalP = doc.createElement("final")
      string = doc.createTextNode("true")
      finalP.appendChild(string)
    desc = doc.createElement("description")
    string = doc.createTextNode(description)
    desc.appendChild(string)
    prop.appendChild(nameP)
    prop.appendChild(valueP)
    if final:
      prop.appendChild(finalP)
    prop.appendChild(desc)
    
    return prop

  def gen_site_conf(self, confDir, tempDir, numNodes, hdfsAddr, mrSysDir,\
             mapredAddr=None, clientParams=None, serverParams=None,\
             finalServerParams=None, clusterFactor=None):
    if not mapredAddr:
      mapredAddr = "dummy:8181"
    
    implementation = getDOMImplementation()
    doc = implementation.createDocument('', 'configuration', None)
    comment = doc.createComment(
      "This is an auto generated hadoop-site.xml, do not modify")
    topElement = doc.documentElement
    topElement.appendChild(comment)

    description = {}
    paramsDict = {  'mapred.job.tracker'    : mapredAddr , \
                    'fs.default.name'       : "hdfs://" + hdfsAddr, \
                    'hadoop.tmp.dir'        : tempDir, \
                 }

    paramsDict['mapred.system.dir'] = mrSysDir
    
    # mapred-default.xml is no longer used now.
    numred = int(math.floor(clusterFactor * (int(numNodes) - 1)))
    paramsDict['mapred.reduce.tasks'] = str(numred)
    # end

    # for all the above vars generated, set the description
    for k, v in paramsDict.iteritems():
      description[k] = 'Hod generated parameter'

    # finalservelParams
    if finalServerParams:
      for k, v in finalServerParams.iteritems():
        if not description.has_key(k):
          description[k] = "final server parameter"
          paramsDict[k] = v

    # servelParams
    if serverParams:
      for k, v in serverParams.iteritems():
        if not description.has_key(k):
          # if no final value for same param is mentioned
          description[k] = "server parameter"
          paramsDict[k] = v

    # clientParams
    if clientParams:
      for k, v in clientParams.iteritems():
        if not description.has_key(k) or description[k] == "server parameter":
          # Just add, if no final value for same param is mentioned.
          # Replace even if server param is mentioned for same config variable
          description[k] = "client-side parameter"
          paramsDict[k] = v
    
    # generate the xml elements
    for k,v in paramsDict.iteritems():
      if ( description[k] == "final server parameter" or \
                             description[k] == "Hod generated parameter" ): 
         final = True
      else: final = False
      prop = self.__create_xml_element(doc, k, v, description[k], final)
      topElement.appendChild(prop)

    siteName = os.path.join(confDir, "hadoop-site.xml")
    sitefile = file(siteName, 'w')
    print >> sitefile, topElement.toxml()
    sitefile.close()

class hadoopCluster:
  def __init__(self, cfg, log):
    self.__cfg = cfg
    self.__log = log
    self.__changedClusterParams = []
    
    self.__hostname = local_fqdn()    
    self.__svcrgyClient = None
    self.__nodePool = NodePoolUtil.getNodePool(self.__cfg['nodepooldesc'], 
                                               self.__cfg, self.__log)        
    self.__hadoopCfg = hadoopConfig()
    self.jobId = None
    self.mapredInfo = None
    self.hdfsInfo = None
    self.ringmasterXRS = None

  def __get_svcrgy_client(self):
    svcrgyUrl = to_http_url(self.__cfg['hod']['xrs-address'])
    return hodXRClient(svcrgyUrl)

  def __get_service_status(self):
    serviceData = self.__get_service_data()
    
    status = True
    hdfs = False
    mapred = False
    
    for host in serviceData.keys():
      for item in serviceData[host]:
        service = item.keys()
        if service[0] == 'hdfs.grid' and \
          self.__cfg['gridservice-hdfs']['external'] == False:
          hdfs = True
        elif service[0] == 'mapred.grid':
          mapred = True
    
    if not mapred:
      status = "mapred"
    
    if not hdfs and self.__cfg['gridservice-hdfs']['external'] == False:
      if status != True:
        status = "mapred and hdfs"
      else:
        status = "hdfs"
      
    return status
  
  def __get_service_data(self):
    registry = to_http_url(self.__cfg['hod']['xrs-address'])
    serviceData = self.__svcrgyClient.getServiceInfo(
      self.__cfg['hod']['userid'], self.__setup.np.getNodePoolId())
    
    return serviceData
  
  def __check_job_status(self):
    failureCount = 0
    status = False
    state = 'Q'
    userLimitsFirstFlag = True

    while (state=='Q') or (state==False):
      if hodInterrupt.isSet():
        raise HodInterruptException()

      jobInfo = self.__nodePool.getJobInfo()
      state = jobInfo['job_state']
      self.__log.debug('job state %s' % state)
      if state == False:
        failureCount += 1
        if (failureCount >= self.__cfg['hod']['job-status-query-failure-retries']):
          self.__log.debug('Number of retries reached max limit while querying job status')
          break
        time.sleep(self.__cfg['hod']['job-command-failure-interval'])
      elif state!='Q':
        break
      else:
        self.__log.debug('querying for job status after job-status-query-interval')
        time.sleep(self.__cfg['hod']['job-status-query-interval'])

      if self.__cfg['hod'].has_key('job-feasibility-attr') and \
                      self.__cfg['hod']['job-feasibility-attr']:
        (status, msg) = self.__isJobFeasible()
        if status == "Never":
          self.__log.critical(TORQUE_USER_LIMITS_EXCEEDED_MSG + msg + \
                "This cluster cannot be allocated now.")
          return -1
        elif status == False:
          if userLimitsFirstFlag:
            self.__log.critical(TORQUE_USER_LIMITS_EXCEEDED_MSG + msg + \
                "This cluster allocation will succeed only after other " + \
                "clusters are deallocated.")
            userLimitsFirstFlag = False
   
    if state and state != 'C':
      status = True
    
    return status

  def __isJobFeasible(self):
    return self.__nodePool.isJobFeasible()
  
  def __get_ringmaster_client(self):
    ringmasterXRS = None
   
    ringList = self.__svcrgyClient.getServiceInfo(
      self.__cfg['ringmaster']['userid'], self.__nodePool.getServiceId(), 
      'ringmaster', 'hod')

    if ringList and len(ringList):
      if isinstance(ringList, list):
        ringmasterXRS = ringList[0]['xrs']
    else:    
      count = 0
      waitTime = self.__cfg['hod']['allocate-wait-time']
  
      while count < waitTime:
        if hodInterrupt.isSet():
          raise HodInterruptException()

        ringList = self.__svcrgyClient.getServiceInfo(
          self.__cfg['ringmaster']['userid'], self.__nodePool.getServiceId(), 
          'ringmaster', 
          'hod')
        
        if ringList and len(ringList):
          if isinstance(ringList, list):        
            ringmasterXRS = ringList[0]['xrs']
        
        if ringmasterXRS is not None:
          break
        else:
          time.sleep(1)
          count = count + 1
          # check to see if the job exited by any chance in that time:
          if (count % self.__cfg['hod']['job-status-query-interval'] == 0):
            if not self.__check_job_status():
              break
    return ringmasterXRS
 
  def __init_hadoop_service(self, serviceName, xmlrpcClient):
    status = True
    serviceAddress = None
    serviceInfo = None
 
    for i in range(0, 250): 
      try:
        if hodInterrupt.isSet():
            raise HodInterruptException()

        serviceAddress = xmlrpcClient.getServiceAddr(serviceName)
        if serviceAddress:
          if serviceAddress == 'not found':
            time.sleep(1)
          # check to see if the job exited by any chance in that time:
            if ((i+1) % self.__cfg['hod']['job-status-query-interval'] == 0):
              if not self.__check_job_status():
                break
          else:
            serviceInfo = xmlrpcClient.getURLs(serviceName)           
            break 
      except HodInterruptException,h :
        raise h
      except:
        self.__log.critical("'%s': ringmaster xmlrpc error." % serviceName)
        self.__log.debug(get_exception_string())
        status = False
        break
    
    if serviceAddress == 'not found' or not serviceAddress:
      self.__log.critical("Failed to retrieve '%s' service address." % 
                          serviceName)
      status = False
    elif serviceAddress.startswith("Error: "):
      errs = serviceAddress[len("Error: "):]
      self.__log.critical("Cluster could not be allocated because of the following errors.\n%s" % \
                             errs)
      status = False
    else:
      try:
        self.__svcrgyClient.registerService(self.__cfg['hodring']['userid'], 
                                            self.jobId, self.__hostname, 
                                            serviceName, 'grid', serviceInfo)
        
      except HodInterruptException, h:
        raise h
      except:
        self.__log.critical("'%s': registry xmlrpc error." % serviceName)    
        self.__log.debug(get_exception_string())
        status = False
        
    return status, serviceAddress, serviceInfo

  def __collect_jobtracker_ui(self, dir):

     link = self.mapredInfo + "/jobtracker.jsp"
     parser = miniHTMLParser()
     parser.setBaseUrl(self.mapredInfo)
     node_cache = {}

     self.__log.debug("collect_jobtracker_ui seeded with " + link)

     def alarm_handler(number, stack):
         raise AlarmException("timeout")
       
     signal.signal(signal.SIGALRM, alarm_handler)

     input = None
     while link:
       self.__log.debug("link: %s" % link)
       # taskstats.jsp,taskdetails.jsp not included since too many to collect
       if re.search(
         "jobfailures\.jsp|jobtracker\.jsp|jobdetails\.jsp|jobtasks\.jsp", 
         link):

         for i in range(1,5):
           if hodInterrupt.isSet():
             raise HodInterruptException()
           try:
             input = urllib.urlopen(link)
             break
           except:
             self.__log.debug(get_exception_string())
             time.sleep(1)
  
         if input:
           out = None
    
           self.__log.debug("collecting " + link + "...")
           filename = re.sub(self.mapredInfo, "", link)
           filename = dir + "/"  + filename
           filename = re.sub("http://","", filename)
           filename = re.sub("[\?\&=:]","_",filename)
           filename = filename + ".html"
    
           try:
             tempdir, tail = os.path.split(filename)
             if not os.path.exists(tempdir):
               os.makedirs(tempdir)
           except:
             self.__log.debug(get_exception_string())
    
           out = open(filename, 'w')
           
           bufSz = 8192
           
           signal.alarm(10)
           
           try:
             self.__log.debug("Starting to grab: %s" % link)
             buf = input.read(bufSz)
      
             while len(buf) > 0:
               # Feed the file into the HTML parser
               parser.feed(buf)
        
         # Re-write the hrefs in the file
               p = re.compile("\?(.+?)=(.+?)")
               buf = p.sub(r"_\1_\2",buf)
               p= re.compile("&(.+?)=(.+?)")
               buf = p.sub(r"_\1_\2",buf)
               p = re.compile("http://(.+?):(\d+)?")
               buf = p.sub(r"\1_\2/",buf)
               buf = re.sub("href=\"/","href=\"",buf)
               p = re.compile("href=\"(.+?)\"")
               buf = p.sub(r"href=\1.html",buf)
 
               out.write(buf)
               buf = input.read(bufSz)
      
             signal.alarm(0)
             input.close()
             if out:
               out.close()
               
             self.__log.debug("Finished grabbing: %s" % link)
           except AlarmException:
             if hodInterrupt.isSet():
               raise HodInterruptException()
             if out: out.close()
             if input: input.close()
             
             self.__log.debug("Failed to retrieve: %s" % link)
         else:
           self.__log.debug("Failed to retrieve: %s" % link)
         
       # Get the next link in level traversal order
       link = parser.getNextLink()

     parser.close()
     
  def check_cluster(self, clusterInfo):
    status = 0

    if 'mapred' in clusterInfo:
      mapredAddress = clusterInfo['mapred'][7:]
      hdfsAddress = clusterInfo['hdfs'][7:]
      status = get_cluster_status(hdfsAddress, mapredAddress)
      if status == 0:
        status = 12
    else:
      status = 15

    return status

  def is_cluster_deallocated(self, jobId):
    """Returns True if the JobId that represents this cluster
       is in the Completed or exiting state."""
    jobInfo = self.__nodePool.getJobInfo(jobId)
    state = None
    if jobInfo is not None and jobInfo.has_key('job_state'):
      state = jobInfo['job_state']
    return ((state == 'C') or (state == 'E'))

  def cleanup(self):
    if self.__nodePool: self.__nodePool.finalize()     

  def get_job_id(self):
    return self.jobId

  def delete_job(self, jobId):
    '''Delete a job given it's ID'''
    ret = 0
    if self.__nodePool: 
      ret = self.__nodePool.deleteJob(jobId)
    else:
      raise Exception("Invalid state: Node pool is not initialized to delete the given job.")
    return ret
         
  def is_valid_account(self):
    """Verify if the account being used to submit the job is a valid account.
       This code looks for a file <install-dir>/bin/verify-account. 
       If the file is present, it executes the file, passing as argument 
       the account name. It returns the exit code and output from the 
       script on non-zero exit code."""

    accountValidationScript = os.path.abspath('./verify-account')
    if not os.path.exists(accountValidationScript):
      return (0, None)

    account = self.__nodePool.getAccountString()
    exitCode = 0
    errMsg = None
    try:
      accountValidationCmd = simpleCommand('Account Validation Command',\
                                             '%s %s' % (accountValidationScript,
                                                        account))
      accountValidationCmd.start()
      accountValidationCmd.wait()
      accountValidationCmd.join()
      exitCode = accountValidationCmd.exit_code()
      self.__log.debug('account validation script is run %d' \
                          % exitCode)
      errMsg = None
      if exitCode is not 0:
        errMsg = accountValidationCmd.output()
    except Exception, e:
      exitCode = 0
      self.__log.warn('Error executing account script: %s ' \
                         'Accounting is disabled.' \
                          % get_exception_error_string())
      self.__log.debug(get_exception_string())
    return (exitCode, errMsg)
    
  def allocate(self, clusterDir, min, max=None):
    status = 0
    failureCount = 0
    self.__svcrgyClient = self.__get_svcrgy_client()
        
    self.__log.debug("allocate %s %s %s" % (clusterDir, min, max))
    
    if min < 3:
      self.__log.critical("Minimum nodes must be greater than 2.")
      status = 2
    else:
      nodeSet = self.__nodePool.newNodeSet(min)
      walltime = None
      if self.__cfg['hod'].has_key('walltime'):
        walltime = self.__cfg['hod']['walltime']
      self.jobId, exitCode = self.__nodePool.submitNodeSet(nodeSet, walltime)
      # if the job submission returned an error other than no resources
      # retry a couple of times
      while (self.jobId is False) and (exitCode != 188):
        if hodInterrupt.isSet():
          raise HodInterruptException()

        failureCount += 1
        if (failureCount >= self.__cfg['hod']['job-status-query-failure-retries']):
          self.__log.debug("failed submitting job more than the retries. exiting")
          break
        else:
          # wait a bit before retrying
          time.sleep(self.__cfg['hod']['job-command-failure-interval'])
          if hodInterrupt.isSet():
            raise HodInterruptException()
          self.jobId, exitCode = self.__nodePool.submitNodeSet(nodeSet, walltime)

      if self.jobId:
        jobStatus = None
        try:
          jobStatus = self.__check_job_status()
        except HodInterruptException, h:
          self.__log.info(HOD_INTERRUPTED_MESG)
          self.delete_job(self.jobId)
          self.__log.info("Cluster %s removed from queue." % self.jobId)
          raise h
        else:
          if jobStatus == -1:
            self.delete_job(self.jobId);
            status = 4
            return status

        if jobStatus:
          self.__log.info("Cluster Id %s" \
                                                              % self.jobId)
          try:
            self.ringmasterXRS = self.__get_ringmaster_client()
            
            self.__log.debug("Ringmaster at : %s" % self.ringmasterXRS )
            ringClient = None
            if self.ringmasterXRS:
              ringClient =  hodXRClient(self.ringmasterXRS)
                
              hdfsStatus, hdfsAddr, self.hdfsInfo = \
                self.__init_hadoop_service('hdfs', ringClient)
                
              if hdfsStatus:
                self.__log.info("HDFS UI at http://%s" % self.hdfsInfo)
  
                mapredStatus, mapredAddr, self.mapredInfo = \
                  self.__init_hadoop_service('mapred', ringClient)
  
                if mapredStatus:
                  self.__log.info("Mapred UI at http://%s" % self.mapredInfo)
  
                  if self.__cfg['hod'].has_key('update-worker-info') \
                    and self.__cfg['hod']['update-worker-info']:
                    workerInfoMap = {}
                    workerInfoMap['HDFS UI'] = 'http://%s' % self.hdfsInfo
                    workerInfoMap['Mapred UI'] = 'http://%s' % self.mapredInfo
                    # Ringmaster URL sample format : http://hostname:port/
                    workerInfoMap['RM RPC Port'] = '%s' % self.ringmasterXRS.split(":")[2].strip("/")
                    if mapredAddr.find(':') != -1:
                      workerInfoMap['Mapred RPC Port'] = mapredAddr.split(':')[1]
                    ret = self.__nodePool.updateWorkerInfo(workerInfoMap, self.jobId)
                    if ret != 0:
                      self.__log.warn('Could not update HDFS and Mapred information.' \
                                      'User Portal may not show relevant information.' \
                                      'Error code=%s' % ret)
  
                  self.__cfg.replace_escape_seqs()
                    
                  # Go generate the client side hadoop-site.xml now
                  # adding final-params as well, just so that conf on 
                  # client-side and server-side are (almost) the same
                  clientParams = None
                  serverParams = {}
                  finalServerParams = {}
  
                  # client-params
                  if self.__cfg['hod'].has_key('client-params'):
                    clientParams = self.__cfg['hod']['client-params']
  
                  # server-params
                  if self.__cfg['gridservice-mapred'].has_key('server-params'):
                    serverParams.update(\
                      self.__cfg['gridservice-mapred']['server-params'])
                  if self.__cfg['gridservice-hdfs'].has_key('server-params'):
                    # note that if there are params in both mapred and hdfs
                    # sections, the ones in hdfs overwirte the ones in mapred
                    serverParams.update(\
                        self.__cfg['gridservice-hdfs']['server-params'])
                    
                  # final-server-params
                  if self.__cfg['gridservice-mapred'].has_key(\
                                                    'final-server-params'):
                    finalServerParams.update(\
                      self.__cfg['gridservice-mapred']['final-server-params'])
                  if self.__cfg['gridservice-hdfs'].has_key(
                                                    'final-server-params'):
                    finalServerParams.update(\
                        self.__cfg['gridservice-hdfs']['final-server-params'])
  
                  clusterFactor = self.__cfg['hod']['cluster-factor']
                  tempDir = self.__cfg['hod']['temp-dir']
                  if not os.path.exists(tempDir):
                    os.makedirs(tempDir)
                  tempDir = os.path.join( tempDir, self.__cfg['hod']['userid']\
                                  + "." + self.jobId )
                  mrSysDir = getMapredSystemDirectory(self.__cfg['hodring']['mapred-system-dir-root'],\
                                      self.__cfg['hod']['userid'], self.jobId)
                  self.__hadoopCfg.gen_site_conf(clusterDir, tempDir, min,\
                            hdfsAddr, mrSysDir, mapredAddr, clientParams,\
                            serverParams, finalServerParams,\
                            clusterFactor)
                  self.__log.info("hadoop-site.xml at %s" % clusterDir)
                  # end of hadoop-site.xml generation
                else:
                  status = 8
              else:
                status = 7  
            else:
              status = 6
            if status != 0:
              self.__log.debug("Cleaning up cluster id %s, as cluster could not be allocated." % self.jobId)
              if ringClient is None:
                self.delete_job(self.jobId)
              else:
                self.__log.debug("Calling rm.stop()")
                ringClient.stopRM()
                self.__log.debug("Returning from rm.stop()")
          except HodInterruptException, h:
            self.__log.info(HOD_INTERRUPTED_MESG)
            if self.ringmasterXRS:
              if ringClient is None:
                ringClient =  hodXRClient(self.ringmasterXRS)
              self.__log.debug("Calling rm.stop()")
              ringClient.stopRM()
              self.__log.debug("Returning from rm.stop()")
              self.__log.info("Cluster Shutdown by informing ringmaster.")
            else:
              self.delete_job(self.jobId)
              self.__log.info("Cluster %s removed from queue directly." % self.jobId)
            raise h
        else:
          self.__log.critical("No cluster found, ringmaster failed to run.")
          status = 5 

      elif self.jobId == False:
        if exitCode == 188:
          self.__log.critical("Request execeeded maximum resource allocation.")
        else:
          self.__log.critical("Job submission failed with exit code %s" % exitCode)
        status = 4
      else:    
        self.__log.critical("Scheduler failure, allocation failed.\n\n")        
        status = 4
    
    if status == 5 or status == 6:
      ringMasterErrors = self.__svcrgyClient.getRMError()
      if ringMasterErrors:
        self.__log.critical("Cluster could not be allocated because" \
                            " of the following errors on the "\
                            "ringmaster host %s.\n%s" % \
                               (ringMasterErrors[0], ringMasterErrors[1]))
        self.__log.debug("Stack trace on ringmaster: %s" % ringMasterErrors[2])
    return status

  def __isRingMasterAlive(self, rmAddr):
    ret = True
    rmSocket = tcpSocket(rmAddr)
    try:
      rmSocket.open()
      rmSocket.close()
    except tcpError:
      ret = False

    return ret

  def deallocate(self, clusterDir, clusterInfo):
    status = 0 
    
    nodeSet = self.__nodePool.newNodeSet(clusterInfo['min'], 
                                         id=clusterInfo['jobid'])
    self.mapredInfo = clusterInfo['mapred']
    self.hdfsInfo = clusterInfo['hdfs']

    try:
      if self.__cfg['hod'].has_key('hadoop-ui-log-dir'):
        clusterStatus = self.check_cluster(clusterInfo)
        if clusterStatus != 14 and clusterStatus != 10:   
          # If JT is still alive
          self.__collect_jobtracker_ui(self.__cfg['hod']['hadoop-ui-log-dir'])
      else:
        self.__log.debug('hadoop-ui-log-dir not specified. Skipping Hadoop UI log collection.')
    except HodInterruptException, h:
      # got an interrupt. just pass and proceed to qdel
      pass 
    except:
      self.__log.info("Exception in collecting Job tracker logs. Ignoring.")
    
    rmAddr = None
    if clusterInfo.has_key('ring'):
      # format is http://host:port/ We need host:port
      rmAddr = clusterInfo['ring'][7:]
      if rmAddr.endswith('/'):
        rmAddr = rmAddr[:-1]

    if (rmAddr is None) or (not self.__isRingMasterAlive(rmAddr)):
      # Cluster is already dead, don't try to contact ringmaster.
      self.__nodePool.finalize()
      status = 10 # As cluster is dead, we just set the status to 'cluster dead'.
    else:
      xrsAddr = clusterInfo['ring']
      rmClient = hodXRClient(xrsAddr)
      self.__log.debug('calling rm.stop')
      rmClient.stopRM()
      self.__log.debug('completed rm.stop')

    # cleanup hod temp dirs
    tempDir = os.path.join( self.__cfg['hod']['temp-dir'], \
                    self.__cfg['hod']['userid'] + "." + clusterInfo['jobid'] )
    if os.path.exists(tempDir):
      shutil.rmtree(tempDir)
   
    return status
  
class hadoopScript:
  def __init__(self, conf, execDir):
    self.__environ = os.environ.copy()
    self.__environ['HADOOP_CONF_DIR'] = conf
    self.__execDir = execDir
    
  def run(self, script):
    scriptThread = simpleCommand(script, script, self.__environ, 4, False, 
                                 False, self.__execDir)
    scriptThread.start()
    scriptThread.wait()
    scriptThread.join()
    
    return scriptThread.exit_code()
