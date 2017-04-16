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
import unittest, getpass, os, sys, re, threading, time

myDirectory = os.path.realpath(sys.argv[0])
rootDirectory   = re.sub("/testing/.*", "", myDirectory)

sys.path.append(rootDirectory)

import tempfile
from testing.lib import BaseTestSuite, MockLogger, MockHadoopCluster
from hodlib.Hod.hod import hodRunner, hodState
from hodlib.Common.desc import NodePoolDesc

excludes = []

# Information about all clusters is written to a file called clusters.state.
from hodlib.Hod.hod import CLUSTER_DATA_FILE as TEST_CLUSTER_DATA_FILE, \
                           INVALID_STATE_FILE_MSGS

# Temp directory prefix
TMP_DIR_PREFIX=os.path.join('/tmp', 'hod-%s' % (getpass.getuser()))

# build a config object with all required keys for initializing hod.
def setupConf():
  cfg = {
          'hod' : {
                    'original-dir' : os.getcwd(),
                    'stream' : True,
                    # store all the info about clusters in this directory
                    'user_state' : '/tmp/hodtest',
                    'debug' : 3,
                    'java-home' : os.getenv('JAVA_HOME'),
                    'cluster' : 'dummy',
                    'cluster-factor' : 1.8,
                    'xrs-port-range' : (32768,65536),
                    'allocate-wait-time' : 3600,
                    'temp-dir' : '/tmp/hod'
                  },
          # just set everything to dummy. Need something to initialize the
          # node pool description object.
          'resource_manager' : {
                                 'id' : 'dummy',
                                 'batch-home' : 'dummy',
                                 'queue' : 'dummy',
                               }
        }
  cfg['nodepooldesc'] = NodePoolDesc(cfg['resource_manager'])
  return cfg

# Test class that defines methods to test invalid arguments to hod operations.
class test_InvalidArgsOperations(unittest.TestCase):
  def setUp(self):

    self.cfg = setupConf()
    # initialize the mock objects
    self.log = MockLogger()
    self.cluster = MockHadoopCluster()

    # Use the test logger. This will be used for test verification.
    self.client = hodRunner(self.cfg, log=self.log, cluster=self.cluster)
    # Create the hodState object to set the test state you want.
    self.state = hodState(self.cfg['hod']['user_state'])
    if not os.path.exists(self.cfg['hod']['user_state']):
      os.path.mkdir(self.cfg['hod']['user_state'])
    p = os.path.join(self.cfg['hod']['user_state'], '%s.state' % TEST_CLUSTER_DATA_FILE)
    # ensure cluster data file exists, so write works in the tests.
    f = open(p, 'w')
    f.close()
  
  def tearDown(self):
    # clean up cluster data file and directory
    p = os.path.join(self.cfg['hod']['user_state'], '%s.state' % TEST_CLUSTER_DATA_FILE)
    os.remove(p)
    os.rmdir(self.cfg['hod']['user_state'])

  # Test that list works with deleted cluster directories - more than one entries which are invalid.
  def testListInvalidDirectory(self):
    userState = { os.path.join(TMP_DIR_PREFIX, 'testListInvalidDirectory1') : '123.dummy.id1', 
                  os.path.join(TMP_DIR_PREFIX, 'testListInvalidDirectory2') : '123.dummy.id2' }
    self.__setupClusterState(userState)
    self.client._op_list(['list'])
    # assert that required errors are logged.
    for clusterDir in userState.keys():
      self.assertTrue(self.log.hasMessage('cluster state unknown\t%s\t%s' \
                            % (userState[clusterDir], clusterDir), 'info'))

    # simulate a test where a directory is deleted, and created again, without deallocation
    clusterDir = os.path.join(TMP_DIR_PREFIX, 'testListEmptyDirectory')
    os.makedirs(clusterDir)
    self.assertTrue(os.path.isdir(clusterDir))
    userState = { clusterDir : '123.dummy.id3' }
    self.__setupClusterState(userState, False)
    self.client._op_list(['list'])
    self.assertTrue(self.log.hasMessage('cluster state unknown\t%s\t%s' \
                          % (userState[clusterDir], clusterDir), 'info'))
    os.rmdir(clusterDir)
    
  # Test that info works with a deleted cluster directory
  def testInfoInvalidDirectory(self):
    clusterDir = os.path.join(TMP_DIR_PREFIX, 'testInfoInvalidDirectory')
    userState = { clusterDir : '456.dummy.id' }
    self.__setupClusterState(userState)
    self.client._op_info(['info', clusterDir])
    self.assertTrue(self.log.hasMessage("Cannot find information for cluster with id '%s' in previously allocated cluster directory '%s'." % (userState[clusterDir], clusterDir), 'critical'))

    # simulate a test where a directory is deleted, and created again, without deallocation
    clusterDir = os.path.join(TMP_DIR_PREFIX, 'testInfoEmptyDirectory')
    os.makedirs(clusterDir)
    self.assertTrue(os.path.isdir(clusterDir))
    userState = { clusterDir : '456.dummy.id1' }
    self.__setupClusterState(userState, False)
    self.client._op_info(['info', clusterDir])
    self.assertTrue(self.log.hasMessage("Cannot find information for cluster with id '%s' in previously allocated cluster directory '%s'." % (userState[clusterDir], clusterDir), 'critical'))
    os.rmdir(clusterDir)

  # Test info works with an invalid cluster directory
  def testInfoNonExistentDirectory(self):
    clusterDir = '/tmp/hod/testInfoNonExistentDirectory'
    self.client._op_info(['info', clusterDir])
    self.assertTrue(self.log.hasMessage("Invalid hod.clusterdir(--hod.clusterdir or -d). %s : No such directory" % (clusterDir), 'critical'))

  # Test that deallocation works on a deleted cluster directory
  # by clearing the job, and removing the state
  def testDeallocateInvalidDirectory(self):
    clusterDir = os.path.join(TMP_DIR_PREFIX,'testDeallocateInvalidDirectory')
    jobid = '789.dummy.id'
    userState = { clusterDir : jobid }
    self.__setupClusterState(userState)
    self.client._op_deallocate(['deallocate', clusterDir])
    # verify job was deleted
    self.assertTrue(self.cluster.wasOperationPerformed('delete_job', jobid))
    # verify appropriate message was logged.
    self.assertTrue(self.log.hasMessage("Cannot find information for cluster with id '%s' in previously allocated cluster directory '%s'." % (userState[clusterDir], clusterDir), 'critical'))
    self.assertTrue(self.log.hasMessage("Freeing resources allocated to the cluster.", 'critical'))
    # verify that the state information was cleared.
    userState = self.state.read(TEST_CLUSTER_DATA_FILE)
    self.assertFalse(clusterDir in userState.keys())
 
    # simulate a test where a directory is deleted, and created again, without deallocation
    clusterDir = os.path.join(TMP_DIR_PREFIX,'testDeallocateEmptyDirectory')
    os.makedirs(clusterDir)
    self.assertTrue(os.path.isdir(clusterDir))
    jobid = '789.dummy.id1'
    userState = { clusterDir : jobid }
    self.__setupClusterState(userState, False)
    self.client._op_deallocate(['deallocate', clusterDir])
    # verify job was deleted
    self.assertTrue(self.cluster.wasOperationPerformed('delete_job', jobid))
    # verify appropriate message was logged.
    self.assertTrue(self.log.hasMessage("Cannot find information for cluster with id '%s' in previously allocated cluster directory '%s'." % (userState[clusterDir], clusterDir), 'critical'))
    self.assertTrue(self.log.hasMessage("Freeing resources allocated to the cluster.", 'critical'))
    # verify that the state information was cleared.
    userState = self.state.read(TEST_CLUSTER_DATA_FILE)
    self.assertFalse(clusterDir in userState.keys())
    os.rmdir(clusterDir)
     
  # Test that deallocation works on a nonexistent directory.
  def testDeallocateNonExistentDirectory(self):
    clusterDir = os.path.join(TMP_DIR_PREFIX,'testDeallocateNonExistentDirectory')
    self.client._op_deallocate(['deallocate', clusterDir])
    # there should be no call..
    self.assertFalse(self.cluster.wasOperationPerformed('delete_job', None))
    self.assertTrue(self.log.hasMessage("Invalid hod.clusterdir(--hod.clusterdir or -d). %s : No such directory" % (clusterDir), 'critical'))

  # Test that allocation on an previously deleted directory fails.    
  def testAllocateOnDeletedDirectory(self):
    clusterDir = os.path.join(TMP_DIR_PREFIX, 'testAllocateOnDeletedDirectory')
    os.makedirs(clusterDir)
    self.assertTrue(os.path.isdir(clusterDir))
    jobid = '1234.abc.com'
    userState = { clusterDir : jobid }
    self.__setupClusterState(userState, False)
    self.client._op_allocate(['allocate', clusterDir, '3'])
    self.assertTrue(self.log.hasMessage("Found a previously allocated cluster at "\
                      "cluster directory '%s'. HOD cannot determine if this cluster "\
                      "can be automatically deallocated. Deallocate the cluster if it "\
                      "is unused." % (clusterDir), 'critical'))
    os.rmdir(clusterDir)

  def __setupClusterState(self, clusterStateMap, verifyDirIsAbsent=True):
    for clusterDir in clusterStateMap.keys():
      # ensure directory doesn't exist, just in case.
      if verifyDirIsAbsent:
        self.assertFalse(os.path.exists(clusterDir))
    # set up required state.
    self.state.write(TEST_CLUSTER_DATA_FILE, clusterStateMap)
    # verify everything is stored correctly.
    state = self.state.read(TEST_CLUSTER_DATA_FILE)
    for clusterDir in clusterStateMap.keys():
      self.assertTrue(clusterDir in state.keys())
      self.assertEquals(clusterStateMap[clusterDir], state[clusterDir])

class test_InvalidHodStateFiles(unittest.TestCase):
  def setUp(self):
    self.rootDir = '/tmp/hod-%s' % getpass.getuser()
    self.cfg = setupConf() # creat a conf
    # Modify hod.user_state
    self.cfg['hod']['user_state'] = tempfile.mkdtemp(dir=self.rootDir,
                              prefix='HodTestSuite.test_InvalidHodStateFiles_')
    self.log = MockLogger() # mock logger
    self.cluster = MockHadoopCluster() # mock hadoop cluster
    self.client = hodRunner(self.cfg, log=self.log, cluster=self.cluster)
    self.state = hodState(self.cfg['hod']['user_state'])
    self.statePath = os.path.join(self.cfg['hod']['user_state'], '%s.state' % \
                                  TEST_CLUSTER_DATA_FILE)
    self.clusterDir = tempfile.mkdtemp(dir=self.rootDir,
                              prefix='HodTestSuite.test_InvalidHodStateFiles_')
  
  def testOperationWithInvalidStateFile(self):
    jobid = '1234.hadoop.apache.org'
    # create user state file with invalid permissions
    stateFile = open(self.statePath, "w")
    os.chmod(self.statePath, 000) # has no read/write permissions
    self.client._hodRunner__cfg['hod']['operation'] = \
                                             "info %s" % self.clusterDir
    ret = self.client.operation()
    os.chmod(self.statePath, 700) # restore permissions
    stateFile.close()
    os.remove(self.statePath)

    # print self.log._MockLogger__logLines
    self.assertTrue(self.log.hasMessage(INVALID_STATE_FILE_MSGS[0] % \
                          os.path.realpath(self.statePath), 'critical'))
    self.assertEquals(ret, 1)
    
  def testAllocateWithInvalidStateFile(self):
    jobid = '1234.hadoop.apache.org'
    # create user state file with invalid permissions
    stateFile = open(self.statePath, "w")
    os.chmod(self.statePath, 0400) # has no write permissions
    self.client._hodRunner__cfg['hod']['operation'] = \
                                        "allocate %s %s" % (self.clusterDir, '3')
    ret = self.client.operation()
    os.chmod(self.statePath, 700) # restore permissions
    stateFile.close()
    os.remove(self.statePath)

    # print self.log._MockLogger__logLines
    self.assertTrue(self.log.hasMessage(INVALID_STATE_FILE_MSGS[2] % \
                        os.path.realpath(self.statePath), 'critical'))
    self.assertEquals(ret, 1)
  
  def testAllocateWithInvalidStateStore(self):
    jobid = '1234.hadoop.apache.org'
    self.client._hodRunner__cfg['hod']['operation'] = \
                                      "allocate %s %s" % (self.clusterDir, 3)

    ###### check with no executable permissions ######
    stateFile = open(self.statePath, "w") # create user state file
    os.chmod(self.cfg['hod']['user_state'], 0600) 
    ret = self.client.operation()
    os.chmod(self.cfg['hod']['user_state'], 0700) # restore permissions
    stateFile.close()
    os.remove(self.statePath)
    # print self.log._MockLogger__logLines
    self.assertTrue(self.log.hasMessage(INVALID_STATE_FILE_MSGS[0] % \
                          os.path.realpath(self.statePath), 'critical'))
    self.assertEquals(ret, 1)
    
    ###### check with no write permissions ######
    stateFile = open(self.statePath, "w") # create user state file
    os.chmod(self.cfg['hod']['user_state'], 0500) 
    ret = self.client.operation()
    os.chmod(self.cfg['hod']['user_state'], 0700) # restore permissions
    stateFile.close()
    os.remove(self.statePath)
    # print self.log._MockLogger__logLines
    self.assertTrue(self.log.hasMessage(INVALID_STATE_FILE_MSGS[0] % \
                          os.path.realpath(self.statePath), 'critical'))
    self.assertEquals(ret, 1)

  def tearDown(self):
    if os.path.exists(self.clusterDir): os.rmdir(self.clusterDir)
    if os.path.exists(self.cfg['hod']['user_state']):
      os.rmdir(self.cfg['hod']['user_state'])


class HodTestSuite(BaseTestSuite):
  def __init__(self):
    # suite setup
    BaseTestSuite.__init__(self, __name__, excludes)
    pass
  
  def cleanUp(self):
    # suite tearDown
    pass

def RunHodTests():
  # modulename_suite
  suite = HodTestSuite()
  testResult = suite.runTests()
  suite.cleanUp()
  return testResult

if __name__ == "__main__":
  RunHodTests()
