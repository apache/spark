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
import unittest, os, sys, re, threading, time

myDirectory = os.path.realpath(sys.argv[0])
rootDirectory   = re.sub("/testing/.*", "", myDirectory)

sys.path.append(rootDirectory)

from testing.lib import BaseTestSuite
from hodlib.Common.util import *
from hodlib.Common.threads import simpleCommand

excludes = []

class test_Util(unittest.TestCase):

  def testProcessStatus(self):
    sc = simpleCommand('testsleep', 'sleep 60')
    sc.start()
    pid = sc.getPid()
    while pid is None:
      pid = sc.getPid()
    self.assertTrue(isProcessRunning(pid))
    sc.kill()
    sc.wait()
    sc.join()
    self.assertFalse(isProcessRunning(pid))
    

class UtilTestSuite(BaseTestSuite):
  def __init__(self):
    # suite setup
    BaseTestSuite.__init__(self, __name__, excludes)
    pass
  
  def cleanUp(self):
    # suite tearDown
    pass

def RunUtilTests():
  # modulename_suite
  suite = UtilTestSuite()
  testResult = suite.runTests()
  suite.cleanUp()
  return testResult

if __name__ == "__main__":
  RunUtilTests()
