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
import unittest, re, sys

class BaseTestSuite():
  def __init__(self, name, excludes):
    self.name = name
    self.excludes = excludes
    pass
  
  def runTests(self):
    # Create a runner
    self.runner = unittest.TextTestRunner()
    
    # Get all the test-case classes
    # From module import *
    mod = __import__(self.name, fromlist=['*'])
    modItemsList = dir(mod)

    allsuites = []

    # Create all the test suites
    for modItem in modItemsList:
      if re.search(r"^test_", modItem):
        # Yes this is a test class
        if modItem not in self.excludes:
          test_class = getattr(mod, modItem)
          allsuites.append(unittest.makeSuite(test_class))

    # Create a master suite to be run.
    alltests = unittest.TestSuite(tuple(allsuites))

    # Run the master test suite.
    runner = self.runner.run(alltests)
    if(runner.wasSuccessful()): return 0
    printLine( "%s test(s) failed." % runner.failures.__len__())
    printLine( "%s test(s) threw errors." % runner.errors.__len__())
    return runner.failures.__len__() + runner.errors.__len__()

  def cleanUp(self):
    # suite tearDown
    pass

def printLine(str):
  print >>sys.stderr, str

def printSeparator():
  str = ""
  for i in range(0,79):
    str = str + "*"
  print >>sys.stderr, "\n", str, "\n"

# This class captures all log messages logged by hodRunner and other classes.
# It is then used to verify that certain log messages have come. This is one
# way to validate that messages printed to the logger are correctly written.
class MockLogger:
  def __init__(self):
    self.__logLines = {}

  def info(self, message):
    self.__logLines[message] = 'info'

  def critical(self, message):
    self.__logLines[message] = 'critical'

  def warn(self, message):
    self.__logLines[message] = 'warn'

  def debug(self, message):
    # don't track debug lines.
    pass

  # verify a certain message has been logged at the defined level of severity.
  def hasMessage(self, message, level):
    if not self.__logLines.has_key(message):
      return False
    return self.__logLines[message] == level

# Stub class to test cluster manipulation operations.
class MockHadoopCluster:
  
  def __init__(self):
    # store the operations received.
    self.__operations = {}
  
  def delete_job(self, jobid):
    self.__operations['delete_job'] = [jobid]
 
  def is_cluster_deallocated(self, dummy):
    return False
 
  def wasOperationPerformed(self, operation, args):
    if self.__operations.has_key(operation):
      actualArgs = self.__operations[operation]
      for arg in actualArgs:
        if arg not in args:
          break
      else:
        return True
    return False
