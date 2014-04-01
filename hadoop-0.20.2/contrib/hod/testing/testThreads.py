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

# module specific imports
import os, tempfile, random

excludes = []

import getpass
from hodlib.Common.threads import simpleCommand
from testing.helper import sampleText

# All test-case classes should have the naming convention test_.*
class test_SimpleCommand(unittest.TestCase):
  def setUp(self):
    self.rootDir = '/tmp/hod-%s' % getpass.getuser()
    if not os.path.exists(self.rootDir):
      os.mkdir(self.rootDir)
    self.prefix= 'ThreadsTestSuite.test_SimpleCommand'
    self.testFile = None
    pass

  def testRedirectedStdout(self):
    self.testFile= tempfile.NamedTemporaryFile(dir=self.rootDir, \
                                               prefix=self.prefix)
    cmd=simpleCommand('helper','%s %s 1 1>%s' % \
                      (sys.executable, \
                      os.path.join(rootDirectory, "testing", "helper.py"), \
                      self.testFile.name))

    cmd.start()
    cmd.join()
    
    self.testFile.seek(0)
    stdout = self.testFile.read()
    # print stdout, sampleText
    assert(stdout == sampleText)
    pass

  def testRedirectedStderr(self):
    self.testFile= tempfile.NamedTemporaryFile(dir=self.rootDir, \
                                                prefix=self.prefix)
    cmd=simpleCommand('helper','%s %s 2 2>%s' % \
                      (sys.executable, \
                      os.path.join(rootDirectory, "testing", "helper.py"), \
                      self.testFile.name))
    cmd.start()
    cmd.join()
     
    self.testFile.seek(0)
    stderror = self.testFile.read()
    # print stderror, sampleText
    assert(stderror == sampleText)
    pass

  def tearDown(self):
    if self.testFile: self.testFile.close()
    pass

class ThreadsTestSuite(BaseTestSuite):
  def __init__(self):
    # suite setup
    BaseTestSuite.__init__(self, __name__, excludes)
    pass
  
  def cleanUp(self):
    # suite tearDown
    pass

def RunThreadsTests():
  # modulename_suite
  suite = ThreadsTestSuite()
  testResult = suite.runTests()
  suite.cleanUp()
  return testResult

if __name__ == "__main__":
  RunThreadsTests()
