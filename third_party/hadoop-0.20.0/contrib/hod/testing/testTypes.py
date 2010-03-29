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

excludes = ['']

import tempfile, shutil, getpass, random
from hodlib.Common.types import typeValidator

# All test-case classes should have the naming convention test_.*
class test_typeValidator(unittest.TestCase):
  def setUp(self):
    self.originalDir = os.getcwd()
    self.validator = typeValidator(self.originalDir)
    self.tempDir = tempfile.mkdtemp(dir='/tmp/hod-%s' % getpass.getuser(),
                                    prefix='test_Types_typeValidator_tempDir')
    self.tempFile = tempfile.NamedTemporaryFile(dir=self.tempDir)

    # verification : error strings
    self.errorStringsForVerify = {
                          'pos_int' : 0,
                          'uri' : '%s is an invalid uri',
                          'directory' : 0,
                          'file' : 0,
                        }

    # verification : valid vals
    self.verifyValidVals = [
                            ('pos_int', 0),
                            ('pos_int', 1),
                            ('directory', self.tempDir),
                            ('directory', '/tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempDir)),
                            ('file', self.tempFile.name),
                            ('file', '/tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempFile.name)),
                            ('uri', 'file://localhost/' + self.tempDir),
                            ('uri', 'file:///' + self.tempDir),
                            ('uri', 'file:///tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempDir)),
                            ('uri', 'file://localhost/tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempDir)),
                            ('uri', 'http://hadoop.apache.org/core/'),
                            ('uri', self.tempDir),
                            ('uri', '/tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempDir)),
                           ]

    # generate an invalid uri
    randomNum = random.random()
    while os.path.exists('/%s' % randomNum):
      # Just to be sure :)
      randomNum = random.random()
    invalidUri = 'file://localhost/%s' % randomNum

    # verification : invalid vals
    self.verifyInvalidVals = [
                              ('pos_int', -1),
                              ('uri', invalidUri),
                              ('directory', self.tempFile.name),
                              ('file', self.tempDir),
                             ]

    # normalization : vals
    self.normalizeVals = [
                            ('pos_int', 1, 1),
                            ('pos_int', '1', 1),
                            ('directory', self.tempDir, self.tempDir),
                            ('directory', '/tmp/hod-%s/../../%s' % \
                                  (getpass.getuser(), self.tempDir), 
                                                      self.tempDir),
                            ('file', self.tempFile.name, self.tempFile.name),
                            ('file', '/tmp/hod-%s/../../%s' % \
                                    (getpass.getuser(), self.tempFile.name),
                                                         self.tempFile.name),
                            ('uri', 'file://localhost' + self.tempDir, 
                                  'file://' + self.tempDir),
                            ('uri', 'file://127.0.0.1' + self.tempDir, 
                                  'file://' + self.tempDir),
                            ('uri', 'http://hadoop.apache.org/core',
                                  'http://hadoop.apache.org/core'),
                            ('uri', self.tempDir, self.tempDir),
                            ('uri', '/tmp/hod-%s/../../%s' % \
                                  (getpass.getuser(), self.tempDir), 
                                                      self.tempDir),
                         ]
    pass

  # All testMethods have to have their names start with 'test'
  def testnormalize(self):
    for (type, originalVal, normalizedVal) in self.normalizeVals:
      # print type, originalVal, normalizedVal,\
      #                          self.validator.normalize(type, originalVal)
      assert(self.validator.normalize(type, originalVal) == normalizedVal)
    pass

  def test__normalize(self):
    # Special test for functionality of private method __normalizedPath
    tmpdir = tempfile.mkdtemp(dir=self.originalDir) #create in self.originalDir
    oldWd = os.getcwd()
    os.chdir('/')
    tmpdirName = re.sub(".*/","",tmpdir)
    # print re.sub(".*/","",tmpdirName)
    # print os.path.join(self.originalDir,tmpdir)
    (type, originalVal, normalizedVal) = \
                                    ('file', tmpdirName, \
                                    os.path.join(self.originalDir,tmpdirName))
    assert(self.validator.normalize(type, originalVal) == normalizedVal)
    os.chdir(oldWd)
    os.rmdir(tmpdir)
    pass
    
  def testverify(self):
    # test verify method

    # test valid vals
    for (type,value) in self.verifyValidVals:
      valueInfo = { 'isValid' : 0, 'normalized' : 0, 'errorData' : 0 }
      valueInfo = self.validator.verify(type,value)
      # print type, value, valueInfo
      assert(valueInfo['isValid'] == 1)

    # test invalid vals
    for (type,value) in self.verifyInvalidVals:
      valueInfo = { 'isValid' : 0, 'normalized' : 0, 'errorData' : 0 }
      valueInfo = self.validator.verify(type,value)
      # print type, value, valueInfo
      assert(valueInfo['isValid'] == 0)
      if valueInfo['errorData'] != 0:
        # if there is any errorData, check
        assert(valueInfo['errorData'] == \
                                      self.errorStringsForVerify[type] % value)

    pass

  def tearDown(self):
    self.tempFile.close()
    if os.path.exists(self.tempDir):
      shutil.rmtree(self.tempDir)
    pass

class TypesTestSuite(BaseTestSuite):
  def __init__(self):
    # suite setup
    BaseTestSuite.__init__(self, __name__, excludes)
    pass
  
  def cleanUp(self):
    # suite tearDown
    pass

def RunTypesTests():
  # modulename_suite
  suite = TypesTestSuite()
  testResult = suite.runTests()
  suite.cleanUp()
  return testResult

if __name__ == "__main__":
  RunTypesTests()
