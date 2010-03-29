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
import unittest, os, sys, re

myPath = os.path.realpath(sys.argv[0])
rootDirectory   = re.sub("/testing/.*", "", myPath)
testingDir = os.path.join(rootDirectory, "testing")

sys.path.append(rootDirectory)

from testing.lib import printSeparator, printLine

moduleList = []
allList = []
excludes = [
           ]

# Build a module list by scanning through all files in testingDir
for file in os.listdir(testingDir):
  if(re.search(r".py$", file) and re.search(r"^test", file)):
    # All .py files with names starting in 'test'
    module = re.sub(r"^test","",file)
    module = re.sub(r".py$","",module)
    allList.append(module)
    if module not in excludes:
      moduleList.append(module)

printLine("All testcases - %s" % allList)
printLine("Excluding the testcases - %s" % excludes)
printLine("Executing the testcases - %s" % moduleList)

testsResult = 0
# Now import each of these modules and start calling the corresponding
#testSuite methods
for moduleBaseName in moduleList:
  try:
    module = "testing.test" + moduleBaseName
    suiteCaller = "Run" + moduleBaseName + "Tests"
    printSeparator()
    printLine("Running %s" % suiteCaller)

    # Import the corresponding test cases module
    imported_module = __import__(module , fromlist=[suiteCaller] )
    
    # Call the corresponding suite method now
    testRes = getattr(imported_module, suiteCaller)()
    testsResult = testsResult + testRes
    printLine("Finished %s. TestSuite Result : %s\n" % \
                                              (suiteCaller, testRes))
  except ImportError, i:
    # Failed to import a test module
    printLine(i)
    testsResult = testsResult + 1
    pass
  except AttributeError, n:
    # Failed to get suiteCaller from a test module
    printLine(n)
    testsResult = testsResult + 1
    pass
  except Exception, e:
    # Test module suiteCaller threw some exception
    printLine("%s failed. \nReason : %s" % (suiteCaller, e))
    printLine("Skipping %s" % suiteCaller)
    testsResult = testsResult + 1
    pass

if testsResult != 0:
  printSeparator()
  printLine("Total testcases with failure or error : %s" % testsResult)
sys.exit(testsResult)
