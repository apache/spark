#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest
import doctest
try:
    import xmlrunner
except ImportError:
    xmlrunner = None


def run_doctests(file_name, globs={}, optionflags=0):
    t = doctest.DocFileSuite(file_name, module_relative=False,
                             globs=globs, optionflags=optionflags)
    if xmlrunner:
        return xmlrunner.XMLTestRunner(output='target/test-reports',
                                       verbosity=3).run(t)
    else:
        return unittest.TextTestRunner(verbosity=3).run(t)
