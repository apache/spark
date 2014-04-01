# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ConfigParser
import StringIO
import unittest

from hadoop.cloud.util import bash_quote
from hadoop.cloud.util import bash_quote_env
from hadoop.cloud.util import build_env_string
from hadoop.cloud.util import merge_config_with_options
from hadoop.cloud.util import xstr

class TestUtilFunctions(unittest.TestCase):

  def test_bash_quote(self):
    self.assertEqual("", bash_quote(None))
    self.assertEqual("''", bash_quote(""))
    self.assertEqual("'a'", bash_quote("a"))
    self.assertEqual("'a b'", bash_quote("a b"))
    self.assertEqual("'a\b'", bash_quote("a\b"))
    self.assertEqual("'a '\\'' b'", bash_quote("a ' b"))

  def test_bash_quote_env(self):
    self.assertEqual("", bash_quote_env(""))
    self.assertEqual("a", bash_quote_env("a"))
    self.assertEqual("a='b'", bash_quote_env("a=b"))
    self.assertEqual("a='b c'", bash_quote_env("a=b c"))
    self.assertEqual("a='b\c'", bash_quote_env("a=b\c"))
    self.assertEqual("a='b '\\'' c'", bash_quote_env("a=b ' c"))

  def test_build_env_string(self):
    self.assertEqual("", build_env_string())
    self.assertEqual("a='b' c='d'",
                     build_env_string(env_strings=["a=b", "c=d"]))
    self.assertEqual("a='b' c='d'",
                     build_env_string(pairs={"a": "b", "c": "d"}))

  def test_merge_config_with_options(self):
    options = { "a": "b" }
    config = ConfigParser.ConfigParser()
    self.assertEqual({ "a": "b" },
                     merge_config_with_options("section", config, options))
    config.add_section("section")
    self.assertEqual({ "a": "b" },
                     merge_config_with_options("section", config, options))
    config.set("section", "a", "z")
    config.set("section", "c", "d")
    self.assertEqual({ "a": "z", "c": "d" },
                     merge_config_with_options("section", config, {}))
    self.assertEqual({ "a": "b", "c": "d" },
                     merge_config_with_options("section", config, options))

  def test_merge_config_with_options_list(self):
    config = ConfigParser.ConfigParser()
    config.readfp(StringIO.StringIO("""[section]
env1=a=b
 c=d
env2=e=f
 g=h"""))
    self.assertEqual({ "env1": ["a=b", "c=d"], "env2": ["e=f", "g=h"] },
                     merge_config_with_options("section", config, {}))

  def test_xstr(self):
    self.assertEqual("", xstr(None))
    self.assertEqual("a", xstr("a"))

if __name__ == '__main__':
  unittest.main()
