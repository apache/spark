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

"""
Utility functions.
"""

import ConfigParser
import socket
import urllib2

def bash_quote(text):
  """Quotes a string for bash, by using single quotes."""
  if text == None:
    return ""
  return "'%s'" % text.replace("'", "'\\''")

def bash_quote_env(env):
  """Quotes the value in an environment variable assignment."""
  if env.find("=") == -1:
    return env
  (var, value) = env.split("=")
  return "%s=%s" % (var, bash_quote(value))

def build_env_string(env_strings=[], pairs={}):
  """Build a bash environment variable assignment"""
  env = ''
  if env_strings:
    for env_string in env_strings:
      env += "%s " % bash_quote_env(env_string)
  if pairs:
    for key, val in pairs.items():
      env += "%s=%s " % (key, bash_quote(val))
  return env[:-1]

def merge_config_with_options(section_name, config, options):
  """
  Merge configuration options with a dictionary of options.
  Keys in the options dictionary take precedence.
  """
  res = {}
  try:
    for (key, value) in config.items(section_name):
      if value.find("\n") != -1:
        res[key] = value.split("\n")
      else:
        res[key] = value
  except ConfigParser.NoSectionError:
    pass
  for key in options:
    if options[key] != None:
      res[key] = options[key]
  return res

def url_get(url, timeout=10, retries=0):
  """
  Retrieve content from the given URL.
  """
   # in Python 2.6 we can pass timeout to urllib2.urlopen
  socket.setdefaulttimeout(timeout)
  attempts = 0
  while True:
    try:
      return urllib2.urlopen(url).read()
    except urllib2.URLError:
      attempts = attempts + 1
      if attempts > retries:
        raise

def xstr(string):
  """Sane string conversion: return an empty string if string is None."""
  return '' if string is None else str(string)
