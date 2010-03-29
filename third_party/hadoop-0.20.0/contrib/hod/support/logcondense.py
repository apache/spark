#!/bin/sh

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
""":"
work_dir=$(dirname $0)
base_name=$(basename $0)
cd $work_dir

if [ $HOD_PYTHON_HOME ]; then
exec $HOD_PYTHON_HOME -OO -u $base_name ${1+"$@"}
elif [ -e /usr/bin/python ]; then
exec /usr/bin/python -OO -u $base_name ${1+"$@"}
elif [ -e /usr/local/bin/python ]; then
exec /usr/local/bin/python -OO -u $base_name ${1+"$@"}
else
exec python -OO -u $base_name ${1+"$@"}
fi
":"""
					      
from os import popen3
import os, sys
import re
import time
from datetime import datetime
from optparse import OptionParser

myName          = os.path.basename(sys.argv[0])
myName          = re.sub(".*/", "", myName)

reVersion = re.compile(".*(\d+_\d+).*")

VERSION = '$HeadURL: https://svn.apache.org/repos/asf/hadoop/core/branches/branch-0.20/src/contrib/hod/support/logcondense.py $'

reMatch = reVersion.match(VERSION)
if reMatch:
    VERSION = reMatch.group(1)
    VERSION = re.sub("_", ".", VERSION)
else:
    VERSION = 'DEV'

options = ( {'short'   : "-p",
             'long'    : "--package",
             'type'    : "string",
             'action'  : "store",
             'dest'    : "package",
             'metavar' : " ",
             'default' : 'hadoop',
             'help'    : "Bin file for hadoop"},

	    {'short'   : "-d",
	     'long'    : "--days",
	     'type'    : "int",
	     'action'  : "store",
	     'dest'    : "days",
	     'metavar' : " ",
	     'default' : 7,
	     'help'    : "Number of days before logs are deleted"},
	    
	    {'short'   : "-c",
	     'long'    : "--config",
	     'type'    : "string",
	     'action'  : "store",
	     'dest'    : "config",
	     'metavar' : " ",
	     'default' : None,
	     'help'    : "config directory for hadoop"},
	    
	    {'short'   : "-l",
	     'long'    : "--logs",
	     'type'    : "string",
	     'action'  : "store",
	     'dest'    : "log",
	     'metavar' : " ",
	     'default' : "/user",
	     'help'    : "directory prefix under which logs are stored per user"},

	    {'short'   : "-n",
	     'long'    : "--dynamicdfs",
	     'type'    : "string",
	     'action'  : "store",
	     'dest'    : "dynamicdfs",
	     'metavar' : " ",
	     'default' : "false",
	     'help'    : "'true', if the cluster is used to bring up dynamic dfs clusters, 'false' otherwise"}
	    )

def getDfsCommand(options, args):
  if (options.config == None): 
    cmd = options.package + " " + "dfs " + args
  else:
    cmd = options.package + " " + "--config " + options.config + " dfs " + args
  return cmd

def runcondense():
  import shutil
  
  options = process_args()
  # if the cluster is used to bring up dynamic dfs, we must leave NameNode and JobTracker logs, 
  # otherwise only JobTracker logs. Likewise, in case of dynamic dfs, we must also look for
  # deleting datanode logs
  filteredNames = ['jobtracker']
  deletedNamePrefixes = ['*-tasktracker-*']
  if options.dynamicdfs == 'true':
    filteredNames.append('namenode')
    deletedNamePrefixes.append('*-datanode-*')

  filepath = '%s/\*/hod-logs/' % (options.log)
  cmd = getDfsCommand(options, "-lsr " + filepath)
  (stdin, stdout, stderr) = popen3(cmd)
  lastjobid = 'none'
  toPurge = { }
  for line in stdout:
    try:
      m = re.match("^.*\s(.*)\n$", line)
      filename = m.group(1)
      # file name format: <prefix>/<user>/hod-logs/<jobid>/[0-9]*-[jobtracker|tasktracker|datanode|namenode|]-hostname-YYYYMMDDtime-random.tar.gz
      # first strip prefix:
      if filename.startswith(options.log):
        filename = filename.lstrip(options.log)
        if not filename.startswith('/'):
          filename = '/' + filename
      else:
        continue
    
      # Now get other details from filename.
      k = re.match("/(.*)/hod-logs/(.*)/.*-.*-([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9]).*$", filename)
      if k:
        username = k.group(1)
        jobid =  k.group(2)
        datetimefile = datetime(int(k.group(3)), int(k.group(4)), int(k.group(5)))
        datetimenow = datetime.utcnow()
        diff = datetimenow - datetimefile
        filedate = k.group(3) + k.group(4) + k.group(5)
        newdate = datetimenow.strftime("%Y%m%d")
        print "%s %s %s %d" % (filename,  filedate, newdate, diff.days)

        # if the cluster is used to bring up dynamic dfs, we must also leave NameNode logs.
        foundFilteredName = False
        for name in filteredNames:
          if filename.find(name) >= 0:
            foundFilteredName = True
            break

        if foundFilteredName:
          continue

        if (diff.days > options.days):
          desttodel = filename
          if not toPurge.has_key(jobid):
            toPurge[jobid] = options.log.rstrip("/") + "/" + username + "/hod-logs/" + jobid
    except Exception, e:
      print >> sys.stderr, e

  for job in toPurge.keys():
    try:
      for prefix in deletedNamePrefixes:
        cmd = getDfsCommand(options, "-rm " + toPurge[job] + '/' + prefix)
        print cmd
        ret = 0
        ret = os.system(cmd)
        if (ret != 0):
          print >> sys.stderr, "Command failed to delete file " + cmd 
    except Exception, e:
      print >> sys.stderr, e
	  
	
def process_args():
  global options, myName, VERSION
  
  usage = "usage: %s <ARGS>" % (myName)
  
  version = "%s %s" % (myName, VERSION)
  
  argParser = OptionParser(usage=usage, version=VERSION)
  
  for option_element in options:
    argParser.add_option(option_element['short'], option_element['long'],
			 type=option_element['type'], action=option_element['action'],
			 dest=option_element['dest'], default=option_element['default'],
			 metavar=option_element['metavar'], help=option_element['help'])

  (parsedOptions, args) = argParser.parse_args()
  
  if not os.path.exists(parsedOptions.package):
    argParser.error("Could not find path to hadoop binary: %s" % parsedOptions.package)
  if not os.path.exists(parsedOptions.config):
    argParser.error("Could not find config: %s" % parsedOptions.config)
  if parsedOptions.days <= 0:
    argParser.error("Invalid number of days specified, must be > 0: %s" % parsedOptions.config)
  if parsedOptions.dynamicdfs!='true' and parsedOptions.dynamicdfs!='false':
    argParser.error("Invalid option for dynamicdfs, must be true or false: %s" % parsedOptions.dynamicdfs)

  return parsedOptions
  
  
if __name__ == '__main__':
  runcondense()
  
