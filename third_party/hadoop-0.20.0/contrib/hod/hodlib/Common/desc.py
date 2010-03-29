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
"""manage component descriptors"""
# -*- python -*-

import random

from sets import Set
from pprint import pformat
from hodlib.Common.util import local_fqdn
from hodlib.Common.tcp import tcpSocket, tcpError

class Schema:
  """the primary class for describing
  schema's """
  STRING, LIST, MAP = range(3)

  def __init__(self, name, type = STRING, delim=','):
    self.name = name
    self.type = type
    self.delim = delim

  def getName(self):
    return self.name

  def getType(self):
    return self.type

  def getDelim(self):
    return self.delim

class _Merger:
  """A class to merge lists and add key/value
  pairs to a dictionary"""
  def mergeList(x, y, uniq=True):
    l = []
    l.extend(x)
    l.extend(y)
    if not uniq:
      return l

    s = Set(l)
    l = list(s)
    return l

  mergeList = staticmethod(mergeList)

  def mergeMap(to, add):

    for k in add:
      to.setdefault(k, add[k])

    return to

  mergeMap = staticmethod(mergeMap)

class NodePoolDesc:
  """a schema for describing
  Nodepools"""
  def __init__(self, dict):
    self.dict = dict.copy()

    self.dict.setdefault('attrs', {})

    self._checkRequired()

    if 'options' in dict: self.dict['attrs'] = dict['options']

  def _checkRequired(self):

    if not 'id' in self.dict:
      raise ValueError, "nodepool needs 'id'"

    if self.getPkgDir() == None:
      raise ValueError, "nodepool %s needs 'pkgs'" % (self.getName())

  def getName(self):
    return self.dict['id']

  def getPkgDir(self):
    return self.dict['batch-home']

  def getAttrs(self):
    return self.dict['attrs']

  def getSchema():
    schema = {}

    s = Schema('id')
    schema[s.getName()] = s

    s = Schema('batch-home', Schema.LIST, ':')
    schema[s.getName()] = s

    s = Schema('attrs', Schema.MAP)
    schema[s.getName()] = s

    return schema

  getSchema = staticmethod(getSchema)

class ServiceDesc:
  """A schema for describing services"""
  def __init__(self, dict):
    self.dict = dict.copy()

    self.dict.setdefault('external', False)
    self.dict.setdefault('attrs', {})
    self.dict.setdefault('envs', {})
    self.dict.setdefault('host',None)
    self.dict.setdefault('port',None)
    self.dict.setdefault('tar', None)
    self.dict.setdefault('pkgs', '')
    self.dict.setdefault('final-attrs', {})
    self._checkRequired()
    if self.dict.has_key('hadoop-tar-ball'):
      self.dict['tar'] = self.dict['hadoop-tar-ball']  

  def _checkRequired(self):

    if not 'id' in self.dict:
      raise ValueError, "service description needs 'id'"

#    if len(self.getPkgDirs()) <= 0:
#      raise ValueError, "service description %s needs 'pkgs'" % (self.getName())

  def getName(self):
    return self.dict['id']

  def isExternal(self):
    """True if the service is outside hod. 
    e.g. connect to existing HDFS"""
    
    return self.dict['external']

  def getPkgDirs(self):
    return self.dict['pkgs']
  
  def getTar(self):
    return self.dict['tar']
  
  def getAttrs(self):
    return self.dict['attrs']

  def getfinalAttrs(self):
    return self.dict['final-attrs']
  
  def getEnvs(self):
    return self.dict['envs']

  def getSchema():
    schema = {}

    s = Schema('id')
    schema[s.getName()] = s

    s = Schema('external')
    schema[s.getName()] = s

    s = Schema('pkgs', Schema.LIST, ':')
    schema[s.getName()] = s
    
    s = Schema('tar', Schema.LIST, ":")
    schema[s.getName()] = s
    
    s = Schema('attrs', Schema.MAP)
    schema[s.getName()] = s

    s = Schema('final-attrs', Schema.MAP)
    schema[s.getName()] = s
    
    s = Schema('envs', Schema.MAP)
    schema[s.getName()] = s

    return schema
  
  getSchema = staticmethod(getSchema)

class CommandDesc:

  def __init__(self, dict):
    """a class for how a command is described"""
    self.dict = dict

  def __repr__(self):
    return pformat(self.dict)
  
  def _getName(self):
    """return the name of the command to be run"""
    return self.dict['name']

  def _getProgram(self):
    """return where the program is """
    return self.dict['program']

  def _getArgv(self):
    """return the arguments for the command to be run"""
    return self.dict['argv']

  def _getEnvs(self):
    """return the environment in which the command is to be run"""
    return self.dict['envs']
  
  def _getPkgDirs(self):
    """return the packages for this command"""
    return self.dict['pkgdirs']

  def _getWorkDirs(self):
    """return the working directories for this command"""
    return self.dict['workdirs']

  def _getAttrs(self):
    """return the list of attributes for this command"""
    return self.dict['attrs']

  def _getfinalAttrs(self):
    """return the final xml params list for this command"""
    return self.dict['final-attrs']
  
  def _getForeground(self):
    """return if the command is to be run in foreground or not"""
    return self.dict['fg']

  def _getStdin(self):
    return self.dict['stdin']

  def toString(cmdDesc):
    """return a string representation of this command"""
    row = []
    row.append('name=%s' % (cmdDesc._getName()))
    row.append('program=%s' % (cmdDesc._getProgram()))
    row.append('pkgdirs=%s' % CommandDesc._csv(cmdDesc._getPkgDirs(), ':'))

    if 'argv' in cmdDesc.dict:
      row.append('argv=%s' % CommandDesc._csv(cmdDesc._getArgv()))

    if 'envs' in cmdDesc.dict:
      envs = cmdDesc._getEnvs()
      list = []
      for k in envs:
        v = envs[k]
        list.append('%s=%s' % (k, v))
      row.append('envs=%s' % CommandDesc._csv(list))

    if 'workdirs' in cmdDesc.dict:
      row.append('workdirs=%s' % CommandDesc._csv(cmdDesc._getWorkDirs(), ':'))

    if 'attrs' in cmdDesc.dict:
      attrs = cmdDesc._getAttrs()
      list = []
      for k in attrs:
        v = attrs[k]
        list.append('%s=%s' % (k, v))
      row.append('attrs=%s' % CommandDesc._csv(list))

    if 'final-attrs' in cmdDesc.dict:
      fattrs = cmdDesc._getAttrs()
      list = []
      for k in fattrs:
	v = fattrs[k]
	list.append('%s=%s' % (k, v)) 
      row.append('final-attrs=%s' % CommandDesc._cvs(list))
      
    if 'fg' in cmdDesc.dict:
      row.append('fg=%s' % (cmdDesc._getForeground()))

    if 'stdin' in cmdDesc.dict:
      row.append('stdin=%s' % (cmdDesc._getStdin()))

    return CommandDesc._csv(row)

  toString = staticmethod(toString)

  def _csv(row, delim=','):
    """return a string in csv format"""
    import cStringIO
    import csv

    queue = cStringIO.StringIO()
    writer = csv.writer(queue, delimiter=delim, escapechar='\\', quoting=csv.QUOTE_NONE, 
                          doublequote=False, lineterminator='\n')
    writer.writerow(row)
    return queue.getvalue().rstrip('\n')

  _csv = staticmethod(_csv)
