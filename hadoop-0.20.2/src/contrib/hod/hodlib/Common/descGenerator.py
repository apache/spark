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
"""manage hod configuration"""
# -*- python -*-

import sys, csv, os
from optparse import Option, OptionParser
from xml.dom import minidom
from sets import Set
from select import select, poll, POLLIN

from hodlib.Common.desc import *

class DescGenerator:
  """Contains the conversion to descriptors and other method calls
  to config"""  
  def __init__(self, hodConfig):
    """parse all the descriptors"""
    
    self.hodConfig = hodConfig
    
  def initializeDesc(self):
    self.hodConfig['nodepooldesc'] = self.createNodePoolDesc()
    self.hodConfig['servicedesc'] = self.createServiceDescDict()
    
    return self.hodConfig
  
  def getServices(self):
    """get all the services from the config"""
    
    sdd = {}
    for keys in self.hodConfig:
      if keys.startswith('gridservice-'):
        str = keys.split('-')
        dict = self.hodConfig[keys]
        if 'server-params' in dict: dict['attrs'] = dict['server-params']
        if 'final-server-params' in dict: dict['final-attrs'] = dict['final-server-params']
        dict['id'] = str[1]
        desc = ServiceDesc(dict)
        sdd[desc.getName()] = desc 
        
    return sdd
  
  def createNodePoolDesc(self):
    """ create a node pool descriptor and store
    it in hodconfig"""
    
    desc = NodePoolDesc(self.hodConfig['resource_manager'])
    return desc
  
  def createServiceDescDict(self):
    """create a service descriptor for 
    all the services and store it in the 
    hodconfig"""
    
    sdd = self.getServices()
    return sdd
  
  
