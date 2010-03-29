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
"""HOD Service Proxy Implementation"""
# -*- python -*-

import sys, time, signal, httplib, socket, threading
import sha, base64, hmac
import xml.dom.minidom

from hodlib.Common.socketServers import hodHTTPServer
from hodlib.Common.hodsvc import hodBaseService
from hodlib.Common.threads import loop
from hodlib.Common.tcp import tcpSocket
from hodlib.Common.util import get_exception_string
from hodlib.Common.AllocationManagerUtil import *

class svcpxy(hodBaseService):
    def __init__(self, config):
        hodBaseService.__init__(self, 'serviceProxy', config['service_proxy'],
                                xrtype='twisted')
        self.amcfg=config['allocation_manager']

    def _xr_method_isProjectUserValid(self, userid, project, ignoreErrors = False, timeOut = 15):
       return self.isProjectUserValid(userid, project, ignoreErrors, timeOut)
    
    def isProjectUserValid(self, userid, project, ignoreErrors, timeOut):
        """Method thats called upon by
        the hodshell to verify if the 
        specified (user, project) combination 
        is valid"""
        self.logs['main'].info("Begin isProjectUserValid()")
        am = AllocationManagerUtil.getAllocationManager(self.amcfg['id'], 
                                                        self.amcfg,
                                                        self.logs['main'])
        self.logs['main'].info("End isProjectUserValid()")
        return am.getQuote(userid, project)
