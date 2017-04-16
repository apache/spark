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
# $Id:setup.py 5158 2007-04-09 00:14:35Z zim $
#
#------------------------------------------------------------------------------
import os, time, shutil, xmlrpclib, socket, pprint

from signal import *

from hodlib.Common.logger import hodLog, hodDummyLogger
from hodlib.Common.socketServers import hodXMLRPCServer
from hodlib.Common.util import local_fqdn
from hodlib.Common.xmlrpc import hodXRClient

class hodBaseService:
  """hodBaseService class - This class provides service registration, logging,
     and configuration access methods.  It also provides an XML-RPC server.
     This class should be extended to create hod services.  Methods beginning
     with _xr_method will automatically be added to instances of this class.
     """
  def __init__(self, name, config, xrtype='threaded'):
    """ Initialization requires a name string and a config object of type
        hodlib.Common.setup.options or hodlib.Common.setup.config."""
        
    self.name = name
    self.hostname = local_fqdn()
    self._cfg = config
    self._xrc = None
    self.logs = {}
    self._baseLogger = None
    self._serviceID = os.getenv('PBS_JOBID')
        
    self.__logDir = None
    self.__svcrgy = None
    self.__stop = False
    self.__xrtype = xrtype
    
    self._init_logging()
        
    if name != 'serviceRegistry': self._init_signals()
    self._init_xrc_server()
    
  def __set_logging_level(self, level):
    self.logs['main'].info("Setting log level to %s." % level)
    for loggerName in self.loggers.keys():
      self.logs['main'].set_logger_level(loggerName, level)

  def __get_logging_level(self):
    if self._cfg.has_key('stream'):
      return self.loggers['main'].get_level('stream', 'main')
    elif self._cfg.has_key('log-dir'):
      return self.loggers['main'].get_level('file', 'main')
    else:
      return 0
  
  def _xr_method_stop(self, *args):
    """XML-RPC method, calls stop() on ourselves."""
    
    return self.stop()
  
  def _xr_method_status(self, *args):
    """XML-RPC method, calls status() on ourselves."""
    
    return self.status()
  
  def _init_logging(self):
    if self._cfg.has_key('debug'):
      if self._cfg['debug'] > 0:
        self._baseLogger = hodLog(self.name)
        self.logs['main'] = self._baseLogger.add_logger('main')
        
        if self._cfg.has_key('stream'):
          if self._cfg['stream']:
            self._baseLogger.add_stream(level=self._cfg['debug'], 
                                 addToLoggerNames=('main',))
            
        if self._cfg.has_key('log-dir'):
          if self._serviceID:
              self.__logDir = os.path.join(self._cfg['log-dir'], "%s.%s" % (
                                       self._cfg['userid'], self._serviceID))
          else:
              self.__logDir = os.path.join(self._cfg['log-dir'], 
                                           self._cfg['userid'])
          if not os.path.exists(self.__logDir):
            os.mkdir(self.__logDir)
            
          self._baseLogger.add_file(logDirectory=self.__logDir, 
            level=self._cfg['debug'], addToLoggerNames=('main',))
          
        if self._cfg.has_key('syslog-address'):
          self._baseLogger.add_syslog(self._cfg['syslog-address'], 
            level=self._cfg['debug'], addToLoggerNames=('main',))
        
        if not self.logs.has_key('main'):
          self.logs['main'] = hodDummyLogger()
      else:
        self.logs['main'] = hodDummyLogger()
    else:
      self.logs['main'] = hodDummyLogger()
  
  def _init_signals(self):
    def sigStop(sigNum, handler):
      self.sig_wrapper(sigNum, self.stop)

    def toggleLevel():
      currentLevel = self.__get_logging_level()
      if currentLevel == 4:
        self.__set_logging_level(1)
      else:
        self.__set_logging_level(currentLevel + 1)

    def sigStop(sigNum, handler):
      self._sig_wrapper(sigNum, self.stop)

    def sigDebug(sigNum, handler):
      self.sig_wrapper(sigNum, toggleLevel)

    signal(SIGTERM, sigStop)
    signal(SIGQUIT, sigStop)
    signal(SIGINT, sigStop)
    signal(SIGUSR2, sigDebug)

  def _sig_wrapper(self, sigNum, handler, *args):
    self.logs['main'].info("Caught signal %s." % sigNum)

    if args:
        handler(args)
    else:
        handler()
  
  def _init_xrc_server(self):
    host = None
    ports = None
    if self._cfg.has_key('xrs-address'):
      (host, port) = (self._cfg['xrs-address'][0], self._cfg['xrs-address'][1])
      ports = (port,)
    elif self._cfg.has_key('xrs-port-range'):
      host = ''
      ports = self._cfg['xrs-port-range']
    
    if host != None:  
      if self.__xrtype == 'threaded':
        self._xrc = hodXMLRPCServer(host, ports)
      elif self.__xrtype == 'twisted':
        try:
          from socketServers import twistedXMLRPCServer
          self._xrc = twistedXMLRPCServer(host, ports, self.logs['main'])
        except ImportError:
          self.logs['main'].error("Twisted XML-RPC server not available, "
                                  + "falling back on threaded server.")
          self._xrc = hodXMLRPCServer(host, ports)
      for attr in dir(self):
        if attr.startswith('_xr_method_'):
          self._xrc.register_function(getattr(self, attr),
                                      attr[11:])
    
      self._xrc.register_introspection_functions()
  
  def _register_service(self, port=None, installSignalHandlers=1):
    if self.__svcrgy:
      self.logs['main'].info(
          "Registering service with service registery %s... " % self.__svcrgy)
      svcrgy = hodXRClient(self.__svcrgy, None, None, 0, 0, installSignalHandlers)
      
      if self._xrc and self._http:
        svcrgy.registerService(self._cfg['userid'], self._serviceID, 
                               self.hostname, self.name, 'hod', {
                               'xrs' : "http://%s:%s" % (
                               self._xrc.server_address[0], 
                               self._xrc.server_address[1]),'http' : 
                               "http://%s:%s" % (self._http.server_address[0], 
                               self._http.server_address[1])})
      elif self._xrc:
        svcrgy.registerService(self._cfg['userid'], self._serviceID, 
                               self.hostname, self.name, 'hod', {
                               'xrs' : "http://%s:%s" % (
                               self._xrc.server_address[0], 
                               self._xrc.server_address[1]),})
      elif self._http:
        svcrgy.registerService(self._cfg['userid'], self._serviceID, 
                               self.hostname, self.name, 'hod', {'http' : 
                               "http://%s:%s" % (self._http.server_address[0], 
                               self._http.server_address[1]),})        
      else:
        svcrgy.registerService(self._cfg['userid'], self._serviceID, 
                               self.hostname, name, 'hod', {} )
  
  def start(self):
    """ Start XML-RPC server and register service."""
    
    self.logs['main'].info("Starting HOD service: %s ..." % self.name)

    if self._xrc: self._xrc.serve_forever()
    if self._cfg.has_key('register') and self._cfg['register']:
        self._register_service()
  
  def stop(self):
    """ Stop XML-RPC server, unregister service and set stop flag. """
    
    self.logs['main'].info("Stopping service...")
    if self._xrc: self._xrc.stop()
    self.__stop = True
  
    return True
  
  def status(self):
    """Returns true, should be overriden."""
    
    return True
  
  def wait(self):
    """Wait until stop method is called."""
    
    while not self.__stop:
      time.sleep(.1)
