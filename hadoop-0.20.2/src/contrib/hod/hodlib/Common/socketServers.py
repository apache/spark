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
# Various socket server and helper classes.
#
#
import os, sys, socket, threading, pprint, re, xmlrpclib, time
  
from select import select
from SocketServer import ThreadingMixIn, ForkingMixIn
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from random import Random
from urlparse import urlparse

Fault = xmlrpclib.Fault

from hodlib.Common.util import local_fqdn
from hodlib.Common.logger import hodDummyLogger

class hodHTTPHandler(BaseHTTPRequestHandler):
  port = -1

  def __init__(self, request, client_address, server, registerService):
    self.registerService = registerService
    BaseHTTPRequestHandler.__init__(self, request, client_address, server)
  
  def log_message(self, *args):
    """Forget logging for now."""
    
    pass
      
  def do_GET(self):
    self.fullUrl = "http://%s:%s%s" % (self.server.server_address[0],
                                       self.server.server_address[1], 
                                       self.path)
    
    parsedUrl = urlparse(self.fullUrl)
    self.writeHeaders()
    self.writeData(parsedUrl)
  
  def w(self, string):
    self.wfile.write("%s\n" % string)
  
  def writeHeaders(self):
   self.send_response(200, 'OK')
   self.send_header('Content-type', 'text/html')
   self.end_headers()   
     
  def sendWrongPage(self, userJob):
    self.w('<font class="alert">')
    if userJob == False:
      self.w('invalid URL specified')   
    elif re.match("^\d+$", userJob):
      self.w('invalid URL specified, job <b>%s</b> does not exist' % userJob)
    elif re.match("^\w+$", userJob):
      self.w('invalid URL specified, user <b>%s</b> does not exist' % userJob) 
    self.w('</font>')
    
  def getServiceHosts(self, serviceInfo):
    hostInfo = { 'long' : {}, 'short' : {} }
    for user in serviceInfo:
      for job in serviceInfo[user]:
        for host in serviceInfo[user][job]:
          for serviceItem in serviceInfo[user][job][host]:
            serviceName = serviceItem.keys()
            serviceName = serviceName[0]
            if isinstance(serviceItem[serviceName], str):
              hostInfo['short'][self.getJobKey(user, job, host)] = True
            hostInfo['long'][self.getJobKey(user, job, host)] = True
    
    return hostInfo

  def getJobInfo(self, job, serviceInfo):
    jobInfo = {}
    
    for user in serviceInfo.keys():
      for someJob in serviceInfo[user].keys():
        if job == someJob:
          jobInfo[user] = { job : serviceInfo[user][job] }
    
    return jobInfo
  
  def getJobKey(self, user, job, host):
    return "%s-%s-%s" % (user, job, host)
  
  def writeData(self, parsedUrl):
    options = parsedUrl[4]
    serviceInfo = self.server.service.getServiceInfo()
    users = serviceInfo.keys()
    users.sort()

    self.w("<html>")
    self.w("<body>")
    self.w("<head>")
    self.writeCSS()
    self.w("</head>")
    self.w('<font class="header2">HOD Service Registry Information</font>')
    if serviceInfo == {}:
      self.w('<br><br><font class="header">&nbsp;&nbsp;No HOD clusters configured.</font>')
    else:
      if parsedUrl[2] == '/':
        self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
        count = 0
        for user in users:
          self.writeUserData(user, options, serviceInfo, count)
          count = count + 1
      elif parsedUrl[2][1:] in serviceInfo:
        self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
        self.writeUserData(parsedUrl[2][1:], options, serviceInfo, 0)
      elif re.match("^\d+$", parsedUrl[2][1:]):
        jobInfo = self.getJobInfo(parsedUrl[2][1:], serviceInfo)
        if jobInfo.keys():
          self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
          for user in jobInfo.keys():
            self.writeUserData(user, options, jobInfo, 0)   
        else:
          self.sendWrongPage(parsedUrl[2][1:]) 
          self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
          count = 0
          for user in users:
            self.writeUserData(user, options, serviceInfo, count)
            count = count + 1
      elif re.match("^\w+$", parsedUrl[2][1:]):
        self.sendWrongPage(parsedUrl[2][1:]) 
        self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
        count = 0
        for user in users:
          self.writeUserData(user, options, serviceInfo, count)
          count = count + 1        
      else:
        self.sendWrongPage(False) 
        self.w('&nbsp;&nbsp;&nbsp;<table class="main">')
        count = 0
        for user in users:
          self.writeUserData(user, options, serviceInfo, count)
          count = count + 1

    self.w('</table>')
    self.w("</pre>")
    self.w("</body>")
    self.w("</html>")

  def writeCSS(self):
    self.w('<style type="text/css">')
    
    self.w('table.main { border: 0px; padding: 1; background-color: #E1ECE0; width: 70%; margin: 10; }')
    self.w('table.sub1 { background-color: #F1F1F1; padding: 0; }')
    self.w('table.sub2 { background-color: #FFFFFF; padding: 0; }')
    self.w('table.sub3 { border: 1px solid #EEEEEE; background-color: #FFFFFF; padding: 0; }')
    self.w('td.header { border-bottom: 1px solid #CCCCCC; padding: 2;}')
    self.w('td.service1 { border: 0px; background-color: #FFFFFF; padding: 2; width: 10%}')
    self.w('td.service2 { border: 0px; background-color: #FFFFFF; padding: 2; width: 90%}')
    self.w('td { vertical-align: top; padding: 0; }')
    self.w('td.noborder { border-style: none; border-collapse: collapse; }')
    self.w('tr.colored { background-color: #F1F1F1; }')
    self.w('font { font-family: Helvetica, Arial, sans-serif; font-size: 10pt; color: #666666; }')
    self.w('font.header { font-family: Helvetica, Arial, sans-serif;  font-size: 10pt; color: #333333; font-style: bold }')
    self.w('font.header2 { font-family: Helvetica, Arial, sans-serif; font-size: 16pt; color: #333333; }')
    self.w('font.sml { font-family: Helvetica, Arial, sans-serif; font-size: 8pt; color: #666666; }')
    self.w('font.alert { font-family: Helvetica, Arial, sans-serif; font-size: 9pt; color: #FF7A22; }')
    self.w('a { font-family: Helvetica, Arial, sans-serif; text-decoration:none; font-size: 10pt; color: #111111; }')
    self.w('a:visited { font-family: Helvetica, Arial, sans-serif; color:#2D4628; text-decoration:none; font-size: 10pt; }')
    self.w('a:hover { font-family: Helvetica, Arial, sans-serif; color:#00A033; text-decoration:none; font-size: 10pt; }')
    self.w('a.small { font-family:  Helvetica, Arial, sans-serif; text-decoration:none; font-size: 8pt }')
    self.w('a.small:hover { color:#822499; text-decoration:none; font-size: 8pt }')

    self.w("</style>")

  def writeUserData(self, user, options, serviceInfo, count):
    hostInfo = self.getServiceHosts(serviceInfo)
    hostKey = 'short'
    if options == 'display=long':
      hostKey = 'long'

    if count == 0:
      self.w('<tr>')
      self.w('<td class="header" colspan="2">')
      self.w('<font class="header">Active Users</font>')
      self.w('</td>')
      self.w('</tr>')
    self.w('<tr>')
    self.w('<td><font>%s</font></td>' % user)
    self.w('<td>')
    jobIDs = serviceInfo[user].keys()
    jobIDs.sort()
    for jobID in jobIDs: 
      self.w('<table class="sub1" width="100%">')
      if count == 0:
        self.w('<tr>')
        self.w('<td class="header" colspan="2">')
        self.w('<font class="header">PBS Job Identifiers</font>')
        self.w('</td>')
        self.w('</tr>')        
      self.w('<tr>')
      self.w('<td><font>%s</font></td>' % jobID)
      self.w('<td>')
      hosts = serviceInfo[user][jobID].keys()
      hosts.sort()
      for host in hosts:
        if hostInfo[hostKey].has_key(self.getJobKey(user, jobID, host)):
          self.w('<table class="sub2" width="100%">')
          if count == 0:
            self.w('<tr>')
            self.w('<td class="header" colspan="2">')
            self.w('<font class="header">Hosts Running Services</font>')
            self.w('</td>')
            self.w('</tr>')  
          self.w('<tr>')
          self.w('<td><font>%s</font></td>' % host)
          self.w('<td>')
          self.w('<table class="sub3" width="100%">')
          self.w('<tr>')
          self.w('<td colspan="2">')
          self.w('<font class="header">Service Information</font>')
          self.w('</td>')
          self.w('</tr>')  
          for serviceItem in serviceInfo[user][jobID][host]:
            serviceName = serviceItem.keys()
            serviceName = serviceName[0]
            if isinstance(serviceItem[serviceName], dict) and \
              options == 'display=long':
              self.w('<tr class="colored">')
              self.w('<td><font>%s</font></td>' % serviceName)
              self.w('<td>')
              self.w('<table width="100%">')
              for key in serviceItem[serviceName]:
                self.w('<tr>')
                self.w('<td class="service1"><font>%s</font></td>' % key)
                self.w('<td class="service2"><font>%s</font></td>' % serviceItem[serviceName][key])
                self.w('</tr>')
              self.w('</table>')
              self.w('</td>')
              self.w('</tr>')
            elif isinstance(serviceItem[serviceName], str):
              self.w('<tr class="colored">')
              self.w('<td><font class="service1">%s</font></td>' % serviceName)
              self.w('<td>')
              (host, port) = serviceItem[serviceName].split(':')
              hostnameInfo = socket.gethostbyname_ex(host)
              if serviceName.startswith('mapred'):
                self.w('<a href="http://%s:%s">Hadoop Job Tracker</a>' % (hostnameInfo[0], port))
              elif serviceName.startswith('hdfs'):
                self.w('<a href="http://%s:%s">HDFS Name Node</a>&nbsp' % (hostnameInfo[0], port))
              else:
                self.w('<font class="service2">%s</font>' % serviceItem[serviceName])
              self.w('</td>')
              self.w('</tr>')
          self.w('</table>')    
          self.w('</td>')
          self.w('</tr>')
          self.w('</table>')
          count = count + 1
      self.w('</td>')  
      self.w('</tr>')
      self.w('</table>')
      count = count + 1
    self.w('</td>')
    self.w('</tr>')
#    self.w("<pre>")
#    self.w(pprint.pformat(serviceInfo))
#    self.w("</pre>")
    
class baseSocketServer:
    def __init__(self, host, ports):
        self.host = host
        self.ports = ports
        self.__stopForever = threading.Event()
        self.__stopForever.clear()
        self.__run = threading.Event()
        self.__run.set()    
        self.server_address = ()
        self.mThread = None
        
    def server_bind(self):
        """server_bind() method binds to a random range of ports."""

        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if len(self.ports) > 1:
            randomPort = Random(os.getpid())
            portSequence = range(self.ports[0], self.ports[1])

            maxTryCount = abs(self.ports[0] - self.ports[1])
            tryCount = 0
            while True:
                somePort = randomPort.choice(portSequence)
                self.server_address = (self.host, somePort)
                try:
                    self.socket.bind(self.server_address)
                except socket.gaierror, errData:
                    raise socket.gaierror, errData
                except:
                    tryCount = tryCount + 1
                    if tryCount > maxTryCount:
                        bindError = "bind failure for port range %s:%d" % (
                            self.ports)

                        raise socket.error, bindError
                else:
                    break
        else:
            self.server_address = (self.host, int(self.ports[0]))
            self.socket.bind(self.server_address)
        
        if self.host == '':
            self.server_address = (local_fqdn(), self.server_address[1])

    def _serve_forever(self):
        """Replacement for serve_forever loop.
        
           All baseSocketServers run within a master thread; that thread
           imitates serve_forever, but checks an event (self.__stopForever) 
           before processing new connections.
        """
        
        while not self.__stopForever.isSet():
            (rlist, wlist, xlist) = select([self.socket], [], [], 
                                           1)
            
            if (len(rlist) > 0 and self.socket == rlist[0]):
                self.handle_request()
        
            while not self.__run.isSet():
                if self.__stopForever.isSet():
                    break
                time.sleep(1)
        
        self.server_close()
        
        return True

    def serve_forever(self):
        """Handle requests until stopForever event flag indicates stop."""

        self.mThread = threading.Thread(name="baseSocketServer", 
                                        target=self._serve_forever)
        self.mThread.start()

        return self.mThread

    def pause(self):
        """Temporarily stop servicing requests."""

        self.__run.clear()

    def cont(self):
        """Resume servicing requests."""

        self.__run.set()

    def stop(self):
        """Set the stopForever flag to tell serve_forever() to exit."""
    
        self.__stopForever.set()
        if self.mThread: self.mThread.join()
        return True

    def is_alive(self):
        if self.mThread != None:
            return self.mThread.isAlive()
        else:
            return False

class threadedHTTPServer(baseSocketServer, ThreadingMixIn, HTTPServer):
    def __init__(self, host, ports):
        baseSocketServer.__init__(self, host, ports)
        HTTPServer.__init__(self, self.server_address, SimpleHTTPRequestHandler)

class forkingHTTPServer(baseSocketServer, ForkingMixIn, HTTPServer):
    def __init__(self, host, ports):
        baseSocketServer.__init__(self, host, ports)
        HTTPServer.__init__(self, self.server_address, SimpleHTTPRequestHandler)

class hodHTTPServer(baseSocketServer, ThreadingMixIn, HTTPServer):
    service = None 
    def __init__(self, host, ports, serviceobj = None):
        self.service = serviceobj
        baseSocketServer.__init__(self, host, ports)
        HTTPServer.__init__(self, self.server_address, hodHTTPHandler)

    def finish_request(self, request, client_address):
        self.RequestHandlerClass(request, client_address, self, self.service)
        
class hodXMLRPCServer(baseSocketServer, ThreadingMixIn, SimpleXMLRPCServer):
    def __init__(self, host, ports, 
                 requestHandler=SimpleXMLRPCRequestHandler, 
                 logRequests=False, allow_none=False, encoding=None):
        baseSocketServer.__init__(self, host, ports)
        SimpleXMLRPCServer.__init__(self, self.server_address, requestHandler, 
                                    logRequests)
        
        self.register_function(self.stop, 'stop')

try:
    from twisted.web import server, xmlrpc
    from twisted.internet import reactor, defer
    from twisted.internet.threads import deferToThread
    from twisted.python import log
                
    class twistedXMLRPC(xmlrpc.XMLRPC):
        def __init__(self, logger):
            xmlrpc.XMLRPC.__init__(self)
            
            self.__XRMethods = {}
            self.__numRequests = 0
            self.__logger = logger
            self.__pause = False
    
        def render(self, request):
            request.content.seek(0, 0)
            args, functionPath = xmlrpclib.loads(request.content.read())
            try:
                function = self._getFunction(functionPath)
            except Fault, f:
                self._cbRender(f, request)
            else:
                request.setHeader("content-type", "text/xml")
                defer.maybeDeferred(function, *args).addErrback(
                    self._ebRender).addCallback(self._cbRender, request)
            
            return server.NOT_DONE_YET
    
        def _cbRender(self, result, request):
            if isinstance(result, xmlrpc.Handler):
                result = result.result
            if not isinstance(result, Fault):
                result = (result,)
            try:
                s = xmlrpclib.dumps(result, methodresponse=1)
            except:
                f = Fault(self.FAILURE, "can't serialize output")
                s = xmlrpclib.dumps(f, methodresponse=1)
            request.setHeader("content-length", str(len(s)))
            request.write(s)
            request.finish()
     
        def _ebRender(self, failure):
            if isinstance(failure.value, Fault):
                return failure.value
            log.err(failure)
            return Fault(self.FAILURE, "error")
        
        def _getFunction(self, methodName):
            while self.__pause:
                time.sleep(1)
            
            self.__numRequests = self.__numRequests + 1
            function = None
            try:
                def defer_function(*args):
                    return deferToThread(self.__XRMethods[methodName], 
                                         *args)
                function = defer_function
                self.__logger.info(
                    "[%s] processing defered XML-RPC call to: %s ..." % 
                    (self.__numRequests, methodName))            
            except KeyError:
                self.__logger.warn(
                    "[%s] fault %s on XML-RPC call to %s, method not found." % (
                    self.__numRequests, self.NOT_FOUND, methodName))
                raise xmlrpc.NoSuchFunction(self.NOT_FOUND, 
                                            "method %s not found" % methodName)
            
            return function
        
        def register_function(self, functionRef, methodName):
            self.__XRMethods[methodName] = functionRef
            
        def list_methods(self):
            return self.__XRMethods.keys()
        
        def num_requests(self):
            return self.__numRequests
        
        def pause(self):
            self.__pause = True
        
        def cont(self):
            self.__pause = False
            
    class twistedXMLRPCServer:
        def __init__(self, host, ports, logger=None, threadPoolSize=100):
            self.__host = host
            self.__ports = ports
            
            if logger == None:
                logger = hodDummyLogger()
            
            self.__logger = logger
                
            self.server_address = ['', '']
            reactor.suggestThreadPoolSize(threadPoolSize)    
    
            self.__stopForever = threading.Event()
            self.__stopForever.clear()
            self.__mThread = None
                
            self.__xmlrpc = twistedXMLRPC(self.__logger)
                
        def _serve_forever(self):
            if len(self.__ports) > 1:
                randomPort = Random(os.getpid())
                portSequence = range(self.__ports[0], self.__ports[1])
    
                maxTryCount = abs(self.__ports[0] - self.__ports[1])
                tryCount = 0
                while True:
                    somePort = randomPort.choice(portSequence)
                    self.server_address = (self.__host, int(somePort))
                    if self.__host == '':
                        self.server_address = (local_fqdn(), self.server_address[1])
                    try:
                        reactor.listenTCP(int(somePort), server.Site(
                            self.__xmlrpc), interface=self.__host)
                        reactor.run(installSignalHandlers=0)
                    except:
                        self.__logger.debug("Failed to bind to: %s:%s." % (
                            self.__host, somePort))
                        tryCount = tryCount + 1
                        if tryCount > maxTryCount:
                            self.__logger.warn("Failed to bind to: %s:%s" % (
                                self.__host, self.__ports))
                            sys.exit(1)
                    else:
                        break
            else:
                try:
                    self.server_address = (self.__host, int(self.__ports[0]))
                    if self.__host == '':
                        self.server_address = (local_fqdn(), self.server_address[1])
                    reactor.listenTCP(int(self.__ports[0]), server.Site(self.__xmlrpc), 
                                      interface=self.__host)
                    reactor.run(installSignalHandlers=0)
                except:
                    self.__logger.warn("Failed to bind to: %s:%s."% (
                            self.__host, self.__ports[0]))
                    sys.exit(1)
            
        def serve_forever(self):
            """Handle requests until stopForever event flag indicates stop."""
    
            self.__mThread = threading.Thread(name="XRServer",
                                              target=self._serve_forever)
            self.__mThread.start()
            
            if not self.__mThread.isAlive():
                raise Exception("Twisted XMLRPC server thread dead.")
                    
        def register_function(self, functionRef, methodName):
            self.__xmlrpc.register_function(functionRef, methodName)
        
        def register_introspection_functions(self):
            pass
        
        def register_instance(self, instance):
            for method in dir(instance):
                if not method.startswith('_'):
                    self.register_function(getattr(instance, method), method)
        
        def pause(self):
            self.__xmlrpc.pause()
        
        def cont(self):
            self.__xmlrpc.cont()
        
        def stop(self):
            def stop_thread():
                time.sleep(2)
                reactor.stop()
                
            self.__stopForever.set()
            
            stopThread = threading.Thread(name='XRStop', target=stop_thread)
            stopThread.start()
                
            return True
            
        def is_alive(self):
            status = False
            if reactor.running == 1:
                status = True
            
            return status
        
        def status(self):
            """Return status information on running XMLRPC Server."""
            stat = { 'XR server address'     : self.server_address,
                     'XR methods'            : self.system_listMethods(),
                     'XR server alive'       : self.is_alive(),
                     'XR requests processed' : self.__xmlrpc.num_requests(),
                     'XR server stop flag'   : self.__stopForever.isSet()}
            return(stat)
        
        def system_listMethods(self):
            return self.__xmlrpc.list_methods()
        
        def get_server_address(self):
            waitCount = 0
            while self.server_address == '':
                if waitCount == 9:
                    break 
                time.sleep(1)
                waitCount = waitCount + 1
                
            return self.server_address
except ImportError:
    pass
