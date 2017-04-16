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
import xmlrpclib, time, random, signal
from hodlib.Common.util import hodInterrupt, HodInterruptException

class hodXRClient(xmlrpclib.ServerProxy):
    def __init__(self, uri, transport=None, encoding=None, verbose=0,
                 allow_none=0, installSignalHandlers=1, retryRequests=True, timeOut=15):
        xmlrpclib.ServerProxy.__init__(self, uri, transport, encoding, verbose, 
                                       allow_none)
        self.__retryRequests = retryRequests
        self.__timeOut = timeOut
        if (installSignalHandlers!=0):
          self.__set_alarm()
    
    def __set_alarm(self):
        def alarm_handler(sigNum, sigHandler):
            raise Exception("XML-RPC socket timeout.")
          
        signal.signal(signal.SIGALRM, alarm_handler)
      
    def __request(self, methodname, params):
        response = None
        retryWaitTime = 5 + random.randint(0, 5)
        for i in range(0, 30):
            signal.alarm(self.__timeOut)
            try:
                response = self._ServerProxy__request(methodname, params)
                signal.alarm(0)
                break
            except Exception:
                if self.__retryRequests:
                  if hodInterrupt.isSet():
                    raise HodInterruptException()
                  time.sleep(retryWaitTime)
                else:
                  raise Exception("hodXRClientTimeout")

        return response
                
    def __getattr__(self, name):
        # magic method dispatcher
        return xmlrpclib._Method(self.__request, name)
                           
