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
"""Gold Allocation Manager Implementation"""
# -*- python -*-

import sys, httplib
import sha, base64, hmac
import xml.dom.minidom

from hodlib.Common.util import *

class goldAllocationManager:
  def __init__(self, cfg, log):
    self.__GOLD_SECRET_KEY_FILE = cfg['auth-file']
    (self.__goldHost, self.__goldPort) = (cfg['allocation-manager-address'][0], 
                                          cfg['allocation-manager-address'][1])
    self.cfg = cfg
    self.log = log

  def getQuote(self, user, project, ignoreErrors=True):
    # Get Secret Key from File
    secret = ''
    try:
      secretFile = open(self.__GOLD_SECRET_KEY_FILE)
      secret = secretFile.readline()
    except Exception, e:
      self.log.error("Unable to open file %s" % self.__GOLD_SECRET_KEY_FILE)
      self.log.debug(get_exception_string())
      return (ignoreErrors or False)
    secretFile.close()
    secret = secret.rstrip()

    # construct the SSRMAP request body 
    body = '<Body><Request action="Quote" actor="hod"><Object>Job</Object><Data><Job><ProjectId>%s</ProjectId><UserId>%s</UserId><WallDuration>10</WallDuration></Job></Data></Request></Body>' % (project, user)

    # compute digest
    message = sha.new()
    message.update(body)
    digest = message.digest()
    digestStr = base64.b64encode(digest)

    # compute signature
    message = hmac.new(secret, digest, sha)
    signatureStr = base64.b64encode(message.digest())

    # construct the SSSRMAP Message
    sssrmapRequest = '<?xml version="1.0" encoding="UTF-8"?>\
<Envelope>%s<Signature><DigestValue>%s</DigestValue><SignatureValue>%s</SignatureValue><SecurityToken type="Symmetric"></SecurityToken></Signature></Envelope>' % (body, digestStr, signatureStr)
    self.log.info('sssrmapRequest: %s' % sssrmapRequest)

    try:
      # post message to GOLD server
      webservice = httplib.HTTP(self.__goldHost, self.__goldPort)
      webservice.putrequest("POST", "/SSSRMAP3 HTTP/1.1")
      webservice.putheader("Content-Type", "text/xml; charset=\"utf-8\"")
      webservice.putheader("Transfer-Encoding", "chunked")
      webservice.endheaders()
      webservice.send("%X" % len(sssrmapRequest) + "\r\n" + sssrmapRequest + '0\r\n')

      # handle the response
      statusCode, statusmessage, header = webservice.getreply()
      responseStr = webservice.getfile().read()
      self.log.debug("httpStatusCode: %d" % statusCode)
      self.log.info('responseStr: %s' % responseStr)

      # parse XML response
      if (statusCode == 200):
        responseArr = responseStr.split("\n")
        responseBody = responseArr[2]
        try:
          doc = xml.dom.minidom.parseString(responseBody)
          responseVal = doc.getElementsByTagName("Value")[0].firstChild.nodeValue
          self.log.info("responseVal: %s" % responseVal)
          if (responseVal == 'Success'):
            return True
          else:
            return False
        except Exception, e:
          self.log.error("Unable to parse GOLD responseBody XML \"(%s)\" to get responseVal" % (responseBody))
          self.log.debug(get_exception_string())
          return (ignoreErrors or False)
      else:
        self.log.error("Invalid HTTP statusCode %d" % statusCode)
    except Exception, e:
      self.log.error("Unable to POST message to GOLD server (%s, %d)" %
                       (self.__goldHost, self.__goldPort))
      self.log.debug(get_exception_string())
      return (ignoreErrors or False)

    return True

