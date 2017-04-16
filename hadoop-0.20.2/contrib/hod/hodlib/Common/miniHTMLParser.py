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
import urllib, urlparse, re

from HTMLParser import HTMLParser

class miniHTMLParser( HTMLParser ):

  viewedQueue = []
  instQueue = []

  def setBaseUrl(self, url):
    self.baseUrl = url

  def getNextLink( self ):
    if self.instQueue == []:
      return None
    else:
      return self.instQueue.pop(0)

  def handle_starttag( self, tag, attrs ):
    if tag == 'a':
      newstr = urlparse.urljoin(self.baseUrl, str(attrs[0][1]))
      if re.search('mailto', newstr) != None:
        return

      if (newstr in self.viewedQueue) == False:
        self.instQueue.append( newstr )
        self.viewedQueue.append( newstr )



