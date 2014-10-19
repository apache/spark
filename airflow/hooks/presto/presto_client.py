#!/usr/bin/env python

""" PrestoClient provides a method to communicate with a Presto server. Presto is a fast query
    engine developed by Facebook that runs distributed queries against Hadoop HDFS servers.

    Copyright 2013-2014 Ivo Herweijer | easydatawarehousing.com

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at:

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
"""

import httplib
import socket
import urllib2
import json
import getpass
from time import sleep


class PrestoClient:
    """
    PrestoClient
    ============

    PrestoClient implements a Python class to communicate with a Presto server.
    Presto (U{http://prestodb.io/}) is a fast query engine developed
    by Facebook that runs distributed queries against a (cluster of)
    Hadoop HDFS servers (U{http://hadoop.apache.org/}).
    Presto uses SQL as its query language. Presto is an alternative for
    Hadoop-Hive.

    PrestoClient was developed and tested on Presto versions 0.52 to 0.68. Python version used is 2.7.6

    You can use this class with this sample code:

    >>> import prestoclient
    >>>
    >>> sql = "SHOW TABLES"
    >>>
    >>> # Replace localhost with ip address or dns name of the Presto server running the discovery service
    >>> presto = prestoclient.PrestoClient("localhost")
    >>>
    >>> if not presto.runquery(sql):
    >>>     print "Error: ", presto.getlasterrormessage()
    >>> else:
    >>>     # We're done now, so let's show the results
    >>>     print "Columns: ", presto.getcolumns()
    >>>     if presto.getdata(): print "Datalength: ", presto.getnumberofdatarows(), " Data: ", presto.getdata()

    Presto client protocol
    ======================

    The communication protocol used between Presto clients and servers is not documented yet. It seems to
    be as follows:

    Client sends http POST request to the Presto server, page: "/v1/statement". Header information should
    include: X-Presto-Catalog, X-Presto-Source, X-Presto-Schema, User-Agent, X-Presto-User. The body of the
    request should contain the sql statement. The server responds by returning JSON data (http status-code 200).
    This reply may contain up to 3 uri's. One giving the link to get more information about the query execution
    ('infoUri'), another giving the link to fetch the next packet of data ('nextUri') and one with the uri to
    cancel the query ('partialCancelUri').

    The client should send GET requests to the server (Header: X-Presto-Source, User-Agent, X-Presto-User.
    Body: empty) following the 'nextUri' link from the previous response until the servers response does not
    contain an 'nextUri' link anymore. When there is no 'nextUri' the query is finished. If the last response
    from the server included an error section ('error') the query failed, otherwise the query succeeded. If
    the http status of the server response is anything other than 200 with Content-Type application/json, the
    query should also be considered failed. A 503 http response means that the server is (too) busy. Retry the
    request after waiting at least 50ms.
    The server response may contain a 'state' variable. This is for informational purposes only (may be subject
    to change in future implementations).
    Each response by the server to a 'nextUri' may contain information about the columns returned by the query
    and all- or part of the querydata. If the response contains a data section the columns section will always
    be available.

    The server reponse may contain a variable with the uri to cancel the query ('partialCancelUri'). The client
    may issue a DELETE request to the server using this link. Response http status-code is 204.

    The Presto server will retain information about finished queries for 15 minutes. When a client does not
    respond to the server (by following the 'nextUri' links) the server will cancel these 'dead' queries after
    5 minutes. These timeouts are hardcoded in the Presto server source code.

    Todo
    ====

        - Enable PrestoClient to handle multiple running queries simultaneously. Currently you can only run
        one query per instance of this class.

        - Add support for https connections

        - Add support for insert/update queries (if and when Presto server supports this).

    Availability
    ============

    PrestoClient source code is available through: U{https://github.com/easydatawarehousing/prestoclient}

    Additional information may be found here: U{http://www.easydatawarehousing.com/tag/presto/}

    """

    __source = "PyPrestoClient"                 #: Client name sent to Presto server
    __version = "0.3.1"                         #: PrestoClient version string
    __useragent = __source + "/" + __version    #: Useragent name sent to Presto server
    __urltimeout = 5000                         #: Timeout in millisec to wait for Presto server to respond
    __updatewaittimemsec = 1500                 #: Wait time in millisec to wait between requests to Presto server
    __retrievewaittimemsec = 50                 #: Wait time in millisec to wait between requests when recieving data
    __retrywaittimemsec = 100                   #: Wait time in millisec to wait before retrying a request
    __maximumretries = 5                        #: Maximum number of retries fro request in case of 503 errors
    __server = ""                               #: IP address or DNS name of Presto server
    __port = 0                                  #: TCP port of Presto server
    __catalog = ""                              #: Catalog name to be used by Presto server
    __user = ""                                 #: Username to pass to Presto server
    __timezone = ""                             #: Timezone to pass to Presto server
    __language = ""                             #: Language to pass to Presto server
    __lasterror = ""                            #: Html error of last request
    __lastinfouri = ""                          #: Uri to query information on the Presto server
    __lastnexturi = ""                          #: Uri to next dataframe on the Presto server
    __lastcanceluri = ""                        #: Uri to cancel query on the Presto server
    __laststate = ""                            #: State returned by last request to Presto server
    __clientstatus = "NONE"                     #: Status defined by PrestoClient: NONE, RUNNING, SUCCEEDED, FAILED
    __cancelquery = False                       #: Boolean, when set to True signals that query should be cancelled
    __lastresponse = {}                         #: Buffer for last response of Presto server
    __columns = {}                              #: Buffer for the column information returned by the query
    __data = []                                 #: Buffer for the data returned by the query

    def __init__(self, in_server, in_port=8080, in_catalog="hive", in_user="", in_timezone="", in_language=""):
        """ Constructor of PrestoClient class.

        Arguments:

        in_server   -- IP Address or dns name of the Presto server running the discovery service

        in_port     -- TCP port of the Prestoserver running the discovery service (default 8080)

        in_catalog  -- Catalog name that the Prestoserver should use to query hdfs (default 'hive')

        in_user     -- Username to pass to the Prestoserver. If left blank the username from the OS is used
        (default '')

        in_timezone -- Timezone to pass to the Prestoserver. Leave blank (=default) for servers default timezone.
        (IANA timezone format)

        in_language -- Language to pass to the Prestoserver. Leave blank (=default) for the servers default language.
        (ISO-639-1 code)

        """
        self.__server = in_server
        self.__port = in_port
        self.__catalog = in_catalog
        self.__timezone = in_timezone
        self.__language = in_language

        if in_user == "":
            self.__user = getpass.getuser()
        else:
            self.__user = in_user

        return

    def runquery(self, in_sql_statement, in_schema="default"):
        """ Execute a query. Currently, only one simultaneous query per instance of the PrestoClient class is allowed.
        Starting a new query will discard any data previously retrieved ! Returns True if query succeeded.

        Arguments:

        in_sql_statement -- The query that should be executed by the Presto server

        in_schema        -- The HDFS schema that should be used (default 'default')

        """

        if not self.startquery(in_sql_statement, in_schema):
            return False
        else:
            self.waituntilfinished()

        return self.__clientstatus == "SUCCEEDED"

    def getversion(self):
        """ Return PrestoClient version number. """
        return self.__version

    def getlastresponse(self):
        """ Return response of last executed request to the prestoserver. """
        return self.__lastresponse

    def getstatus(self):
        """ Return status of the client. Note this is not the same as the state reported by the Presto server! """
        return self.__clientstatus

    def getlastserverstate(self):
        """ Return state of the request as reported by the Presto server. """
        return self.__laststate

    def getlasterrormessage(self):
        """ Return error message of last executed request to the prestoserver or empty string if there is no error. """
        return self.__lasterror

    def getcolumns(self):
        """ Return the column information of the queryresults. Nested list of datatype / fieldname. """
        return self.__columns

    def getnumberofdatarows(self):
        """ Return the length of the currently buffered data in number of rows. """
        return len(self.__data)

    def getdata(self):
        """ Return the currently buffered data. """
        return self.__data

    def cleardata(self):
        """ Empty the data buffer. You can use this function to implement your own 'streaming' data retrieval setup. """
        self.__data = {}
        return

    def startquery(self, in_sql_statement, in_schema="default"):
        """ Start a query. Currently, only one simultaneous query per instance of the PrestoClient class is allowed.
        Starting a new query will discard any data previously retrieved !

        Arguments:

        in_sql_statement -- The query that should be executed by the Presto server

        in_schema        -- The HDFS schema that should be used (default 'default')

        """

        headers = {"X-Presto-Catalog": self.__catalog,
                   "X-Presto-Source":  self.__source,
                   "X-Presto-Schema":  in_schema,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        if self.__timezone:
            headers["X-Presto-Time-Zone"] = self.__timezone

        if self.__language:
            headers["X-Presto-Language"] = self.__language

        # Prepare statement
        sql = in_sql_statement
        sql = sql.strip()
        sql = sql.rstrip(";")

        if sql == "":
            self.__lasterror = "No query entered"
            return False

        # Check current state, any status except running is okay
        if self.__clientstatus == "RUNNING":
            self.__lasterror = "Query already running. Please create a new instance of PrestoClient class"
            return False

        # Reset variables
        self.__lasterror = ""
        self.__lastinfouri = ""
        self.__lastnexturi = ""
        self.__lastcanceluri = ""
        self.__laststate = ""
        self.__cancelquery = False
        self.__lastresponse = {}
        self.__columns = {}
        self.__data = []

        try:
            conn = httplib.HTTPConnection(self.__server, self.__port, False, self.__urltimeout)
            conn.request("POST", "/v1/statement", sql, headers)
            response = conn.getresponse()

            # Todo: add check on 503 return codes. In that case retry request. See __openuri

            if response.status != 200:
                self.__lasterror = "Connection error: " + str(response.status) + " " + response.reason
                conn.close()
                return False

            answer = response.read()

            conn.close()

            self.__lastresponse = json.loads(answer)
            self.__getvarsfromresponse()

        except (httplib.HTTPException, socket.error) as e:
            self.__lasterror = "Error connecting to server: " + self.__server + ":" + str(self.__port)
            return False

        else:
            pass

        return True

    def waituntilfinished(self, in_verbose=False):
        """ Returns when query has finished. Override this function to implement your own data retrieval setup.
        For instance to run this function in a separate thread so other threads may request a cancellation.

        Arguments:

        in_verbose -- If True print some simple progress messages (default False)

        """
        tries = 0

        while self.queryisrunning():
            if in_verbose:
                tries += 1
                print "Ping: ", tries, " Rows=", len(self.__data)

            if self.__data:
                sleep(self.__retrievewaittimemsec/1000)
            else:
                sleep(self.__updatewaittimemsec/1000)

        if in_verbose:
            print "Done: ", tries + 1, " Rows=", len(self.__data)

        return

    def queryisrunning(self):
        """ Returns True if query is running. """
        if self.__cancelquery:
            self.__cancel()
            return False

        if self.__lastnexturi == "":
            # This should never happen !
            return False

        if not self.__openuri(self.__lastnexturi):
            return False

        #print "response: ", self.__lastresponse

        self.__getvarsfromresponse()

        if self.__lastnexturi == "":
            return False

        return True

    def getqueryinfo(self):
        """ Requests query information from the Presto server and returns this as a dictonary. The Presto
        server removes this information 15 minutes after finishing the query.

        """
        if self.__lastinfouri == "":
            return {}

        if not self.__openuri(self.__lastinfouri):
            return {}

        return self.__lastresponse

    def cancelquery(self):
        """ Inform Prestoclient to cancel the running query. When queryisrunning() is called
        prestoclient will send a cancel query request to the Presto server.

        """
        self.__cancelquery = True
        return

    def __openuri(self, in_uri):
        """ Internal function, sends a GET request to the Presto server """
        headers = {"X-Presto-Source":  self.__source,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        self.__lasterror = ""

        try:
            retry = True
            retrycount = 0

            while retry:
                retrycount += 1
                conn = urllib2.urlopen(in_uri, None, self.__urltimeout)
                answer = conn.read()
                conn.close()
                self.__lastresponse = json.loads(answer)

                if retrycount > self.__maximumretries:
                    self.__lasterror = "Maximum number of retries reached"
                    return False

                if conn.getcode() == 503:
                    sleep(self.__retrywaittimemsec/1000)
                else:
                    retry = False

        except urllib2.HTTPError as e:
            self.__lasterror = "HTTP error: " + e.reason
            return False

        except urllib2.URLError as e:
            self.__lasterror = "URL error: " + e.reason
            return False

        else:
            pass

        return True

    def __getvarsfromresponse(self):
        """ Internal function, retrieves some information from the response of the Presto server. Keep
        the last known values, except for 'nextUri'.

        """
        if "infoUri" in self.__lastresponse:
            self.__lastinfouri = self.__lastresponse["infoUri"]

        if "nextUri" in self.__lastresponse:
            self.__lastnexturi = self.__lastresponse["nextUri"]
        else:
            self.__lastnexturi = ""

        if "partialCancelUri" in self.__lastresponse:
            self.__lastcanceluri = self.__lastresponse["partialCancelUri"]

        if "state" in self.__lastresponse:
            self.__laststate = self.__lastresponse["state"]
        elif "stats" in self.__lastresponse:
            if "state" in self.__lastresponse["stats"]:
                self.__laststate = self.__lastresponse["stats"]["state"]

        if not self.__columns:
            if "columns" in self.__lastresponse:
                self.__columns = self.__lastresponse["columns"]

        if "data" in self.__lastresponse:
            if self.__data:
                self.__data.extend(self.__lastresponse["data"])
            else:
                self.__data = self.__lastresponse["data"]

        # Determine state
        if self.__lastnexturi != "":
            self.__clientstatus = "RUNNING"
        elif "error" in self.__lastresponse:
            self.__clientstatus = "FAILED"

            # Get errormessage
            if "failureInfo" in self.__lastresponse["error"]:
                if "message" in self.__lastresponse["error"]["failureInfo"]:
                    self.__lasterror = self.__lastresponse["error"]["failureInfo"]["message"]
        else:
            self.__clientstatus = "SUCCEEDED"

        return

    def __cancel(self):
        """ Internal function, sends a cancel request to the Prestoserver. """
        if self.__lastcanceluri == "":
            return False

        headers = {"X-Presto-Source":  self.__source,
                   "User-Agent":       self.__useragent,
                   "X-Presto-User":    self.__user}

        self.__lasterror = ""

        try:
            conn = httplib.HTTPConnection(self.__server, self.__port, False, self.__urltimeout)
            conn.request("DELETE", self.__lastcanceluri, None, headers)
            response = conn.getresponse()

            if response.status != 204:
                self.__lasterror = "Connection error: " + str(response.status) + " " + response.reason
                conn.close()
                return False

            conn.close()

        except httplib.HTTPException:
            self.__lasterror = "Connection error: " + str(httplib.HTTPException.message)
            return False

        else:
            pass

        self.__clientstatus = "NONE"

        return True
