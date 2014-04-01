#!/usr/bin/env python

"""
  hdfs.py is a python client for the thrift interface to HDFS.
  
  Licensed under the Apache License, Version 2.0 (the "License"); 
  you may not use this file except in compliance with the License. 
  You may obtain a copy of the License at 
  
  http://www.apache.org/licenses/LICENSE-2.0 
  
  Unless required by applicable law or agreed to in writing, software 
  distributed under the License is distributed on an "AS IS" BASIS, 
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
  implied. See the License for the specific language governing permissions 
  and limitations under the License. 

"""
import sys
sys.path.append('../gen-py')

from optparse import OptionParser
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hadoopfs import ThriftHadoopFileSystem
from hadoopfs.ttypes import *
from readline import *
from cmd import *
import os
import re
import readline
import subprocess

#
# The address of the FileSystemClientProxy. If the host and port are
# not specified, then a proxy server is automatically spawned. 
#
host = 'localhost'
port = 4677                       # use any port
proxyStartScript = './start_thrift_server.sh'
startServer = True                # shall we start a proxy server?

#
# The hdfs interactive shell. The Cmd class is a builtin that uses readline + implements
# a whole bunch of utility stuff like help and custom tab completions.
# It makes everything real easy.
#
class hadoopthrift_cli(Cmd):

  # my custom prompt looks better than the default
  prompt = 'hdfs>> '

  #############################
  # Class constructor
  #############################
  def __init__(self, server_name, server_port):
    Cmd.__init__(self)
    self.server_name = server_name
    self.server_port = server_port

  #############################
  # Start the ClientProxy Server if we can find it.
  # Read in its stdout to determine what port it is running on
  #############################
  def startProxyServer(self):
    try:
      p = subprocess.Popen(proxyStartScript, self.server_port, stdout=subprocess.PIPE)
      content = p.stdout.readline()
      p.stdout.close()
      val = re.split( '\[|\]', content)
      print val[1]
      self.server_port = val[1]
      return True

    except Exception, ex:
      print "ERROR in starting proxy  server " + proxyStartScript
      print '%s' % (ex.message)
      return False

  #############################
  # Connect to clientproxy
  #############################
  def connect(self):
    try:
      # connect to hdfs thrift server
      self.transport = TSocket.TSocket(self.server_name, self.server_port)
      self.transport = TTransport.TBufferedTransport(self.transport)
      self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

      # Create a client to use the protocol encoder
      self.client = ThriftHadoopFileSystem.Client(self.protocol)
      self.transport.open()

      # tell the HadoopThrift server to die after 60 minutes of inactivity
      self.client.setInactivityTimeoutPeriod(60*60)
      return True

    except Thrift.TException, tx:
      print "ERROR in connecting to ", self.server_name, ":", self.server_port
      print '%s' % (tx.message)
      return False


  #
  # Disconnect from client proxy
  #
  def shutdown(self):
    try :
      self.transport.close()
    except Exception, tx:
      return False

  #############################
  # Create the specified file. Returns a handle to write data.
  #############################
  def do_create(self, name):
    if name == "":
      print "  ERROR usage: create <pathname>"
      print
      return 0

    # Create the file, and immediately closes the handle
    path = Pathname();
    path.pathname = name;
    status = self.client.create(path)
    self.client.close(status)
    return 0

  #############################
  # Delete the specified file.
  #############################
  def do_rm(self, name):
    if name == "":
      print "  ERROR usage: rm <pathname>\n"
      return 0

    # delete file
    path = Pathname();
    path.pathname = name;
    status = self.client.rm(path, False)
    if status == False:
      print "  ERROR in deleting path: " + name
    return 0

  #############################
  # Rename the specified file/dir
  #############################
  def do_mv(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: mv <srcpathname> <destpathname>\n"
      return 0
    src = params[0].strip()
    dest = params[1].strip()

    if src == "":
      print "  ERROR usage: mv <srcpathname> <destpathname>\n"
      return 0
    if dest == "":
      print "  ERROR usage: mv <srcpathname> <destpathname>\n"
      return 0

    # move file
    path = Pathname();
    path.pathname = src;
    destpath = Pathname();
    destpath.pathname = dest;
    status = self.client.rename(path, destpath)
    if status == False:
      print "  ERROR in renaming path: " + name
    return 0

  #############################
  # Delete the specified file.
  #############################
  def do_mkdirs(self, name):
    if name == "":
      print "  ERROR usage: mkdirs <pathname>\n"
      return 0

    # create directory
    path = Pathname();
    path.pathname = name;
    fields = self.client.mkdirs(path)
    return 0

  #############################
  # does the pathname exist?
  #############################
  def do_exists(self, name):
    if name == "":
      print "  ERROR usage: exists <pathname>\n"
      return 0

    # check existence of pathname
    path = Pathname();
    path.pathname = name;
    fields = self.client.exists(path)
    if (fields == True):
      print name + " exists."
    else:
      print name + " does not exist."
    return 0

  #############################
  # copy local file into hdfs
  #############################
  def do_put(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: put <localpathname> <hdfspathname>\n"
      return 0
    local = params[0].strip()
    hdfs = params[1].strip()

    if local == "":
      print "  ERROR usage: put <localpathname> <hdfspathname>\n"
      return 0
    if hdfs == "":
      print "  ERROR usage: put <localpathname> <hdfspathname>\n"
      return 0

    # open local file
    input = open(local, 'rb')

    # open output file
    path = Pathname();
    path.pathname = hdfs;
    output = self.client.create(path)

    # read 1MB at a time and upload to hdfs
    while True:
      chunk = input.read(1024*1024)
      if not chunk: break
      self.client.write(output, chunk)
      
    self.client.close(output) 
    input.close()

  #############################
  # copy hdfs file into local
  #############################
  def do_get(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: get <hdfspathname> <localpathname>\n"
      return 0
    hdfs = params[0].strip()
    local = params[1].strip()

    if local == "":
      print "  ERROR usage: get <hdfspathname> <localpathname>\n"
      return 0
    if hdfs == "":
      print "  ERROR usage: get <hdfspathname> <localpathname>\n"
      return 0

    # open output local file
    output = open(local, 'wb')

    # open input hdfs file
    path = Pathname();
    path.pathname = hdfs;
    input = self.client.open(path)

    # find size of hdfs file
    filesize = self.client.stat(path).length

    # read 1MB bytes at a time from hdfs
    offset = 0
    chunksize = 1024 * 1024
    while True:
      chunk = self.client.read(input, offset, chunksize)
      if not chunk: break
      output.write(chunk)
      offset += chunksize
      if (offset >= filesize): break
      
    self.client.close(input) 
    output.close()

  #############################
  # List attributes of this path
  #############################
  def do_ls(self, name):
    if name == "":
      print "  ERROR usage: list <pathname>\n"
      return 0

    # list file status
    path = Pathname();
    path.pathname = name;
    status = self.client.stat(path)
    if (status.isdir == False):
      self.printStatus(status)
      return 0
    
    # This is a directory, fetch its contents
    liststatus = self.client.listStatus(path)
    for item in liststatus:
      self.printStatus(item)

  #############################
  # Set permissions for a file
  #############################
  def do_chmod(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: chmod 774 <pathname>\n"
      return 0
    perm = params[0].strip()
    name = params[1].strip()

    if name == "":
      print "  ERROR usage: chmod 774 <pathname>\n"
      return 0
    if perm == "":
      print "  ERROR usage: chmod 774 <pathname>\n"
      return 0

    # set permissions (in octal)
    path = Pathname();
    path.pathname = name;
    status = self.client.chmod(path, int(perm,8))
    return 0

  #############################
  # Set owner for a file. This is not an atomic operation.
  # A change to the group of a file may be overwritten by this one.
  #############################
  def do_chown(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: chown <ownername> <pathname>\n"
      return 0
    owner = params[0].strip()
    name = params[1].strip()
    if name == "":
      print "  ERROR usage: chown <ownername> <pathname>\n"
      return 0

    # get the current owner and group
    path = Pathname();
    path.pathname = name;
    cur = self.client.stat(path)

    # set new owner, keep old group
    status = self.client.chown(path, owner, cur.group)
    return 0

  #######################################
  # Set the replication factor for a file
  ######################################
  def do_setreplication(self, line):
    params = line.split()
    if (len(params) != 2):
      print "  ERROR usage: setreplication <replication factor> <pathname>\n"
      return 0
    repl = params[0].strip()
    name = params[1].strip()
    if name == "":
      print "  ERROR usage: setreplication <replication factor> <pathname>\n"
      return 0
    if repl == "":
      print "  ERROR usage: setreplication <replication factor> <pathname>\n"
      return 0

    path = Pathname();
    path.pathname = name;
    status = self.client.setReplication(path, int(repl))
    return 0

  #############################
  # Display the locations of the blocks of this file
  #############################
  def do_getlocations(self, name):
    if name == "":
      print "  ERROR usage: getlocations <pathname>\n"
      return 0
    path = Pathname();
    path.pathname = name;

    # find size of hdfs file
    filesize = self.client.stat(path).length

    # getlocations file
    blockLocations = self.client.getFileBlockLocations(path, 0, filesize)
    for item in blockLocations:
      self.printLocations(item)
    
    return 0

  #############################
  # Utility methods from here
  #############################
  #
  # If I don't do this, the last command is always re-executed which is annoying.
  #
  def emptyline(self):
    pass

  # 
  # print the status of a path
  #
  def printStatus(self, stat):
    print str(stat.block_replication) + "\t" + str(stat.length) + "\t" + str(stat.modification_time) + "\t" + stat.permission + "\t" + stat.owner + "\t" + stat.group + "\t" + stat.path
          
  # 
  # print the locations of a block
  #
  def printLocations(self, location):
    print str(location.names) + "\t"  + str(location.offset) + "\t" + str(location.length)

  #
  # Various ways to exit the hdfs shell
  #
  def do_quit(self,ignored):
    try:
      if startServer:
        self.client.shutdown(1)
      return -1
    except Exception, ex:
      return -1

  def do_q(self,ignored):
    return self.do_quit(ignored)

  # ctl-d
  def do_EOF(self,ignored):
    return self.do_quit(ignored)

  #
  # Give the user some amount of help - I am a nice guy
  #

  def help_create(self):
    print "create <pathname>"

  def help_rm(self):
    print "rm <pathname>"

  def help_mv(self):
    print "mv <srcpathname> <destpathname>"

  def help_mkdirs(self):
    print "mkdirs <pathname>"

  def help_exists(self):
    print "exists <pathname>"

  def help_put(self):
    print "put <localpathname> <hdfspathname>"

  def help_get(self):
    print "get <hdfspathname> <localpathname>"

  def help_ls(self):
    print "ls <hdfspathname>"

  def help_chmod(self):
    print "chmod 775 <hdfspathname>"

  def help_chown(self):
    print "chown <ownername> <hdfspathname>"

  def help_setreplication(self):
    print "setrep <replication factor> <hdfspathname>"

  def help_getlocations(self):
    print "getlocations <pathname>"

  def help_EOF(self):
    print '<ctl-d> will quit this program.'

  def help_quit(self):
    print 'if you need to know what quit does, you shouldn\'t be using a computer.'

  def help_q(self):
    print 'quit and if you need to know what quit does, you shouldn\'t be using a computer.'

  def help_help(self):
    print 'duh'

  def usage(exec_name):
    print "Usage: "
    print "  %s [proxyclientname [proxyclientport]]" % exec_name
    print "  %s -v" % exec_name
    print "  %s --help" % exec_name
    print "  %s -h" % exec_name

if __name__ == "__main__":

  #
  # Rudimentary command line processing.
  #

  # real parsing:
  parser = OptionParser()
  parser.add_option("-e", "--execute", dest="command_str",
                                      help="execute this command and exit")
  parser.add_option("-s","--proxyclient",dest="host",help="the proxyclient's hostname")
  parser.add_option("-p","--port",dest="port",help="the proxyclient's port number")

  (options, args) = parser.parse_args()

  #
  # Save host and port information of the proxy server
  #
  if (options.host):
    host = options.host
    startServer = False
  if (options.port):
    port = options.port
    startServer = False

  #
  # Retrieve the user's readline history.
  #
  historyFileName = os.path.expanduser("~/.hdfs_history")
  if (os.path.exists(historyFileName)):
    readline.read_history_file(historyFileName)

  #
  # Create class and connect to proxy server
  #
  c = hadoopthrift_cli(host,port)

  if startServer:
    if c.startProxyServer() == False:
      sys.exit(1)
  if c.connect() == False:
    sys.exit(1)
    

  #
  # If this utility was invoked with one argument, process it
  #
  if (options.command_str):
    c.onecmd(options.command_str)
    sys.exit(0)

  #
  # Start looping over user commands.
  #
  c.cmdloop('Welcome to the Thrift interactive shell for Hadoop File System. - how can I help you? ' + '\n'
      'Press tab twice to see the list of commands. ' + '\n' +
      'To complete the name of a command press tab once. \n'
      )
  c.shutdown();

  readline.write_history_file(historyFileName)
  print '' # I am nothing if not courteous.
  sys.exit(0)
