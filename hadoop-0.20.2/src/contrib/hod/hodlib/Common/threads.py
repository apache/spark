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

import threading, time, os, sys, pprint

from popen2 import Popen4, Popen3, MAXFD
from signal import SIGTERM, SIGKILL

class baseThread(threading.Thread):
    """Base CAM threading class.  The run method should be overridden."""

    def __init__(self, name):
        threading.Thread.__init__(self, name=name)
        self.stopFlag = threading.Event()
        self.stopFlag.clear()
        self.running = threading.Event()
        self.running.set()
        self.isFinished = threading.Event()
        self.isFinished.clear()

    def join(self, timeout=None):
        self.stopFlag.set()
        threading.Thread.join(self, timeout)

    def pause(self):
        """Pause thread."""

        self.running.clear()

    def cont(self):
        """Resume thread operation."""

        self.running.set()

class simpleCommand(baseThread):
    """Command execution object.  Command output and exit status are captured.

       Public class attributes:

       cmdString    - command to be executed
       outputBuffer - command output, stdout + stderr
       status       - exit status, as returned by wait
       
       stdin        - standard input for command
       stdout       - standard output of command when buffer == False
       stderr       - standard error of command when mode == 3 and buffer == False
       
       """

    def __init__(self, name, cmdString, env=os.environ, mode=4, buffer=True, 
                 wait=True, chdir=None):
        """Class initialization.

           name        - thread name to use when running the command
           cmdString   - command string to execute
           inputString - string to print to command's stdin
           env         - shell environment dictionary
           mode        - 3 for popen3 and 4 for popen4
           buffer      - out put to be retrieved with output() method
           wait        - return immediately after start() is called and output 
                         command results as they come to stdout"""

        baseThread.__init__(self, name=name)

        self.cmdString = cmdString
        self.__mode = mode
        self.__buffer = buffer
        self.__wait = wait
        self.__chdir = chdir
        self.__outputBuffer = []
        self.__status = None
        self.__pid = None
        self.__isFinished = threading.Event()
        self.__isFinished.clear()
        
        self.stdin = None
        self.stdout = None
        self.stderr = None

        self.__env = env
    
    def run(self):
        """ Overridden run method.  Most of the work happens here.  start()
            should be called in place of this method."""
            
        oldDir = None
        if self.__chdir:
            if os.path.exists(self.__chdir):
                oldDir = os.getcwd()  
                os.chdir(self.__chdir)
            else:
                raise Exception(
                    "simpleCommand: invalid chdir specified: %s" % 
                    self.__chdir)
            
        cmd = None
        if self.__mode == 3:
            cmd = _Popen3Env(self.cmdString, env=self.__env)
        else:
            cmd = _Popen4Env(self.cmdString, env=self.__env)
        self.__pid = cmd.pid

        self.stdin = cmd.tochild
        
        if self.__mode == 3:
            self.stderr = cmd.childerr

        while cmd.fromchild == None:
            time.sleep(1)
        
        if self.__buffer == True:
            output = cmd.fromchild.readline()
            while output != '':
                while not self.running.isSet():
                    if self.stopFlag.isSet():
                        break
                    time.sleep(1)
                self.__outputBuffer.append(output)
                output = cmd.fromchild.readline()

        elif self.__wait == False:
            output = cmd.fromchild.readline()
            while output != '':
                while not self.running.isSet():
                    if self.stopFlag.isSet():
                        break
                    time.sleep(1)
                print output,
                if self.stopFlag.isSet():
                    break
                output = cmd.fromchild.readline()
        else:
            self.stdout = cmd.fromchild

        self.__status = cmd.poll()
        while self.__status == -1:
            while not self.running.isSet():
                if self.stopFlag.isSet():
                    break
                time.sleep(1)

            self.__status = cmd.poll()
            time.sleep(1)

        if oldDir:
            os.chdir(oldDir)

        self.__isFinished.set()
        
        sys.exit(0)

    def getPid(self):
        """return pid of the launches process"""
        return self.__pid

    def output(self):
        return self.__outputBuffer[:]

    def wait(self):
        """Wait blocking until command execution completes."""

        self.__isFinished.wait()

        return os.WEXITSTATUS(self.__status)

    def is_running(self):
        """Returns boolean, are we running?"""
        
        status = True
        if self.__isFinished.isSet():
            status = False
            
        return status 

    def exit_code(self):
        """ Returns process exit code."""
        
        if self.__status != None:
            return os.WEXITSTATUS(self.__status)
        else:
            return None
        
    def exit_status_string(self):
        """Return a string representation of the command's exit status."""

        statusString = None
        if self.__status:
            exitStatus = os.WEXITSTATUS(self.__status)
            exitSignal = os.WIFSIGNALED(self.__status)
            coreDump   = os.WCOREDUMP(self.__status)

            statusString = "exit code: %s | signal: %s | core %s" % \
                (exitStatus, exitSignal, coreDump)

        return(statusString)

    def stop(self):
        """Stop the running command and join it's execution thread."""

        self.join()

    def kill(self):
        count = 0
        while self.is_running():
          try:
            if count > 20:
              os.kill(self.__pid, SIGKILL)
              break
            else:  
              os.kill(self.__pid, SIGTERM)
          except:
            break
          
          time.sleep(.1)
          count = count + 1
        
        self.stop()
        
class _Popen3Env(Popen3):
    def __init__(self, cmd, capturestderr=False, bufsize=-1, env=os.environ):
        self._env = env
        Popen3.__init__(self, cmd, capturestderr, bufsize)
    
    def _run_child(self, cmd):
        if isinstance(cmd, basestring):
            cmd = ['/bin/sh', '-c', cmd]
        for i in xrange(3, MAXFD):
            try:
                os.close(i)
            except OSError:
                pass

        try:
            os.execvpe(cmd[0], cmd, self._env)
        finally:
            os._exit(1)
            
class _Popen4Env(_Popen3Env, Popen4):
    childerr = None

    def __init__(self, cmd, bufsize=-1, env=os.environ):
        self._env = env
        Popen4.__init__(self, cmd, bufsize)
        
class loop(baseThread):
    """ A simple extension of the threading.Thread class which continuously
        executes a block of code until join().
    """

    def __init__(self, name, functionRef, functionArgs=None, sleep=1, wait=0,
        offset=False):
        """Initialize a loop object.

           name         - thread name
           functionRef  - a function reference
           functionArgs - function arguments in the form of a tuple,
           sleep        - time to wait between function execs
           wait         - time to wait before executing the first time
           offset       - set true to sleep as an offset of the start of the
                          last func exec instead of the end of the last func
                          exec
        """

        self.__functionRef  = functionRef
        self.__functionArgs = functionArgs
        self.__sleep        = sleep
        self.__wait         = wait
        self.__offset       = offset

        baseThread.__init__(self, name=name)

    def run(self):
        """Do not call this directly.  Call self.start()."""

        startTime = None
        while not self.stopFlag.isSet():
            sleep = self.__sleep
            if self.__wait > 0:
                startWaitCount = 0
                while not self.stopFlag.isSet():
                    while not self.running.isSet():
                        if self.stopFlag.isSet():
                            break
                        time.sleep(1)
                    time.sleep(0.5)
                    startWaitCount = startWaitCount + .5
                    if startWaitCount >= self.__wait:
                        self.__wait = 0
                        break
            startTime = time.time()

            if not self.stopFlag.isSet():
                if self.running.isSet():
                    if self.__functionArgs:
                        self.__functionRef(self.__functionArgs)
                    else:
                        self.__functionRef()
            endTime = time.time()

            while not self.running.isSet():
                time.sleep(1)

            while not self.stopFlag.isSet():
                while not self.running.isSet():
                    if self.stopFlag.isSet():
                        break
                    time.sleep(1)

                currentTime = time.time()
                if self.__offset:
                    elapsed = time.time() - startTime
                else:
                    elapsed = time.time() - endTime

                if elapsed >= self.__sleep:
                    break

                time.sleep(0.5)
        
        self.isFinished.set()

    def set_sleep(self, sleep, wait=None, offset=None):
        """Modify loop frequency paramaters.

           sleep        - time to wait between function execs
           wait         - time to wait before executing the first time
           offset       - set true to sleep as an offset of the start of the
                          last func exec instead of the end of the last func
                          exec
        """

        self.__sleep = sleep
        if wait != None:
            self.__wait = wait
        if offset != None:
            self.__offset = offset

    def get_sleep(self):
        """Get loop frequency paramaters.
        Returns a dictionary with sleep, wait, offset.
        """

        return {
            'sleep'  : self.__sleep,
            'wait'   : self.__wait,
            'offset' : self.__offset,
            }
        
class func(baseThread):
    """ A simple extension of the threading.Thread class which executes 
        a function in a separate thread.
    """

    def __init__(self, name, functionRef, functionArgs=None):
        """Initialize a func object.

           name         - thread name
           functionRef  - a function reference
           functionArgs - function arguments in the form of a tuple,
        """

        self.__functionRef  = functionRef
        self.__functionArgs = functionArgs

        baseThread.__init__(self, name=name)

    def run(self):
        """Do not call this directly.  Call self.start()."""

        if not self.stopFlag.isSet():
            if self.running.isSet():
                if self.__functionArgs:
                    self.__functionRef(self.__functionArgs)
                else:
                    self.__functionRef()
        sys.exit(0)
