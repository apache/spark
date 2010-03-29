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
"""hodLogger provides a customized interface to Python's core logging package.
"""

import sys, os, re, logging, logging.handlers, inspect, pprint, types
from tcp import get_address_tuple

fileFormatString    = '[%(asctime)s] %(levelname)s/%(levelno)s \
%(module)s:%(lineno)s - %(message)s'

streamFormatString  = '%(levelname)s - %(message)s'

debugStreamFormatString = '[%(asctime)s] %(levelname)s/%(levelno)s \
%(module)s:%(lineno)s - %(message)s'

syslogFormatString = '(%(process)d) %(levelname)s/%(levelno)s \
%(module)s:%(lineno)s - %(message)s'

smtpFormatString    = '[%(asctime)s] %(levelname)s/%(levelno)s \
%(module)s:%(lineno)s\n\n%(message)s'

fileFormater = logging.Formatter(fileFormatString)
streamFormater = logging.Formatter(streamFormatString)
debugStreamFormater = logging.Formatter(debugStreamFormatString)
syslogFormater = logging.Formatter(syslogFormatString)
smtpFormater = logging.Formatter(smtpFormatString)

defaultFileLevel = 3
defaultStreamLevel = 4
defaultSyslogLevel = 3
defaultSmtpLevel = 0

hodLogLevelMap = { 0 : logging.CRITICAL,
                   1 : logging.ERROR,
                   2 : logging.WARNING,
                   3 : logging.INFO,
                   4 : logging.DEBUG    }

hodStreamFormatMap = { 0 : streamFormater,
                       1 : streamFormater,
                       2 : streamFormater,
                       3 : streamFormater,
                       4 : debugStreamFormater }

rehodLogLevelMap = {}
for key in hodLogLevelMap.keys():
    rehodLogLevelMap[hodLogLevelMap[key]] = key


reModule = re.compile("^(.*)\..*$")

hodLogs = {}

class hodRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """ This class needs to be used in place of RotatingFileHandler when
        the 2.4.0 Python interpreter is used."""

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline
        [N.B. this may be removed depending on feedback]. If exception
        information is present, it is formatted using
        traceback.print_exception and appended to the stream.

        *****

        THIS IS A HACK, when instances of hodLogger get passed to the child of
        a child thread for some reason self.stream gets closed.  This version
        of emit re-opens self.stream if it is closed.  After testing it appears
        that self.stream is only closed once after the second thread is
        initialized so there is not performance penalty to this hack.  This
        problem only exists in python 2.4.

        *****
        """
        try:
            if self.shouldRollover(record):
                self.doRollover()
            try:
                msg = self.format(record)
                fs = "%s\n"
                if not hasattr(types, "UnicodeType"): #if no unicode support...
                    self.stream.write(fs % msg)
                else:
                    try:
                        self.stream.write(fs % msg)
                    except UnicodeError:
                        self.stream.write(fs % msg.encode("UTF-8"))
                    except ValueError:
                        self.stream = open(self.baseFilename, self.mode)
                        self.stream.write(fs % msg)

                self.flush()
            except:
                self.handleError(record)
        except:
            self.handleError(record)

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        Basically, see if the supplied record would cause the file to exceed
        the size limit we have.

        *****

        THIS IS A HACK, when instances of hodLogger get passed to the child of
        a child thread for some reason self.stream gets closed.  This version
        of emit re-opens self.stream if it is closed.  After testing it appears
        that self.stream is only closed once after the second thread is
        initialized so there is not performance penalty to this hack. This
        problem only exists in python 2.4.

        *****
        """
        if self.maxBytes > 0:                   # are we rolling over?
            msg = "%s\n" % self.format(record)

            try:
                #due to non-posix-compliant Windows feature
                self.stream.seek(0, 2)
            except ValueError:
                self.stream = open(self.baseFilename, self.mode)
                self.stream.seek(0, 2)

            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1
        return 0

class hodCustomLoggingLogger(logging.Logger):
    """ Slight extension of the logging.Logger class used by the hodLog class.
    """
    def findCaller(self):
        """ findCaller() is supposed to return the callers file name and line
            number of the caller. This was broken when the logging package was
            wrapped by hodLog.  We should return much more relevant info now.
            """

        callerModule = ''
        callerLine = 0

        currentModule = os.path.basename(__file__)
        currentModule = reModule.sub("\g<1>", currentModule)

        frames = inspect.stack()
        for i in range(len(frames)):
            frameModule = os.path.basename(frames[i][1])
            frameModule = reModule.sub("\g<1>", frameModule)
            if frameModule == currentModule:
                previousFrameModule = os.path.basename(frames[i+1][1])
                previousFrameModule = reModule.sub("\g<1>",
                    previousFrameModule)
                callerFile = frames[i+1][1]
                callerLine = frames[i+1][2]
                continue

        returnValues = (callerFile, callerLine)
        if sys.version.startswith('2.4.4') or sys.version.startswith('2.5'):
            returnValues = (callerFile, callerLine, None)
            
        return returnValues

class hodLog:
    """ Cluster management logging class.

        logging levels: 0 - log only critical messages
                        1 - log critical and error messages
                        2 - log critical, error, and warning messages
                        3 - log critical, error, warning, and info messages
                        4 - log critical, error, warning, info, and debug
                            messages"""

    def __init__(self, appName):
        """Constructs a hodLogger object.

        appName      - name of logging application, log filenames will be
                       prepended with this name"""

        self.__appName = appName

        # initialize a dictionary to hold loggerNames
        self.__loggerNames = {}

        # initialize a dictionary to track log handlers and handler classes
        self.__logObjs = { 'file' : {}, 'smtp' : {}, 
                           'syslog' : {}, 'strm' : {} }

        # use a custom logging.Logger class
        logging.setLoggerClass(hodCustomLoggingLogger)

        # get the root app logger
        self.__logger = logging.getLogger(appName)
        self.__logger.setLevel(logging.DEBUG)
        
        hodLogs[self.__appName] = self

    def __attr__(self, attrname):
        """loggerNames  - list of defined logger names"""

        if attrname   == "loggerNames":  return self.__loggerNames.keys()
        else: raise AttributeError, attrname

    def __repr__(self):
        """Returns a string representation of a hodLog object of the form:

           LOG_NAME
                file: FILENAME (level LEVEL)
                smtp: SMTP_SERVER from FROM_ADDRESS (level LEVEL)
                strm: STRM_OBJECT (level LEVEL)
                ... """

        hodLogString = "hodLog: %s\n\n" % self.__appName
        for loggerName in self.__loggerNames.keys():
            hodLogString = "%s    logger: %s\n" % (hodLogString, loggerName)
            handlerClasses = self.__logObjs.keys()
            handlerClasses.sort()
            for handlerClass in handlerClasses:
                try:
                    loggerLevelName = logging.getLevelName(
                        self.__logObjs[handlerClass][loggerName]['level'])
                    hodLogString = "%s        %s: %s (level %s)\n" % (
                        hodLogString, handlerClass,
                        self.__logObjs[handlerClass][loggerName]['data'],
                        loggerLevelName)
                except:
                    hodLogString = "%s        %s: none\n" % (
                        hodLogString, handlerClass)
            hodLogString = "%s\n" % hodLogString

        return hodLogString

    # 'private' method which adds handlers to self.__logObjs
    def __add_to_handlers(self, handlerClass, loggerName, handler, data,
        level):
        self.__logObjs[handlerClass][loggerName] = {}
        self.__logObjs[handlerClass][loggerName]['handler'] = handler
        self.__logObjs[handlerClass][loggerName]['data'] = data
        self.__logObjs[handlerClass][loggerName]['level'] = level

    # 'private' method which determines whether a hod log level is valid and
    #   returns a valid logging.Logger level
    def __get_logging_level(self, level, defaultLevel):
        loggingLevel = ''
        try:
            loggingLevel = hodLogLevelMap[int(level)]
        except:
            loggingLevel = hodLogLevelMap[defaultLevel]

        return loggingLevel

    # make a logging.logger name rootLogger.childLogger in our case the
    #   appName.componentName
    def __get_logging_logger_name(self, loggerName):
        return "%s.%s" % (self.__appName, loggerName)

    def add_logger(self, loggerName):
        """Adds a logger of name loggerName.

           loggerName    - name of component of a given application doing the
                           logging

           Returns a hodLogger object for the just added logger."""

        try:
            self.__loggerNames[loggerName]
        except:
            loggingLoggerName = self.__get_logging_logger_name(loggerName)
            logging.getLogger(loggingLoggerName)

            self.__loggerNames[loggerName] = 1

            return hodLogger(self.__appName, loggingLoggerName)

    def add_file(self, logDirectory, maxBytes=0, backupCount=0,
        level=defaultFileLevel, addToLoggerNames=None):
        """Adds a file handler to all defined loggers or a specified set of
           loggers.  Each log file will be located in logDirectory and have a
           name of the form appName-loggerName.log.

           logDirectory     - logging directory
           maxBytes         - maximum log size to write in bytes before rotate
           backupCount      - number of rotated logs to keep
           level            - cluster management log level
           addToLoggerNames - list of logger names to which stream handling
                              will be added"""

        def add_file_handler(loggerName):
            if not self.__logObjs['file'].has_key(loggerName):
                loggingLevel = self.__get_logging_level(level,
                    defaultFileLevel)

                logFile = os.path.join(logDirectory, "%s-%s.log" % (
                    self.__appName, loggerName))

                logFilePresent = False
                if(os.path.exists(logFile)):
                  logFilePresent = True

                if sys.version.startswith('2.4'):
                    fileHandler = hodRotatingFileHandler(logFile,
                        maxBytes=maxBytes, backupCount=backupCount)
                else:
                    fileHandler = logging.handlers.RotatingFileHandler(logFile,
                        maxBytes=maxBytes, backupCount=backupCount)
                if logFilePresent and backupCount:
                  fileHandler.doRollover()

                fileHandler.setLevel(loggingLevel)
                fileHandler.setFormatter(fileFormater)

                loggingLoggerName = self.__get_logging_logger_name(loggerName)
                aLogger = logging.getLogger(loggingLoggerName)
                aLogger.addHandler(fileHandler)

                fileData = "%s" % logFile
                self.__add_to_handlers('file', loggerName, fileHandler,
                    fileData, loggingLevel)

        if addToLoggerNames:
            for loggerName in addToLoggerNames:
                add_file_handler(loggerName)
        else:
            for loggerName in self.__loggerNames:
                add_file_handler(loggerName)

    def add_stream(self, stream=sys.stderr, level=defaultStreamLevel,
        addToLoggerNames=None):
        """Adds a stream handler to all defined loggers or a specified set of
           loggers.

           stream           - a stream such as sys.stderr or sys.stdout
           level            - cluster management log level
           addToLoggerNames - tupple of logger names to which stream handling
                              will be added"""

        def add_stream_handler(loggerName):
            if not self.__logObjs['strm'].has_key(loggerName):
                loggingLevel = self.__get_logging_level(level,
                    defaultStreamLevel)

                streamHandler = logging.StreamHandler(stream)
                
                streamHandler.setLevel(loggingLevel)
                
                streamHandler.setFormatter(hodStreamFormatMap[int(level)])

                loggingLoggerName = self.__get_logging_logger_name(loggerName)
                aLogger = logging.getLogger(loggingLoggerName)
                aLogger.addHandler(streamHandler)

                streamData = "%s" % stream
                self.__add_to_handlers('strm', loggerName, streamHandler,
                    streamData, loggingLevel)

        if addToLoggerNames:
            for loggerName in addToLoggerNames:
                add_stream_handler(loggerName)
        else:
            for loggerName in self.__loggerNames:
                add_stream_handler(loggerName)

    def add_syslog(self, address, level=defaultSyslogLevel, 
                   addToLoggerNames=None):
        def add_syslog_handler(loggerName):
            if not self.__logObjs['syslog'].has_key(loggerName):
                loggingLevel = self.__get_logging_level(level,
                    defaultSyslogLevel)

                address[1] = int(address[1])
                syslogHandler = logging.handlers.SysLogHandler(tuple(address),
                                                               9)
                
                syslogHandler.setLevel(loggingLevel)
                
                syslogHandler.setFormatter(syslogFormater)

                loggingLoggerName = self.__get_logging_logger_name(loggerName)
                aLogger = logging.getLogger(loggingLoggerName)
                aLogger.addHandler(syslogHandler)

                syslogData = "%s:%s" % (address[0], address[1])
                self.__add_to_handlers('syslog', loggerName, syslogHandler,
                    syslogData, loggingLevel)

        if addToLoggerNames:
            for loggerName in addToLoggerNames:
                add_syslog_handler(loggerName)
        else:
            for loggerName in self.__loggerNames:
                add_syslog_handler(loggerName)      
      

    def add_smtp(self, mailHost, fromAddress, toAddresses,
        level=defaultSmtpLevel, addToLoggerNames=None):
        """Adds an SMTP handler to all defined loggers or a specified set of
           loggers.

           mailHost         - SMTP server to used when sending mail
           fromAddress      - email address to use as the from address when
                              sending mail
           toAdresses       - comma seperated list of email address to which
                              mail will be sent
           level            - cluster management log level
           addToLoggerNames - tupple of logger names to which smtp handling
                              will be added"""

        def add_email_handler(loggerName):
            if not self.__logObjs['smtp'].has_key(loggerName):
                loggingLevel = self.__get_logging_level(level,
                    defaultSmtpLevel)

                subject = loggerName
                if   loggingLevel == 50:
                    subject = "%s - a critical error has occured." % subject
                elif loggingLevel == 40:
                    subject = "%s - an error has occured."         % subject
                elif loggingLevel == 30:
                    subject = "%s - warning message."              % subject
                elif loggingLevel == 20:
                    subject = "%s - information message."          % subject
                elif loggingLevel == 10:
                    subject = "%s - debugging message."            % subject

                mailHostTuple = get_address_tuple(mailHost)
                emailHandler = logging.handlers.SMTPHandler(mailHostTuple,
                    fromAddress, toAddresses, subject)

                emailHandler.setFormatter(smtpFormater)
                emailHandler.setLevel(loggingLevel)

                loggingLoggerName = self.__get_logging_logger_name(loggerName)
                aLogger = logging.getLogger(loggingLoggerName)
                aLogger.addHandler(emailHandler)

                emailData = "%s from %s" % (mailHost, fromAddress)
                self.__add_to_handlers('smtp', loggerName, emailHandler,
                    emailData, loggingLevel)

        if addToLoggerNames:
            for loggerName in addToLoggerNames:
                add_email_handler(loggerName)
        else:
            for loggerName in self.__loggerNames:
                add_email_handler(loggerName)

    def status(self):
        statusStruct = {}
        for loggerName in self.__loggerNames.keys():
            statusStruct[loggerName] = []
            for handlerClass in self.__logObjs.keys():
                loggerDict = {}
                try:
                    level = self.__logObjs[handlerClass][loggerName]['level']
                    level = rehodLogLevelMap[level]

                    loggerDict['handler'] = handlerClass
                    loggerDict['level'] = level
                    loggerDict['data'] = \
                        self.__logObjs[handlerClass][loggerName]['data']
                except:
                    pass
                else:
                    statusStruct[loggerName].append(loggerDict)

        return statusStruct

    def lock_handlers(self):
        for handlerClass in self.__logObjs.keys():
            for loggerName in self.__logObjs[handlerClass].keys():
                self.__logObjs[handlerClass][loggerName]['handler'].acquire()

    def release_handlers(self):
        for handlerClass in self.__logObjs.keys():
            for loggerName in self.__logObjs[handlerClass].keys():
                self.__logObjs[handlerClass][loggerName]['handler'].release()

    def get_level(self, handler, loggerName):
        return rehodLogLevelMap[self.__logObjs[handler][loggerName]['level']]

    def set_level(self, handler, loggerName, level):
        """Sets the logging level of a particular logger and logger handler.

           handler    - handler (smtp, file, or stream)
           loggerName - logger to set level on
           level      - level to set logger
        """

        level = self.__get_logging_level(level, defaultFileLevel)
        self.__logObjs[handler][loggerName]['handler'].setLevel(level)
        self.__logObjs[handler][loggerName]['level'] = level
        
        if handler == 'stream':
            self.__logObjs[handler][loggerName]['handler'].setFormatter(
                hodStreamFormatMap[int(level)])

    def set_logger_level(self, loggerName, level):
        status = 0
        for handlerClass in self.__logObjs.keys():
            if self.__logObjs[handlerClass].has_key(loggerName):
                self.set_level(handlerClass, loggerName, level)
            else:
                status = 1
        
        return status

    def rollover(self, loggerName):
        status = 0 
        if self.__logObjs['file'].has_key(loggerName):
            if self.__logObjs['file'][loggerName]['handler'].shouldRollover():
                self.__logObjs['file'][loggerName]['handler'].doRollover()
        else:
            status = 1
            
        return status
        
    def set_max_bytes(self, maxBytes):
        status = 0
        if self.__logObjs.has_key('file'):
            for loggerName in self.__logObjs['file'].keys():
                self.__logObjs['file'][loggerName]['handler'].maxBytes = 0
        else:
            status = 1
            
        return status

    def get_logger(self, loggerName):
        """ Returns a hodLogger object for a logger by name. """

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        return hodLogger(self.__appName, loggingLoggerName)

    def critical(self, loggerName, msg):
        """Logs a critical message and flushes log buffers.  This method really
           should only be called upon a catastrophic failure.

           loggerName - logger to use
           msg        - message to be logged"""

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        logger = logging.getLogger(loggingLoggerName)
        logger.critical(msg)
        self.flush()

    def error(self, loggerName, msg):
        """Logs an error message and flushes log buffers.

           loggerName - logger to use
           msg        - message to be logged"""

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        logger = logging.getLogger(loggingLoggerName)
        logger.error(msg)
        self.flush()

    def warn(self, loggerName, msg):
        """Logs a warning message.

           loggerName - logger to use
           msg        - message to be logged"""

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        logger = logging.getLogger(loggingLoggerName)
        logger.warn(msg)

    def info(self, loggerName, msg):
        """Logs an information message.

           loggerName - logger to use
           msg        - message to be logged"""

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        logger = logging.getLogger(loggingLoggerName)
        logger.info(msg)

    def debug(self, loggerName, msg):
        """Logs a debugging message.

           loggerName - logger to use
           msg        - message to be logged"""

        loggingLoggerName = self.__get_logging_logger_name(loggerName)
        logger = logging.getLogger(loggingLoggerName)
        logger.debug(msg)

    def flush(self):
        """Flush all log handlers."""

        for handlerClass in self.__logObjs.keys():
            for loggerName in self.__logObjs[handlerClass].keys():
                self.__logObjs[handlerClass][loggerName]['handler'].flush()

    def shutdown(self):
        """Shutdown all logging, flushing all buffers."""

        for handlerClass in self.__logObjs.keys():
            for loggerName in self.__logObjs[handlerClass].keys():
                self.__logObjs[handlerClass][loggerName]['handler'].flush()
                # Causes famous 'ValueError: I/O operation on closed file'
                # self.__logObjs[handlerClass][loggerName]['handler'].close()

class hodLogger:
    """ Encapsulates a particular logger from a hodLog object. """
    def __init__(self, appName, loggingLoggerName):
        """Constructs a hodLogger object (a particular logger in a hodLog
           object).

           loggingLoggerName - name of a logger in hodLog object"""

        self.__appName = appName
        self.__loggerName = loggingLoggerName
        self.__logger = logging.getLogger(self.__loggerName)

    def __repr__(self):
        """Returns a string representation of a hodComponentLog object."""

        return "%s hodLog" % self.__loggerName

    def __call__(self):
        pass

    def set_logger_level(self, loggerName, level):
        
        return hodLogs[self.__appName].set_logger_level(loggerName, level)
        
    def set_max_bytes(self, maxBytes):
            
        return hodLogs[self.__appName].set_max_bytes(maxBytes)

    def rollover(self):
        return hodLogs[self.__appName].rollover(self.__loggerName)

    def get_level(self, handler, loggerName):
    
        return hodLogs[self.__appName].get_level(handler, loggerName)
        
    def critical(self, msg):
        """Logs a critical message and calls sys.exit(1).

           msg     - message to be logged"""

        self.__logger.critical(msg)

    def error(self, msg):
        """Logs an error message.

           msg     - message to be logged"""

        self.__logger.error(msg)

    def warn(self, msg):
        """Logs a warning message.

           msg     - message to be logged"""

        self.__logger.warn(msg)

    def info(self, msg):
        """Logs an information message.

           msg     - message to be logged"""

        self.__logger.info(msg)

    def debug(self, msg):
        """Logs a debugging message.

           msg     - message to be logged"""

        self.__logger.debug(msg)

class hodDummyLogger:
    """ Dummy hodLogger class.  Other hod classes requiring a hodLogger default
        to this hodLogger if no logger is passed."""

    def __init__(self):
        """pass"""

        pass

    def __repr__(self):
        return "dummy hodLogger"

    def __call__(self):
        """pass"""

        pass

    def set_logger_level(self, loggerName, level):
        
        return 0
        
    def set_max_bytes(self, loggerName, maxBytes):
            
        return 0

    def get_level(self, handler, loggerName):
        
        return 4

    def rollover(self):
        
        return 0

    def critical(self, msg):
        """pass"""

        pass

    def error(self, msg):
        """pass"""

        pass

    def warn(self, msg):
        """pass"""

        pass

    def info(self, msg):
        """pass"""

        pass

    def debug(self, msg):
        """pass"""

        pass

def ensureLogDir(logDir):
  """Verify that the passed in log directory exists, and if it doesn't
  create it."""
  if not os.path.exists(logDir):
    try:
      old_mask = os.umask(0)
      os.makedirs(logDir, 01777)
      os.umask(old_mask)
    except Exception, e:
      print >>sys.stderr, "Could not create log directories %s. Exception: %s. Stack Trace: %s" % (logDir, get_exception_error_string(), get_exception_string())
      raise e

def getLogger(cfg, logName):
  if cfg['debug'] > 0:
    user = cfg['userid']
    baseLogger = hodLog(logName)
    log = baseLogger.add_logger('main')

    if cfg.has_key('log-dir'):
      serviceId = os.getenv('PBS_JOBID')
      if serviceId:
        logDir = os.path.join(cfg['log-dir'], "%s.%s" % (user, serviceId))
      else:
        logDir = os.path.join(cfg['log-dir'], user) 
      if not os.path.exists(logDir):
        os.mkdir(logDir)

      baseLogger.add_file(logDirectory=logDir, level=cfg['debug'], 
               addToLoggerNames=('main',))

    try:
      if cfg.has_key('stream') and cfg['stream']:
        baseLogger.add_stream(level=cfg['debug'], addToLoggerNames=('main',))

      if cfg.has_key('syslog-address'):
        baseLogger.add_syslog(cfg['syslog-address'], 
          level=cfg['debug'], addToLoggerNames=('main',))
    except Exception,e:
      # Caught an exception while initialising logger
      log.critical("%s Logger failed to initialise. Reason : %s" % (logName, e))
      pass
    return log
