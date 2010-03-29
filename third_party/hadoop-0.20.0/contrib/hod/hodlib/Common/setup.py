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
# $Id:setup.py 5158 2007-04-09 00:14:35Z zim $
#
#------------------------------------------------------------------------------

"""'setup' provides for reading and verifing configuration files based on
   Python's SafeConfigParser class."""

import sys, os, re, pprint

from ConfigParser import SafeConfigParser
from optparse import OptionParser, IndentedHelpFormatter, OptionGroup
from util import get_perms, replace_escapes
from types import typeValidator, typeValidatorInstance, is_valid_type, \
                  typeToString
from hodlib.Hod.hod import hodHelp

reEmailAddress = re.compile("^.*@.*$")
reEmailDelimit = re.compile("@")
reComma = re.compile("\s*,\s*")
reDot = re.compile("\.")
reCommentHack = re.compile("^.*?\s+#|;.*", flags=re.S)
reCommentNewline = re.compile("\n|\r$")
reKeyVal = r"(?<!\\)="
reKeyVal = re.compile(reKeyVal)
reKeyValList = r"(?<!\\),"
reKeyValList = re.compile(reKeyValList)

errorPrefix = 'error'
requiredPerms = '0660'

class definition:
    def __init__(self):
        """Generates a configuration definition object."""
        self.__def = {}
        self.__defOrder = []

    def __repr__(self):
        return pprint.pformat(self.__def)  

    def __getitem__(self, section):
        return self.__def[section]

    def __iter__(self):
        return iter(self.__def)

    def sections(self):
        """Returns a list of sections/groups."""
        
        if len(self.__defOrder):
            return self.__defOrder
        else:  
            return self.__def.keys()
      
    def add_section(self, section):
        """Add a configuration section / option group."""
        
        if self.__def.has_key(section):
            raise Exception("Section already exists: '%s'" % section)
        else:
            self.__def[section] = {}

    def add_def(self, section, var, type, desc, help = True, default = None, 
                req = True, validate = True, short = None):
        """ Add a variable definition.
        
            section  - section name
            var      - variable name
            type     - valid hodlib.types
            desc     - description of variable
            help     - display help for this variable
            default  - default value
            req      - bool, requried?
            validate - bool, validate type value?
            short    - short symbol (1 character),
            help     - bool, display help?"""
            
        if self.__def.has_key(section):
            if not is_valid_type(type):
                raise Exception("Type (type) is invalid: %s.%s - '%s'" % (section, var, 
                                                                type))
            if not isinstance(desc, str):
                raise Exception("Description (desc) must be a string: %s.%s - '%s'" % (
                    section, var, desc))
            if not isinstance(req, bool):
                raise Exception("Required (req) must be a bool: %s.%s - '%s'" % (section, 
                                                                       var, 
                                                                       req))
            if not isinstance(validate, bool):
                raise Exception("Validate (validate) must be a bool: %s.%s - '%s'" % (
                    section, var, validate))
              
            if self.__def[section].has_key(var):
                raise Exception("Variable name already defined: '%s'" % var)
            else:
                self.__def[section][var] = { 'type'     : type,
                                             'desc'     : desc,
                                             'help'     : help,
                                             'default'  : default,
                                             'req'      : req,
                                             'validate' : validate,
                                             'short'    : short }                
        else:    
            raise Exception("Section does not exist: '%s'" % section)
          
    def add_defs(self, defList, defOrder=None):
        """ Add a series of definitions.
        
            defList = { section0 : ((name0, 
                                     type0, 
                                     desc0, 
                                     help0,
                                     default0,
                                     req0, 
                                     validate0,
                                     short0),
                                  ....
                                    (nameN, 
                                     typeN, 
                                     descN,
                                     helpN, 
                                     defaultN, 
                                     reqN, 
                                     validateN,
                                     shortN)),             
                           ....
                           
                        sectionN : ... }
                        
            Where the short synmbol is optional and can only be one char."""
                        
        for section in defList.keys():
            self.add_section(section)
            for defTuple in defList[section]:
                if isinstance(defTuple, tuple): 
                    if len(defTuple) < 7:
                        raise Exception(
                            "section %s is missing an element: %s" % (
                            section, pprint.pformat(defTuple)))
                else:
                    raise Exception("section %s of defList is not a tuple" % 
                                    section)
                
                if len(defTuple) == 7:
                    self.add_def(section, defTuple[0], defTuple[1], 
                                 defTuple[2], defTuple[3], defTuple[4], 
                                 defTuple[5], defTuple[6])
                else:
                    self.add_def(section, defTuple[0], defTuple[1], 
                                 defTuple[2], defTuple[3], defTuple[4], 
                                 defTuple[5], defTuple[6], defTuple[7])                     
        if defOrder:
            for section in defOrder:
                if section in self.__def:
                    self.__defOrder.append(section)
                    
            for section in self.__def:
                if not section in defOrder:
                    raise Exception(
                        "section %s is missing from specified defOrder." % 
                        section)
            
class baseConfig:
    def __init__(self, configDef, originalDir=None):
        self.__toString = typeToString()
        self.__validated = False
        self._configDef = configDef
        self._options = None
        self._mySections = []
        self._dict = {}
        self.configFile = None
        self.__originalDir = originalDir

        if self._configDef:
            self._mySections = configDef.sections()

    def __repr__(self):
        """Returns a string representation of a config object including all
           normalizations."""

        print_string = '';
        for section in self._mySections:
            print_string = "%s[%s]\n" % (print_string, section)
            options = self._dict[section].keys()
            for option in options:
                print_string = "%s%s = %s\n" % (print_string, option,
                    self._dict[section][option])

            print_string = "%s\n" % (print_string)

        print_string = re.sub("\n\n$", "", print_string)

        return print_string

    def __getitem__(self, section):
        """ Returns a dictionary of configuration name and values by section.
        """
        return self._dict[section]

    def __setitem__(self, section, value):
        self._dict[section] = value

    def __iter__(self):
        return iter(self._dict)

    def has_key(self, section):
        status = False
        if section in self._dict:
            status = True
            
        return status

    # Prints configuration error messages
    def var_error(self, section, option, *addData):
        errorStrings = []  
        if not self._dict[section].has_key(option):
          self._dict[section][option] = None
        errorStrings.append("%s: invalid '%s' specified in section %s (--%s.%s): %s" % (
            errorPrefix, option, section, section, option, self._dict[section][option]))

        if addData:
            errorStrings.append("%s: additional info: %s\n" % (errorPrefix,
                addData[0]))
        return errorStrings

    def var_error_suggest(self, errorStrings):
        if self.configFile:
            errorStrings.append("Check your command line options and/or " + \
                              "your configuration file %s" % self.configFile)
    
    def __get_args(self, section):
        def __dummyToString(type, value):
            return value
        
        toString = __dummyToString
        if self.__validated:
            toString = self.__toString
            
        args = []
        if isinstance(self._dict[section], dict):
            for option in self._dict[section]:
                if section in self._configDef and \
                option in self._configDef[section]:
                  if self._configDef[section][option]['type'] == 'bool':
                    if self._dict[section][option] == 'True' or \
                        self._dict[section][option] == True:
                        args.append("--%s.%s" % (section, option))
                  else:
                    args.append("--%s.%s" % (section, option))
                    args.append(toString(
                           self._configDef[section][option]['type'], 
                           self._dict[section][option]))
        else:
            if section in self._configDef:
              if self._configDef[section][option]['type'] == 'bool':
                if self._dict[section] == 'True' or \
                    self._dict[section] == True:
                    args.append("--%s" % section)
              else:
                if self._dict[section] != 'config':
                  args.append("--%s" % section)
                  args.append(toString(self._configDef[section]['type'], 
                                             self._dict[section]))
                    
        return args
                
    def values(self):
        return self._dict.values()
      
    def keys(self):
        return self._dict.keys()
    
    def get_args(self, exclude=None, section=None):
        """Retrieve a tuple of config arguments."""
        
        args = []
        if section:
            args = self.__get_args(section)
        else:
            for section in self._dict:
                if exclude:
                    if not section in exclude:
                        args.extend(self.__get_args(section))
                else:
                    args.extend(self.__get_args(section))
        
        return tuple(args)
        
    def verify(self):
        """Verifies each configuration variable, using the configValidator
           class, based on its type as defined by the dictionary configDef.
           Upon encountering a problem an error is printed to STDERR and
           false is returned."""
        
        oldDir = os.getcwd()
        if self.__originalDir:
          os.chdir(self.__originalDir)
        
        status = True
        statusMsgs = []
	
        if self._configDef:
            errorCount = 0
            configValidator = typeValidator(self.__originalDir)

            # foreach section and option by type string as defined in configDef
            #   add value to be validated to validator
            for section in self._mySections:
                for option in self._configDef[section].keys():
                    configVarName = "%s.%s" % (section, option)

                    if self._dict[section].has_key(option):
                        if self._configDef[section][option].has_key('validate'):
                            if self._configDef[section][option]['validate']:
                                # is the section.option needed to be validated?
                                configValidator.add(configVarName,
                                    self._configDef[section][option]['type'],
                                    self._dict[section][option])
                            else:
                                # If asked not to validate, just normalize
                                self[section][option] = \
                                    configValidator.normalize(
                                    self._configDef[section][option]['type'], 
                                    self._dict[section][option])
                            if self._configDef[section][option]['default'] != \
                                None:
                                self._configDef[section][option]['default'] = \
                                    configValidator.normalize(
                                    self._configDef[section][option]['type'],
                                    self._configDef[section][option]['default']
                                    )
                                self._configDef[section][option]['default'] = \
                                    self.__toString(
                                    self._configDef[section][option]['type'], 
                                    self._configDef[section][option]['default']
                                    )
                        else:        
                            # This should not happen. Just in case, take this as 'to be validated' case.
                            configValidator.add(configVarName,
                                self._configDef[section][option]['type'],
                                self._dict[section][option])
                    elif self._configDef[section][option]['req']:
                        statusMsgs.append("%s: %s.%s is not defined."
                             % (errorPrefix, section, option))
                        errorCount = errorCount + 1                         

            configValidator.validate()

            for valueInfo in configValidator.validatedInfo:
                sectionsOptions = reDot.split(valueInfo['name'])

                if valueInfo['isValid'] == 1:
                    self._dict[sectionsOptions[0]][sectionsOptions[1]] = \
                        valueInfo['normalized']
                else:
                    if valueInfo['errorData']:
                        statusMsgs.extend(self.var_error(sectionsOptions[0],
                            sectionsOptions[1], valueInfo['errorData']))
                    else:
                        statusMsgs.extend(self.var_error(sectionsOptions[0],
                            sectionsOptions[1]))
                    errorCount = errorCount + 1

            if errorCount > 1:
                statusMsgs.append( "%s: %s problems found." % (
                    errorPrefix, errorCount))
                self.var_error_suggest(statusMsgs)
                status = False
            elif errorCount > 0:
                statusMsgs.append( "%s: %s problem found." % (
                    errorPrefix, errorCount))
                self.var_error_suggest(statusMsgs)
                status = False
        
        self.__validated = True

        if self.__originalDir:
          os.chdir(oldDir)

        return status,statusMsgs

    def normalizeValue(self, section, option)  :
      return typeValidatorInstance.normalize(
                                  self._configDef[section][option]['type'],
                                  self[section][option])

    def validateValue(self, section, option):
      # Validates a section.option and exits on error
      valueInfo = typeValidatorInstance.verify(
                                  self._configDef[section][option]['type'],
                                  self[section][option])
      if valueInfo['isValid'] == 1:
        return []
      else:
        if valueInfo['errorData']:
          return self.var_error(section, option, valueInfo['errorData'])
        else:
          return self.var_error(section, option)

class config(SafeConfigParser, baseConfig):
    def __init__(self, configFile, configDef=None, originalDir=None, 
                 options=None, checkPerms=False):
        """Constructs config object.

           configFile - configuration file to read
           configDef  - definition object
           options    - options object
           checkPerms - check file permission on config file, 0660

           sample configuration file:

            [snis]
            modules_dir  = modules/       ; location of infoModules
            md5_defs_dir = etc/md5_defs   ; location of infoTree md5 defs
            info_store   = var/info       ; location of nodeInfo store
            cam_daemon   = localhost:8200 ; cam daemon address"""


        SafeConfigParser.__init__(self)
        baseConfig.__init__(self, configDef, originalDir)

        if(os.path.exists(configFile)):
          self.configFile = configFile
        else:
          raise IOError
        
        self._options = options
        
	## UNUSED CODE : checkPerms is never True
  ## zim: this code is used if one instantiates config() with checkPerms set to
  ## True.
        if checkPerms: self.__check_perms()

        self.read(configFile)

        self._configDef = configDef
        if not self._configDef:
            self._mySections = self.sections()

        self.__initialize_config_dict()

    def __initialize_config_dict(self):
        """ build a dictionary of config vars keyed by section name defined in
           configDef, if options defined override config"""

        for section in self._mySections:
            items = self.items(section)
            self._dict[section] = {}

            # First fill self._dict with whatever is given in hodrc.
            # Going by this, options given at the command line either override
            # options in hodrc, or get appended to the list, like for
            # hod.client-params. Note that after this dict has _only_ hodrc
            # params
            for keyValuePair in items:
                # stupid commenting bug in ConfigParser class, lines without an
                #  option value pair or section required that ; or # are at the
                #  beginning of the line, :(
                newValue = reCommentHack.sub("", keyValuePair[1])
                newValue = reCommentNewline.sub("", newValue)
                self._dict[section][keyValuePair[0]] = newValue
            # end of filling with options given in hodrc
            # now start filling in command line options
            if self._options:    
                for option in self._configDef[section].keys():
                    if self._options[section].has_key(option):
                        # the user has given an option
                        compoundOpt = "%s.%s" %(section,option)
                        if ( compoundOpt == \
                              'gridservice-mapred.final-server-params' \
                              or compoundOpt == \
                                    'gridservice-hdfs.final-server-params' \
                              or compoundOpt == \
                                    'gridservice-mapred.server-params' \
                              or compoundOpt == \
                                    'gridservice-hdfs.server-params' \
                              or compoundOpt == \
                                    'hod.client-params' ):
                 
                           if ( compoundOpt == \
                              'gridservice-mapred.final-server-params' \
                              or compoundOpt == \
                                    'gridservice-hdfs.final-server-params' ):
                              overwrite = False
                           else: overwrite = True

                           # Append to the current list of values in self._dict
                           if not self._dict[section].has_key(option):
                             self._dict[section][option] = ""
                           dictOpts = reKeyValList.split(self._dict[section][option])
                           dictOptsKeyVals = {}
                           for opt in dictOpts:
                              if opt != '':
                                # when dict _has_ params from hodrc
                                if reKeyVal.search(opt):
                                  (key, val) = reKeyVal.split(opt,1)
                                  # we only consider the first '=' for splitting
                                  # we do this to support passing params like
                                  # mapred.child.java.opts=-Djava.library.path=some_dir
                                  # Even in case of an invalid error like unescaped '=',
                                  # we don't want to fail here itself. We leave such errors 
                                  # to be caught during validation which happens after this
                                  dictOptsKeyVals[key] = val
                                else: 
                                  # this means an invalid option. Leaving it
                                  #for config.verify to catch
                                  dictOptsKeyVals[opt] = None
                                
                           cmdLineOpts = reKeyValList.split(self._options[section][option])

                           for opt in cmdLineOpts:
                              if reKeyVal.search(opt):
                                # Same as for hodrc options. only consider
                                # the first =
                                ( key, val ) = reKeyVal.split(opt,1)
                              else:
                                key = opt
                                val = None
                              # whatever is given at cmdline overrides
                              # what is given in hodrc only for non-final params
                              if dictOptsKeyVals.has_key(key):
                                if overwrite:
                                  dictOptsKeyVals[key] = val
                              else: dictOptsKeyVals[key] = val
                              
                           self._dict[section][option] = ""
                           for key in dictOptsKeyVals:
                              if self._dict[section][option] == "":
                                if dictOptsKeyVals[key]:
                                  self._dict[section][option] = key + "=" + \
                                    dictOptsKeyVals[key]
                                else: #invalid option. let config.verify catch
                                  self._dict[section][option] = key
                              else:
                                if dictOptsKeyVals[key]:
                                  self._dict[section][option] = \
                                    self._dict[section][option] + "," + key + \
                                      "=" + dictOptsKeyVals[key]
                                else:  #invalid option. let config.verify catch
                                  self._dict[section][option] = \
                                    self._dict[section][option] + "," + key

                        else:
                             # for rest of the options, that don't need
                            # appending business.
                            # options = cmdline opts + defaults
                            # dict    = hodrc opts only
                            # only non default opts can overwrite any opt
                            # currently in dict
                           if not self._dict[section].has_key(option):
                              # options not mentioned in hodrc
                              self._dict[section][option] = \
                                               self._options[section][option]
                           elif self._configDef[section][option]['default'] != \
                                               self._options[section][option]:
                              # option mentioned in hodrc but user has given a
                              # non-default option
                              self._dict[section][option] = \
                                               self._options[section][option]

    ## UNUSED METHOD
    ## zim: is too :)
    def __check_perms(self):
        perms = None
        if self._options:  
            try:
                perms = get_perms(self.configFile)
            except OSError, data:
                self._options.print_help()
                raise Exception("*** could not find config file: %s" % data)
                sys.exit(1)
        else:
            perms = get_perms(self.configFile)
               
        if perms != requiredPerms:
            error = "*** '%s' has invalid permission: %s should be %s\n" % \
                (self.configFile, perms, requiredPerms)
            raise Exception( error)
            sys.exit(1)

    def replace_escape_seqs(self):
      """ replace any escaped characters """
      replace_escapes(self)

class formatter(IndentedHelpFormatter):
    def format_option_strings(self, option):
        """Return a comma-separated list of option strings & metavariables."""
        if option.takes_value():
            metavar = option.metavar or option.dest.upper()
            short_opts = [sopt
                          for sopt in option._short_opts]
            long_opts = [self._long_opt_fmt % (lopt, metavar)
                         for lopt in option._long_opts]
        else:
            short_opts = option._short_opts
            long_opts = option._long_opts

        if self.short_first:
            opts = short_opts + long_opts
        else:
            opts = long_opts + short_opts

        return ", ".join(opts)    

class options(OptionParser, baseConfig):

    def __init__(self, optionDef, usage, version, originalDir=None, 
                 withConfig=False, defaultConfig=None, defaultLocation=None,
                 name=None):
        """Constructs and options object.
         
           optionDef     - definition object
           usage         - usage statement
           version       - version string
           withConfig    - used in conjunction with a configuration file
           defaultConfig - default configuration file
        
        """
        OptionParser.__init__(self, usage=usage)
        baseConfig.__init__(self, optionDef, originalDir)
        
        self.formatter = formatter(4, max_help_position=100, width=180, 
                                   short_first=1)
        
        self.__name = name
        self.__version = version
        self.__withConfig = withConfig
        self.__defaultConfig = defaultConfig
        self.__defaultLoc = defaultLocation
        self.args = []
        self.__optionList = []
        self.__compoundOpts = []
        self.__shortMap = {}
        self.__alphaString = 'abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ1234567890'
        self.__alpha = []
        self.__parsedOptions = {}
        self.__reserved = [ 'h' ]
        
        self.__orig_grps = []
        self.__orig_grp_lists = {}
        self.__orig_option_list = []
        
        self.__display_grps = []
        self.__display_grp_lists = {}
        self.__display_option_list = [] 
        
        self.config = None
        
        if self.__withConfig:
            self.__reserved.append('c')
        self.__reserved.append('v')
        
        self.__gen_alpha()            

        # build self.__optionList, so it contains all the options that are
        # possible. the list elements are of the form section.option
        for section in self._mySections:
            if self.__withConfig and section == 'config':
                raise Exception(
                    "withConfig set 'config' cannot be used as a section name")
            for option in self._configDef[section].keys():
                if '.' in option:
                    raise Exception("Options cannot contain: '.'")
                elif self.__withConfig and option == 'config':
                    raise Exception(
                        "With config set, option config is not allowed.")
                elif self.__withConfig and option == 'verbose-help':
                    raise Exception(
                        "With config set, option verbose-help is not allowed.")                 
                self.__optionList.append(self.__splice_compound(section, 
                                                                option))
        self.__build_short_map()
        self.__add_options()
        self.__init_display_options() 
        
        (self.__parsedOptions, self.args) = self.parse_args()

        # Now process the positional arguments only for the client side
        if self.__name == 'hod':

          hodhelp = hodHelp()

          _operation = getattr(self.__parsedOptions,'hod.operation')
          _script = getattr(self.__parsedOptions, 'hod.script')
          nArgs = self.args.__len__()
          if _operation:
            # -o option is given
            if nArgs != 0:
              self.error('invalid syntax : command and operation(-o) cannot coexist')
          elif nArgs == 0 and _script:
            # for a script option, without subcommand: hod -s script ...
            pass
          elif nArgs == 0:
            print "Usage: ",hodhelp.help()
            sys.exit(0)
          else:
            # subcommand is given
            cmdstr = self.args[0] # the subcommand itself
            cmdlist = hodhelp.ops
            if cmdstr not in cmdlist:
              print "Usage: ", hodhelp.help()
              sys.exit(2)

            numNodes = None
            clusterDir = None
            # Check which subcommand. cmdstr  = subcommand itself now.
            if cmdstr == "allocate":
              clusterDir = getattr(self.__parsedOptions, 'hod.clusterdir')
              numNodes = getattr(self.__parsedOptions, 'hod.nodecount')
 
              if not clusterDir or not numNodes:
                print hodhelp.usage(cmdstr)
                sys.exit(3)

              cmdstr = cmdstr + ' ' + clusterDir + ' ' + numNodes

              setattr(self.__parsedOptions,'hod.operation', cmdstr)
 
            elif cmdstr == "deallocate" or cmdstr == "info":
              clusterDir = getattr(self.__parsedOptions, 'hod.clusterdir')

              if not clusterDir:
                print hodhelp.usage(cmdstr)
                sys.exit(3)
 
              cmdstr = cmdstr + ' ' + clusterDir
              setattr(self.__parsedOptions,'hod.operation', cmdstr)

            elif cmdstr == "list":
              setattr(self.__parsedOptions,'hod.operation', cmdstr)
              pass
 
            elif cmdstr == "script":
              clusterDir = getattr(self.__parsedOptions, 'hod.clusterdir')
              numNodes = getattr(self.__parsedOptions, 'hod.nodecount')
              originalDir = getattr(self.__parsedOptions, 'hod.original-dir')

              if originalDir and clusterDir:
                self.remove_exit_code_file(originalDir, clusterDir)

              if not _script or not clusterDir or not numNodes:
                print hodhelp.usage(cmdstr)
                sys.exit(3)
              pass

            elif cmdstr == "help":
              if nArgs == 1:
                self.print_help()
                sys.exit(0)
              elif nArgs != 2:
                self.print_help()
                sys.exit(3)
              elif self.args[1] == 'options':
                self.print_options()
                sys.exit(0)
              cmdstr = cmdstr + ' ' + self.args[1]
              setattr(self.__parsedOptions,'hod.operation', cmdstr)

        # end of processing for arguments on the client side

        if self.__withConfig:
            self.config = self.__parsedOptions.config
            if not self.config:
                self.error("configuration file must be specified")
            if not os.path.isabs(self.config):
                # A relative path. Append the original directory which would be the
                # current directory at the time of launch
                try:  
                    origDir = getattr(self.__parsedOptions, 'hod.original-dir')
                    if origDir is not None:
                        self.config = os.path.join(origDir, self.config)
                        self.__parsedOptions.config = self.config
                except AttributeError, e:
                    self.error("hod.original-dir is not defined.\
                                   Cannot get current directory")
            if not os.path.exists(self.config):
                if self.__defaultLoc and not re.search("/", self.config):
                    self.__parsedOptions.config = os.path.join(
                        self.__defaultLoc, self.config)
        self.__build_dict()   

    def norm_cluster_dir(self, orig_dir, directory):
        directory = os.path.expanduser(directory)
        if not os.path.isabs(directory):
            directory = os.path.join(orig_dir, directory)
        directory = os.path.abspath(directory)

        return directory

    def remove_exit_code_file(self, orig_dir, dir):
        try:
            dir = self.norm_cluster_dir(orig_dir, dir)
            if os.path.exists(dir):
                exit_code_file = os.path.join(dir, "script.exitcode")
                if os.path.exists(exit_code_file):
                    os.remove(exit_code_file)
        except:
            print >>sys.stderr, "Could not remove the script.exitcode file."
    
    def __init_display_options(self):
        self.__orig_option_list = self.option_list[:]
        optionListTitleMap = {}
        for option in self.option_list:
            optionListTitleMap[option._long_opts[0]] = option
      
        self.__orig_grps = self.option_groups[:]
        for group in self.option_groups:
            self.__orig_grp_lists[group.title] = group.option_list[:]
                                    
        groupTitleMap = {}
        optionTitleMap = {}
        for group in self.option_groups:
            groupTitleMap[group.title] = group
            optionTitleMap[group.title] = {}
            for option in group.option_list:
                (sectionName, optionName) = \
                    self.__split_compound(option._long_opts[0])
                optionTitleMap[group.title][optionName] = option
          
        for section in self._mySections:
            for option in self._configDef[section]:
                if self._configDef[section][option]['help']:
                    if groupTitleMap.has_key(section):
                        if not self.__display_grp_lists.has_key(section):
                            self.__display_grp_lists[section] = []
                        self.__display_grp_lists[section].append(
                            optionTitleMap[section][option])
                    
                    try:    
                        self.__display_option_list.append(
                            optionListTitleMap["--" + self.__splice_compound(
                            section, option)])
                    except KeyError:
                        pass
        try:
            self.__display_option_list.append(optionListTitleMap['--config'])
        except KeyError:
            pass
          
        self.__display_option_list.append(optionListTitleMap['--help'])
        self.__display_option_list.append(optionListTitleMap['--verbose-help'])
        self.__display_option_list.append(optionListTitleMap['--version'])
                    
        self.__display_grps = self.option_groups[:]             
        for section in self._mySections:
            if self.__display_grp_lists.has_key(section):
                self.__orig_grp_lists[section] = \
                    groupTitleMap[section].option_list
            else:
                try:
                    self.__display_grps.remove(groupTitleMap[section])
                except KeyError:
                    pass
                
    def __gen_alpha(self):
        assignedOptions = []
        for section in self._configDef:
            for option in self._configDef[section]:
                if self._configDef[section][option]['short']:
                    assignedOptions.append(
                        self._configDef[section][option]['short'])
        
        for symbol in self.__alphaString:
            if not symbol in assignedOptions:
                self.__alpha.append(symbol)

    def __splice_compound(self, section, option):
        return "%s.%s" % (section, option)
        
    def __split_compound(self, compound):    
        return compound.split('.')
        
    def __build_short_map(self):
        """ build a short_map of parametername : short_option. This is done
        only for those parameters that don't have short options already
        defined in configDef.
        If possible, the first letter in the option that is not already
        used/reserved as a short option is allotted. Otherwise the first
        letter in __alpha that isn't still used is allotted.
        e.g. { 'hodring.java-home': 'T', 'resource_manager.batch-home': 'B' }
        """

        optionsKey = {}
        for compound in self.__optionList:
            (section, option) = self.__split_compound(compound)
            if not optionsKey.has_key(section):
                optionsKey[section] = []
            optionsKey[section].append(option)
        
        for section in self._configDef.sections():
            options = optionsKey[section]
            options.sort()
            for option in options:
                if not self._configDef[section][option]['short']:
                    compound = self.__splice_compound(section, option)
                    shortOptions = self.__shortMap.values()
                    for i in range(0, len(option)):
                        letter = option[i]
                        letter = letter.lower()
                        if letter in self.__alpha:
                            if not letter in shortOptions and \
                                not letter in self.__reserved:
                                self.__shortMap[compound] = letter
                                break
                    if not self.__shortMap.has_key(compound):
                        for i in range(0, len(self.__alpha)):
                            letter = self.__alpha[i]
                            if not letter in shortOptions and \
                                not letter in self.__reserved:
                                self.__shortMap[compound] = letter

    def __add_option(self, config, compoundOpt, section, option, group=None):
        addMethod = self.add_option
        if group: addMethod=group.add_option
        
        self.__compoundOpts.append(compoundOpt)
        
        if compoundOpt == 'gridservice-mapred.final-server-params' or \
           compoundOpt == 'gridservice-hdfs.final-server-params' or \
           compoundOpt == 'gridservice-mapred.server-params' or \
           compoundOpt == 'gridservice-hdfs.server-params' or \
           compoundOpt == 'hod.client-params':
          _action = 'append'
        elif config[section][option]['type'] == 'bool':
          _action = 'store_true'
        else:
          _action = 'store'

        if self.__shortMap.has_key(compoundOpt):
          addMethod("-" + self.__shortMap[compoundOpt],
                          "--" + compoundOpt, dest=compoundOpt, 
                          action= _action, 
                          metavar=config[section][option]['type'],
                          default=config[section][option]['default'],
                          help=config[section][option]['desc'])
        else:
          if config[section][option]['short']:
            addMethod("-" + config[section][option]['short'], 
                              "--" + compoundOpt, dest=compoundOpt, 
                              action= _action,
                              metavar=config[section][option]['type'],
                              default=config[section][option]['default'],
                              help=config[section][option]['desc'])   
          else:
            addMethod('', "--" + compoundOpt, dest=compoundOpt, 
                              action= _action, 
                              metavar=config[section][option]['type'],
                              default=config[section][option]['default'],
                              help=config[section][option]['desc'])   
                           
    def __add_options(self):
        if self.__withConfig:
            self.add_option("-c", "--config", dest='config', 
                action='store', default=self.__defaultConfig, 
                metavar='config_file',
                help="Full path to configuration file.")

        self.add_option("", "--verbose-help", 
            action='help', default=None, 
            metavar='flag',
            help="Display verbose help information.")
        
        self.add_option("-v", "--version", 
            action='version', default=None, 
            metavar='flag',
            help="Display version information.")
        
        self.version = self.__version
  
        if len(self._mySections) > 1:
            for section in self._mySections:
                group = OptionGroup(self, section)
                for option in self._configDef[section]:
                    compoundOpt = self.__splice_compound(section, option)
                    self.__add_option(self._configDef, compoundOpt, section, 
                                      option, group)
                self.add_option_group(group)
        else:
            for section in self._mySections:
                for option in self._configDef[section]:
                    compoundOpt = self.__splice_compound(section, option)
                    self.__add_option(self._configDef, compoundOpt, section, 
                                      option)
                    
    def __build_dict(self):
        if self.__withConfig:
            self._dict['config'] = str(getattr(self.__parsedOptions, 'config'))
        for compoundOption in dir(self.__parsedOptions):
            if compoundOption in self.__compoundOpts:
                (section, option) = self.__split_compound(compoundOption)
                if not self._dict.has_key(section):
                    self._dict[section] = {}
                
                if getattr(self.__parsedOptions, compoundOption):
                    _attr = getattr(self.__parsedOptions, compoundOption)
                    # when we have multi-valued parameters passed separately
                    # from command line, python optparser pushes them into a
                    # list. So converting all such lists to strings
                    if type(_attr) == type([]):
                      import string
                      _attr = string.join(_attr,',')
                    self._dict[section][option] = _attr
                    
        for section in self._configDef:
            for option in self._configDef[section]: 
                if self._configDef[section][option]['type'] == 'bool':
                    compoundOption = self.__splice_compound(section, option)
                    if not self._dict.has_key(section):
                        self._dict[section] = {}
                    
                    if option not in self._dict[section]:
                        self._dict[section][option] = False
 
    def __set_display_groups(self):
        if not '--verbose-help' in sys.argv:
            self.option_groups = self.__display_grps
            self.option_list = self.__display_option_list
            for group in self.option_groups:
                group.option_list = self.__display_grp_lists[group.title]
 
    def __unset_display_groups(self):
        if not '--verbose-help' in sys.argv:
            self.option_groups = self.__orig_grps
            self.option_list = self.__orig_option_list
            for group in self.option_groups:
                group.option_list = self.__orig_grp_lists[group.title]      
 
    def print_help(self, file=None):
        self.__set_display_groups()
        OptionParser.print_help(self, file)
        self.__unset_display_groups()

    def print_options(self):
        _usage = self.usage
        self.set_usage('')
        self.print_help()
        self.set_usage(_usage)
                        
    def verify(self):
        return baseConfig.verify(self)

    def replace_escape_seqs(self):
      replace_escapes(self)
