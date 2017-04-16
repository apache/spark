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
# $Id:types.py 6172 2007-05-22 20:26:54Z zim $
#
#------------------------------------------------------------------------------

""" Higher level data types and type related classes.

    Supported Types (Verification and Display):

      address        - validates ip:port and host:port tcp addresses
      ip_address     - validates and IP address
      net_address    - validates an IP like address, ie netmask
      hostname       - validates a hostname with DNS
      eaddress       - validates a single email address or a comma
                       seperated list of email addresses
      http_version   - validates a value is a http version (1.0/1.1)
      tcp_port       - validates a value to be a valid tcp port (2-65535)
      bool           - validates value is (0, 1, true, false) / converts
                       true -> 1 and false -> 0
      directory      - validates a values is a directory / resolves path to
                       absolute path
      file           - validates a value is a file / resolves path to absolute
                       path
      float          - validates a value is a float, converts string to float
      pos_float      - validates a value is a float and >= 0, converts string
                       to float
      pos_num        - same as pos_float
      neg_float      - validates a value is a float and < 0, converts string to
                       float
      int            - validates a value is an integer, converts string to
                       integer
      pos_int        - validates a value is an integer and >= 0, converts
                       string to integer
      neg_int        - validates a values is an integer and < 0, converts
                       striing to integer
      freq           - frequency, positive integer
      size           - validates a size in bytes, kb, mb, kb, and tb
                       (int > 0 post fixed with K, M, G, or T) also converts
                       value to integer bytes
      range          - numeric range, x-y normalized to a tuple, if a single
                       number is supplie a single element tuple is returned
      timestamp      - utc timestamp of the form YYYYMMDDHHMMSS
      user_account   - UNIX user account name
      user_group     - UNIX group name
      string         - arbitrarily long string
      list           - comma seperated list of strings of arbitrary length,
      keyval         - comma seperated list of key=value pairs, key does not 
                       need to be unique.
      uri            - a uri """

import sys, os, socket, pwd, grp, stat, re, re, string, pprint, urlparse

from tcp import tcpSocket, check_net_address, check_ip_address
from util import check_timestamp

types = { 'directory'      : { 'db'    : 'string',
                               'units' : None     },

          'address'        : { 'db'    : 'string',
                               'units' : None     },

          'ip_address'     : { 'db'    : 'string',
                               'units' : None     },

          'net_address'    : { 'db'    : 'string',
                               'units' : None     },

          'bool'           : { 'db'    : 'bool',
                               'units' : None     },

          'int'            : { 'db'    : 'integer',
                               'units' : None     },

          'float'          : { 'db'    : 'float',
                               'units' : None     },

          'pos_int'        : { 'db'    : 'integer',
                               'units' : None     },

          'neg_int'        : { 'db'    : 'integer',
                               'units' : None     },

          'pos_num'        : { 'db'    : 'float',
                               'units' : None     },

          'pos_float'      : { 'db'    : 'float',
                               'units' : None     },

          'neg_float'      : { 'db'    : 'float',
                               'units' : None     },

          'string'         : { 'db'    : 'string',
                               'units' : None     },

          'list'           : { 'db'    : 'string',
                               'units' : None     },

          'file'           : { 'db'    : 'string',
                               'units' : None     },

          'size'           : { 'db'    : 'integer',
                               'units' : 'bytes'  },

          'freq'           : { 'db'    : 'integer',
                               'units' : 'hz'     },

          'eaddress'       : { 'db'    : 'string',
                               'units' : None     },

          'tcp_port'       : { 'db'    : 'integer',
                               'units' : None     },

          'http_version'   : { 'db'    : 'float',
                               'units' : None     },

          'range'          : { 'db'    : 'string',
                               'units' : None     },

          'hostname'       : { 'db'    : 'string',
                               'units' : None     },

          'user_account'   : { 'db'    : 'string',
                               'units' : None     },

          'user_group'     : { 'db'    : 'string',
                               'units' : None     },

          'timestamp'      : { 'db'    : 'timestamp',
                               'units' : None     },

          'keyval'         : { 'db'    : 'string',
                               'units' : None     },
          
          'uri'            : { 'db'    : 'string',
                               'units' : None     },

          ''               : { 'db'    : 'string',
                               'units' : None     }}

dbTypes = { 'string'  :   { 'type'  : 'varchar',
                            'store' : 'type_strings_0',
                            'table' : True              },

            'integer' :   { 'type'  : 'bigint',
                            'store' : 'integers',
                            'table' : False             },

            'float' :     { 'type'  : 'real',
                            'store' : 'floats',
                            'table' : False             },

            'bool' :      { 'type'  : 'boolean',
                            'store' : 'bools',
                            'table' : False             },

            'timestamp' : { 'type'  : 'timestamp(0)',
                            'store' : 'timestamps',
                            'table' : False             }}

reSizeFormat = re.compile("^(\d+)(k|m|g|t|p|kb|mb|gb|tb|pb)$", flags=2)
reDash = re.compile("\s*-\s*")

sizeFactors = { 'b'     : 1,
                'bytes' : 1,
                'k'     : 1024,
                'kb'    : 1024,
                'm'     : 1048576,
                'mb'    : 1048576,
                'g'     : 1073741824,
                'gb'    : 1073741824,
                't'     : 1099511627776,
                'tb'    : 1099511627776,
                'p'     : 1125899906842624,
                'pb'    : 1125899906842624 }

freqFactors = { 'hz'  : 1,
                'khz' : 1000,
                'mhz' : 1000000,
                'ghz' : 1000000000,
                'thz' : 1000000000000,
                'phz' : 1000000000000000 }

sizeMap = [ { 'factor' : sizeFactors['b'],
              'long'   : 'byte',
              'short'  : 'byte'           },

            { 'factor' : sizeFactors['k'],
              'long'   : 'Kilobyte',
              'short'  : 'KB'             },

            { 'factor' : sizeFactors['m'],
              'long'   : 'Megabyte',
              'short'  : 'MB'             },

            { 'factor' : sizeFactors['g'],
              'long'   : 'Gigabyte',
              'short'  : 'GB'             },

            { 'factor' : sizeFactors['t'],
              'long'   : 'Terabyte',
              'short'  : 'TB'             },

            { 'factor' : sizeFactors['p'],
              'long'   : 'Petabyte',
              'short'  : 'PB'             } ]

freqMap = [ { 'factor' : freqFactors['hz'],
              'long'   : 'Hertz',
              'short'  : 'Hz'               },

            { 'factor' : freqFactors['khz'],
              'long'   : 'Kilohertz',
              'short'  : 'KHz'              },

            { 'factor' : freqFactors['mhz'],
              'long'   : 'Megahertz',
              'short'  : 'MHz'              },

            { 'factor' : freqFactors['ghz'],
              'long'   : 'Gigahertz',
              'short'  : 'GHz'              },

            { 'factor' : freqFactors['thz'],
              'long'   : 'Terahertz',
              'short'  : 'THz'              },

            { 'factor' : freqFactors['phz'],
              'long'   : 'Petahertz',
              'short'  : 'PHz'              } ]

reListString = r"(?<!\\),"
reList = re.compile(reListString)

reKeyVal = r"(?<!\\)="
reKeyVal = re.compile(reKeyVal)

class typeToString:
    """Provides method for converting normalized types to strings."""
    def __init__(self):
        self.toStringFunctions = {}
        self.__build_to_string_functions()
 
    def __call__(self, type, value):
        return self.toStringFunctions[type](value) 
 
    def __build_to_string_functions(self):
        functions = {}
        for function in dir(self):
            functions[function] = 1

        for type in types.keys():
            # kinda bad, need to find out how to know the name of the class
            #  I'm in.  But it works.
            functionName = "_typeToString__tostring_%s" % type
            if functions.has_key(functionName):
                self.toStringFunctions[type] = getattr(self, functionName)
            else:
                if type == '':
                    self.toStringFunctions[type] = self.__tostring_nothing
                else:
                    error = "To string function %s for type %s does not exist." \
                        % (functionName, type)
                    raise Exception(error)
                    sys.exit(1)        

    def __tostring(self, value):
        return str(value)

    def __tostring_directory(self, value):
        return self.__tostring(value)

    def __tostring_address(self, value):
        return "%s:%s" % (value[0], value[1])

    def __tostring_ip_address(self, value):
        return self.__tostring(value)

    def __tostring_net_address(self, value):
        return self.__tostring(value)

    def __tostring_bool(self, value):
        if value == False:
            return 'false'
        elif value == True:
            return 'true'
        else:
            return str(value)

    def __tostring_int(self, value):
        return self.__tostring(value)

    def __tostring_float(self, value):
        return self.__tostring(value)

    def __tostring_pos_int(self, value):
        return self.__tostring(value)

    def __tostring_neg_int(self, value):
        return self.__tostring(value)     

    def __tostring_freq(self, value):
        return self.__tostring(value)

    def __tostring_pos_float(self, value):
        return self.__tostring(value)

    def __tostring_pos_num(self, value):
        return self.__tostring(value)

    def __tostring_neg_float(self, value):
        return self.__tostring(value)

    def __tostring_string(self, value):
        return value

    def __tostring_keyval(self, value):
        string = '"' # to protect from shell escapes
        for key in value:
          # for item in value[key]:
          #      string = "%s%s=%s," % (string, key, item)
          # Quotes still cannot protect Double-slashes.
          # Dealing with them separately
          val = re.sub(r"\\\\",r"\\\\\\\\",value[key])

          string = "%s%s=%s," % (string, key, val)

        return string[:-1] + '"'

    def __tostring_list(self, value):
        string = ''
        for item in value:
            string = "%s%s," % (string, item)
            
        return string[:-1]

    def __tostring_file(self, value):
        return self.__tostring(value)
      
    def __tostring_size(self, value):
        return self.__tostring(value)
        
    def __tostring_eaddress(self, value):
        return self.__tostring(value)

    def __tostring_tcp_port(self, value):
        return self.__tostring(value)

    def __tostring_http_version(self, value):
        return self.__tostring(value)

    def __tostring_range(self, value):
        if len(value) < 2:
          return value[0]
        else:
          return "%s-%s" % (value[0], value[1])

    def __tostring_timestamp(self, value):
        return self.__tostring(value)

    def __tostring_hostname(self, value):
        return self.__tostring(value)

    def __tostring_user_account(self, value):
        return self.__tostring(value)

    def __tostring_user_group(self, value):
        return self.__tostring(value)

    def __tostring_uri(self, value):
        return self.__tostring(value)

    def __tostring_nothing(self, value):
        return value

class typeValidator:
    """Type validation class used to normalize values or validated 
       single/large sets of values by type."""

    def __init__(self, originalDir=None):
        self.verifyFunctions = {}
        self.__build_verify_functions()

        self.validateList = []
        self.validatedInfo = []
        self.__originalDir = originalDir

    def __getattr__(self, attrname):
        """validateList  = [ { 'func' : <bound method configValidator>,
                               'name' : 'SA_COMMON.old_xml_dir',
                               'value': 'var/data/old'                 },

                             { 'func' : <bound method configValidator>,
                               'name' : 'SA_COMMON.log_level',
                               'value': '4'                            } ]

           validatedInfo = [ { # name supplied to add()
                               'name'       : 'SA_COMMON.tmp_xml_dir',

                               # is valid or not
                               'isValid'    : 1

                               # normalized value
                               'normalized' : /var/data/tmp,

                               # error string ?
                               'errorData'  : 0                        },

                             { 'name'       : 'SA_COMMON.new_xml_dir',
                               'isValid'    : 1
                               'normalized' : /var/data/new,
                               'errorData'  : 0                        } ]"""

        if attrname   == "validateList":
            return self.validateList   # list of items to be validated
        elif attrname == "validatedInfo":
            return self.validatedInfo  # list of validation results
        else: raise AttributeError, attrname

    def __build_verify_functions(self):
        functions = {}
        for function in dir(self):
            functions[function] = 1

        for type in types.keys():
            # kinda bad, need to find out how to know the name of the class
            #  I'm in.  But it works.
            functionName = "_typeValidator__verify_%s" % type
            if functions.has_key(functionName):
                self.verifyFunctions[type] = getattr(self, functionName)
            else:
                if type == '':
                    self.verifyFunctions[type] = self.__verify_nothing
                else:
                    error = "Verify function %s for type %s does not exist." \
                        % (functionName, type)
                    raise Exception(error)
                    sys.exit(1)

    def __get_value_info(self):
        valueInfo = { 'isValid' : 0, 'normalized' : 0, 'errorData' : 0 }

        return valueInfo

    def __set_value_info(self, valueInfo, **valueData):
        try:
            valueInfo['normalized'] = valueData['normalized']
            valueInfo['isValid'] = 1
        except KeyError:
            valueInfo['isValid'] = 0
            try:
                valueInfo['errorData'] = valueData['errorData']
            except:
                pass

    # start of 'private' verification methods, each one should correspond to a
    #   type string (see self.verify_config())
    def __verify_directory(self, type, value):
        valueInfo = self.__get_value_info()

        if os.path.isdir(value):
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_directory(self, value):
        return self.__normalizedPath(value)

    def __verify_address(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            socket = tcpSocket(value)
            if socket.verify():
                self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
            else:
                self.__set_value_info(valueInfo)
        except:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_address(self, value):
        return value.split(':')

    def __verify_ip_address(self, type, value):
        valueInfo = self.__get_value_info()

        if check_ip_address(value):
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo

    def __verify_net_address(self, type, value):
        valueInfo = self.__get_value_info()

        if check_net_address(value):
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo

    def __verify_bool(self, type, value):
        valueInfo = self.__get_value_info()

        value = str(value)
        if re.match("^false|0|f|no$", value, 2):
            self.__set_value_info(valueInfo, normalized=False)
        elif re.match("^true|1|t|yes$", value, 2):
            self.__set_value_info(valueInfo, normalized=True)
        else:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_bool(self, value):
        value = str(value)
        norm = ""
        if re.match("^false|0|f|no$", value, 2):
            norm = False
        elif re.match("^true|1|t|yes$", value, 2):
            norm = True
        else:
            raise Exception("invalid bool specified: %s" % value)
            
        return norm

    def __verify_int(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        except:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_int(self, value):
        return int(value)

    def __verify_float(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        except:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_float(self, value):
        return float(value)

    def __verify_pos_int(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            value = self.normalize(type, value)
        except:
            self.__set_value_info(valueInfo)
        else:
            self.__set_value_info(valueInfo, normalized=value)

        return valueInfo
      
    def __norm_pos_int(self, value):
        value = int(value)
        if value < 0:
            raise Exception("value is not positive: %s" % value)
        
        return value

    def __verify_neg_int(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            value = self.normalize(type, value)
        except:
            self.__set_value_info(valueInfo)
        else:
            self.__set_value_info(valueInfo, normalized=value)

        return valueInfo
      
    def __norm_neg_int(self, type, value):
        value = int(value)
        if value > 0:
            raise Exception("value is not negative: %s" % value)
        
        return value        

    def __verify_freq(self, type, value):
        return self.__verify_pos_int(type, value)

    def __norm_freq(self, value):
        return self.__norm_pos_int(value)

    def __verify_pos_float(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            value = self.normalize(type, value)
        except:
            self.__set_value_info(valueInfo)
        else:
            self.__set_value_info(valueInfo, normalized=value)

        return valueInfo

    def __norm_pos_float(self, value):
        value = float(value)
        if value < 0:
            raise Exception("value is not positive: %s" % value)
        
        return value

    def __verify_pos_num(self, type, value):
        return self.__verify_pos_float(value)
      
    def __norm_pos_num(self, value):
        return self.__norm_pos_float(value)

    def __verify_neg_float(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            value = self.normalize(type, value)
        except:
            self.__set_value_info(valueInfo)
        else:
            self.__set_value_info(valueInfo, normalized=value)

        return valueInfo

    def __norm_neg_float(self, value):
        value = float(value)
        if value >= 0:
            raise Exception("value is not negative: %s" % value)
        
        return value

    def __verify_string(self, type, value):
        valueInfo = self.__get_value_info()
        self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                   value))
        
        return valueInfo
      
    def __norm_string(self, value):
        return str(value)

    def __verify_keyval(self, type, value):
        valueInfo = self.__get_value_info()

        if reKeyVal.search(value):
          try:
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                value))
          except:
            self.__set_value_info(valueInfo, errorData = \
              "invalid list of key-value pairs : [ %s ]" % value)
        else:
            msg = "No key value pairs found?"
            self.__set_value_info(valueInfo, errorData=msg)

        return valueInfo
      
    def __norm_keyval(self, value):
        list = self.__norm_list(value)
        keyValue = {}
        for item in list:
            (key, value) = reKeyVal.split(item)
            #if not keyValue.has_key(key):
            #    keyValue[key] = []
            #keyValue[key].append(value)
            keyValue[key] = value
        return keyValue     

    def __verify_list(self, type, value):
        valueInfo = self.__get_value_info()

        self.__set_value_info(valueInfo, normalized=self.normalize(type,value))

        return valueInfo
      
    def __norm_list(self, value):
        norm = []
        if reList.search(value):
            norm = reList.split(value)
        else:
            norm = [value,]
            
        return norm

    def __verify_file(self, type, value):
        valueInfo = self.__get_value_info()

        if os.path.isfile(value):
            self.__set_value_info(valueInfo, normalized=self.normalize(type,
                                                                       value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_file(self, value):
        return self.__normalizedPath(value)

    def __verify_size(self, type, value):
        valueInfo = self.__get_value_info()

        value = str(value)
        if reSizeFormat.match(value):
            numberPart = int(reSizeFormat.sub("\g<1>", value))
            factorPart = reSizeFormat.sub("\g<2>", value)
            try:
                normalized = normalize_size(numberPart, factorPart)
                self.__set_value_info(valueInfo,
                    normalized=normalized)
            except:
                self.__set_value_info(valueInfo)
        else:
            try:
                value = int(value)
            except:
                self.__set_value_info(valueInfo)
            else:
                if value >= 0:
                    self.__set_value_info(valueInfo, normalized=value)
                else:
                    self.__set_value_info(valueInfo)

        return valueInfo

    def __norm_size(self, file):
        norm = None
        if reSizeFormat.match(value):
            numberPart = int(reSizeFormat.sub("\g<1>", value))
            factorPart = reSizeFormat.sub("\g<2>", value)
            norm = normalize_size(numberPart, factorPart)            
        else:
            norm = int(value)
            
        return norm
        
        
    def __verify_eaddress(self, type, value):
        valueInfo = self.__get_value_info()

        emailList = reComma.split(value)

        for emailAddress in emailList:
            if reEmailAddress.match(emailAddress):
                emailParts = reEmailDelimit.split(emailAddress)
                try:
                    socket.gethostbyname(emailParts[1])
                    self.__set_value_info(valueInfo, normalized=self.normalize(
                                          type, value))
                except:
                    errorString = "%s is invalid (domain lookup failed)" % \
                        emailAddress
                    self.__set_value_info(valueInfo, errorData=errorString)
            else:
                errorString = "%s is invalid" % emailAddress
                self.__set_value_info(valueInfo, errorData=errorString)

        return valueInfo

    def __verify_tcp_port(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            value = self.__norm_tcp_port(value)
        except:
            self.__set_value_info(valueInfo)
        else:
            if value in range(2, 65536):
                self.__set_value_info(valueInfo, normalized=value)
            else:
                self.__set_value_info(valueInfo)

        return valueInfo
      
    def __norm_tcp_port(self, value):
        return int(value)

    def __verify_http_version(self, type, value):
        valueInfo = self.__get_value_info()

        if value in ('1.0', '1.1'):
            self.__set_value_info(valueInfo, normalized=float(value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo

    def __verify_range(self, type, value):
        valueInfo = self.__get_value_info()

        range = reDash.split(value)

        try:
            if len(range) > 1:
                start = int(range[0])
                end = int(range[1])
            else:
                start = int(range[0])
                end = None
        except:
            self.__set_value_info(valueInfo)
        else:
            if end:
                if end - start != 0:
                    self.__set_value_info(valueInfo, normalized=(start, end))
                else:
                    self.__set_value_info(valueInfo)
            else:
                self.__set_value_info(valueInfo, normalized=(start,))

        return valueInfo
      
    def __norm_range(self, value):
        range = reDash.split(value)
        if len(range) > 1:
            start = int(range[0])
            end = int(range[1])
        else:
            start = int(range[0])
            end = None   
            
        return (start, end)     

    def __verify_uri(self, type, value):
        valueInfo = self.__get_value_info()

        _norm = None
        try:
            uriComponents = urlparse.urlparse(value)
            if uriComponents[0] == '' or uriComponents[0] == 'file':
              # if scheme is '' or 'file'
              if not os.path.isfile(uriComponents[2]) and \
                                         not os.path.isdir(uriComponents[2]):
                  raise Exception("Invalid local URI")
              else:
                  self.__set_value_info(valueInfo, normalized=self.normalize(
                                                                  type,value))
            else:
              # other schemes
              # currently not checking anything. TODO
              self.__set_value_info(valueInfo, normalized=self.normalize(
                                                                   type,value))
        except:
            errorString = "%s is an invalid uri" % value
            self.__set_value_info(valueInfo, errorData=errorString)

        return valueInfo

    def __norm_uri(self, value):
       uriComponents = list(urlparse.urlparse(value))
       if uriComponents[0] == '':
          # if scheme is '''
          return self.__normalizedPath(uriComponents[2])
       elif uriComponents[0] == 'file':
          # if scheme is 'file'
          normalizedPath = self.__normalizedPath(uriComponents[2])
          return urlparse.urlunsplit(uriComponents[0:1] + [normalizedPath] + uriComponents[3:])

       # Not dealing with any other case right now
       return value

    def __verify_timestamp(self, type, value):
        valueInfo = self.__get_value_info()

        if check_timestamp(value):
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        else:
            self.__set_value_info(valueInfo)

        return valueInfo

    def __verify_hostname(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            socket.gethostbyname(value)
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))
        except:
            errorString = "%s is invalid (domain lookup failed)" % value
            self.__set_value_info(valueInfo, errorData=errorString)

        return valueInfo

    def __verify_user_account(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            pwd.getpwnam(value)
        except:
            errorString = "'%s' user account does not exist" % value
            self.__set_value_info(valueInfo, errorData=errorString)
        else:
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))

        return valueInfo

    def __verify_user_group(self, type, value):
        valueInfo = self.__get_value_info()

        try:
            grp.getgrnam(value)
        except:
            errorString = "'%s' group does not exist" % value
            self.__set_value_info(valueInfo, errorData=errorString)
        else:
            self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                       value))

        return valueInfo

    def __verify_nothing(self, type, value):
        valueInfo = self.__get_value_info()

        self.__set_value_info(valueInfo, normalized=self.normalize(type, 
                                                                   value))

        return valueInfo

    #--------------------------------------------------------------------------

    def normalize(self, type, value):
        try:
          normFunc = getattr(self, "_typeValidator__norm_%s" % type)
          return normFunc(value)
        except AttributeError, A:
          # this exception should occur only when we don't have corresponding normalize function
          return value

    def verify(self, type, value, allowNone=False):
        """Verifies a value based on its type.

           type      - supported configValidator type
           value     - data to be validated
           allowNone - don't freak out if None or '' is supplied

           returns a valueInfo dictionary:

            valueInfo = { 'isValid' : 1, 'normalized' : 5, 'errorData' : 0 }

           where:

            isValid    - true or false (0/1)
            normalized - the normalized value
            errorData  - if invalid an error string

           supported types:

            see top level"""

        result = None
        if allowNone:
            if value == '' or value == None:
                result = self.__verify_nothing(None, None)
                result['normalized'] = None
            else:
                result = self.verifyFunctions[type](type, value)
        else:
            result = self.verifyFunctions[type](type, value)

        return result

    def is_valid_type(self, type):
        """Returns true if type is valid."""

        return types.has_key(type)

    def type_info(self, type):
        """Returns type info dictionary."""

        dbInfo = dbTypes[types[type]['db']]
        typeInfo = types[type].copy()
        typeInfo['db'] = dbInfo

        return typeInfo

    def add(self, name, type, value):
        """Adds a value and type by name to the configValidate object to be
           verified using validate().

           name  - name used to key values and access the results of the
                   validation
           type  - configValidator type
           value - data to be verified"""

        self.validateList.append({ 'name' : name,
                                   'type' : type,
                                   'value': value })

    def validate(self, allowNone=False):
        """Validates configValidate object populating validatedInfo with
           valueInfo dictionaries for each value added to the object."""

        for valItem in self.validateList:
            valueInfo = self.verify(valItem['type'], valItem['value'],
                allowNone)
            if valueInfo:
                valueInfo['name'] = valItem['name']
                self.validatedInfo.append(valueInfo)
            else:
                raise Exception("\nMissing a return value: valueInfo\n%s" % \
                    self.verifyFunctions[valItem['type']](valItem['value']))

    def __normalizedPath(self, value):    
        oldWd = os.getcwd()
        if self.__originalDir:
          os.chdir(self.__originalDir)
        normPath = os.path.realpath(value)
        os.chdir(oldWd)
        return normPath


class display:
    def __init__(self):
        self.displayFunctions = {}
        self.__build_dispaly_functions()

    def __build_dispaly_functions(self):
        functions = {}
        for function in dir(self):
            functions[function] = 1

        for type in types.keys():
            # kinda bad, need to find out how to know the name of the class
            #  I'm in.  But it works.
            functionName = "_cisplay__display_%s" % type
            if functions.has_key(functionName):
                self.displayFunctions[type] = getattr(self, functionName)
            else:
                if type == '':
                    self.displayFunctions[type] = self.__display_default
                else:
                    error = "Display function %s for type %s does not exist." \
                        % (functionName, type)
                    raise Exception(error)
                    sys.exit(1)

    def __display_default(self, value, style):
        return value

    def __display_generic_number(self, value):
        displayNumber = ''
        splitNum = string.split(str(value), sep='.')
        numList = list(str(splitNum[0]))
        numList.reverse()
        length = len(numList)
        counter = 0
        for char in numList:
            counter = counter + 1
            if counter % 3 or counter == length:
                displayNumber = "%s%s" % (char, displayNumber)
            else:
                displayNumber = ",%s%s" % (char, displayNumber)

        if len(splitNum) > 1:
            displayNumber = "%s.%s" % (displayNumber, splitNum[1])

        return displayNumber

    def __display_generic_mappable(self, map, value, style, plural=True):
        displayValue = ''
        length = len(str(value))
        if length > 3:
            for factorSet in map:
                displayValue = float(value) / factorSet['factor']
                if len(str(int(displayValue))) <= 3 or \
                    factorSet['factor'] == map[-1]['factor']:
                    displayValue = "%10.2f" % displayValue    
                    if displayValue[-1] == '0':
                        if displayValue > 1 and style != 'short' and plural:
                            displayValue = "%s %ss" % (displayValue[:-1], 
                                                      factorSet[style])
                        else:
                            displayValue = "%s %s" % (displayValue[:-1], 
                                                      factorSet[style])
                    else:
                        if displayValue > 1 and style != 'short' and plural:
                            displayValue = "%s %ss" % (displayValue, 
                                                      factorSet[style])
                        else:
                            displayValue = "%s %s" % (displayValue, 
                                                      factorSet[style])
                    break

        return displayValue

    def __display_directory(self, value, style):
        return self.__display_default(value, style)

    def __display_address(self, value, style):
        return self.__display_default(value, style)

    def __display_ip_address(self, value, style):
        return self.__display_default(value, style)

    def __display_net_address(self, value, style):
        return self.__display_default(value, style)

    def __display_bool(self, value, style):
        displayValue = value
        
        if not isinstance(displayValue, bool):
            if re.match("^false|0|f|no$", value, 2):
                displayValue=False
            elif re.match("^true|1|t|yes$", value, 2):
                displayValue=True

        return displayValue

    def __display_int(self, value, style):
        return self.__display_generic_number(value)

    def __display_float(self, value, style):
        return self.__display_generic_number(value)

    def __display_pos_int(self, value, style):
        return self.__display_generic_number(value)

    def __display_neg_int(self, value, style):
        return self.__display_generic_number(value)

    def __display_pos_num(self, value, style):
        return self.__display_generic_number(value)

    def __display_pos_float(self, value, style):
        return self.__display_generic_number(value)

    def __display_neg_float(self, value, style):
        return self.__display_generic_number(value)

    def __display_string(self, value, style):
        return self.__display_default(value, style)

    def __display_list(self, value, style):
        value = value.rstrip()
        return value.rstrip(',')

    def __display_keyval(self, value, style):
        value = value.rstrip()
        return value.rstrip(',')

    def __display_file(self, value, style):
        return self.__display_default(value, style)

    def __display_size(self, value, style):
        return self.__display_generic_mappable(sizeMap, value, style)

    def __display_freq(self, value, style):
        return self.__display_generic_mappable(freqMap, value, style, False)

    def __display_eaddress(self, value, style):
        return self.__display_default(value, style)

    def __display_tcp_port(self, value, style):
        return self.__display_default(value, style)

    def __display_http_version(self, value, style):
        return self.__display_default(value, style)

    def __display_range(self, value, style):
        return self.__display_default(value, style)

    def __display_hostname(self, value, style):
        return self.__display_default(value, style)

    def __display_user_account(self, value, style):
        return self.__display_default(value, style)

    def __display_user_group(self, value, style):
        return self.__display_default(value, style)

    def __display_timestamp(self, value, style):
        return self.__display_default(value, style)

    def display(self, type, value, style='short'):
        displayValue = value
        if value != None:
            displayValue = self.displayFunctions[type](value, style)

        return displayValue

typeValidatorInstance = typeValidator()

def is_valid_type(type):
    """Returns true if type is valid."""

    return typeValidatorInstance.is_valid_type(type)

def type_info(type):
    """Returns type info dictionary."""

    return typeValidatorInstance.type_info(type)

def verify(type, value, allowNone=False):
    """Returns a normalized valueInfo dictionary."""

    return typeValidatorInstance.verify(type, value, allowNone)

def __normalize(map, val, factor):
    normFactor = string.lower(factor)
    normVal = float(val)
    return int(normVal * map[normFactor])

def normalize_size(size, factor):
    """ Normalize a size to bytes.

        size   - number of B, KB, MB, GB, TB, or PB
        factor - size factor (case insensitive):
                 b | bytes - bytes
                 k | kb    - kilobytes
                 m | mb    - megabytes
                 g | gb    - gigabytes
                 t | tb    - terabytes
                 p | pb    - petabytes
    """

    return __normalize(sizeFactors, size, factor)

def normalize_freq(freq, factor):
    """ Normalize a frequency to hertz.

        freq   - number of Hz, Khz, Mhz, Ghz, Thz, or Phz
        factor - size factor (case insensitive):
                 Hz  - Hertz
                 Mhz - Megahertz
                 Ghz - Gigahertz
                 Thz - Terahertz
                 Phz - Petahertz
    """

    return __normalize(freqFactors, freq, factor)
