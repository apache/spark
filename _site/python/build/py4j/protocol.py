'''
The protocol module defines the primitives and the escaping used by
Py4J protocol.

This is a text-based protocol that is efficient for general-purpose
method calling, but very inefficient with large numbers (because
they are text-based).

Binary protocol (e.g., protobuf) was considered in the past, but
internal benchmarking showed that it was less efficient in
terms of size and time. This is due to the fact that a lot
of small strings are exchanged (method name, class name, variable
names, etc.).

Created on Oct 14, 2010

:author: Barthelemy Dagenais
'''
from __future__ import unicode_literals, absolute_import

from base64 import standard_b64encode, standard_b64decode

from decimal import Decimal

from py4j.compat import long, basestring, unicode, bytearray2,\
        bytestr, isbytestr, isbytearray, ispython3bytestr, \
        bytetoint, bytetostr, strtobyte


JAVA_MAX_INT = 2147483647


ESCAPE_CHAR = "\\"

# Entry point
ENTRY_POINT_OBJECT_ID = 't'
CONNECTION_PROPERTY_OBJECT_ID = 'c'
STATIC_PREFIX = 'z:'

# JVM
DEFAULT_JVM_ID = 'rj'
DEFAULT_JVM_NAME = 'default'

# Types
BYTES_TYPE = 'j'
INTEGER_TYPE = 'i'
LONG_TYPE = 'L'
BOOLEAN_TYPE = 'b'
DOUBLE_TYPE = 'd'
DECIMAL_TYPE = 'D'
STRING_TYPE = 's'
REFERENCE_TYPE = 'r'
ARRAY_TYPE = 't'
SET_TYPE = 'h'
LIST_TYPE = 'l'
MAP_TYPE = 'a'
NULL_TYPE = 'n'
PACKAGE_TYPE = 'p'
CLASS_TYPE = 'c'
METHOD_TYPE = 'm'
NO_MEMBER = 'o'
VOID_TYPE = 'v'
ITERATOR_TYPE = 'g'
PYTHON_PROXY_TYPE = 'f'

# Protocol
END = 'e'
ERROR = 'x'
SUCCESS = 'y'


# Shortcuts
SUCCESS_PACKAGE = SUCCESS + PACKAGE_TYPE
SUCCESS_CLASS = SUCCESS + CLASS_TYPE
CLASS_FQN_START = 2
END_COMMAND_PART = END + '\n'
NO_MEMBER_COMMAND = SUCCESS + NO_MEMBER

# Commands
CALL_COMMAND_NAME = 'c\n'
FIELD_COMMAND_NAME = 'f\n'
CONSTRUCTOR_COMMAND_NAME = 'i\n'
SHUTDOWN_GATEWAY_COMMAND_NAME = 's\n'
LIST_COMMAND_NAME = 'l\n'
REFLECTION_COMMAND_NAME = "r\n"
MEMORY_COMMAND_NAME = "m\n"
HELP_COMMAND_NAME = 'h\n'
ARRAY_COMMAND_NAME = "a\n"
JVMVIEW_COMMAND_NAME = "j\n"
EXCEPTION_COMMAND_NAME = "p\n"

# Array subcommands
ARRAY_GET_SUB_COMMAND_NAME = 'g\n'
ARRAY_SET_SUB_COMMAND_NAME = 's\n'
ARRAY_SLICE_SUB_COMMAND_NAME = 'l\n'
ARRAY_LEN_SUB_COMMAND_NAME = 'e\n'
ARRAY_CREATE_SUB_COMMAND_NAME = 'c\n'

# Reflection subcommands
REFL_GET_UNKNOWN_SUB_COMMAND_NAME = 'u\n'
REFL_GET_MEMBER_SUB_COMMAND_NAME = 'm\n'


# List subcommands
LIST_SORT_SUBCOMMAND_NAME = 's\n'
LIST_REVERSE_SUBCOMMAND_NAME = 'r\n'
LIST_SLICE_SUBCOMMAND_NAME = 'l\n'
LIST_CONCAT_SUBCOMMAND_NAME = 'a\n'
LIST_MULT_SUBCOMMAND_NAME = 'm\n'
LIST_IMULT_SUBCOMMAND_NAME = 'i\n'
LIST_COUNT_SUBCOMMAND_NAME = 'f\n'

# Field subcommands
FIELD_GET_SUBCOMMAND_NAME = 'g\n'
FIELD_SET_SUBCOMMAND_NAME = 's\n'

# Memory subcommands
MEMORY_DEL_SUBCOMMAND_NAME = 'd\n'
MEMORY_ATTACH_SUBCOMMAND_NAME = 'a\n'

# Help subcommands
HELP_OBJECT_SUBCOMMAND_NAME = 'o\n'
HELP_CLASS_SUBCOMMAND_NAME = 'c\n'

# JVM subcommands
JVM_CREATE_VIEW_SUB_COMMAND_NAME = 'c\n'
JVM_IMPORT_SUB_COMMAND_NAME = 'i\n'
JVM_SEARCH_SUB_COMMAND_NAME = 's\n'
REMOVE_IMPORT_SUB_COMMAND_NAME = 'r\n'

# Callback specific
PYTHON_PROXY_PREFIX = 'p'
ERROR_RETURN_MESSAGE = ERROR + '\n'

CALL_PROXY_COMMAND_NAME = 'c'
GARBAGE_COLLECT_PROXY_COMMAND_NAME = 'g'

OUTPUT_CONVERTER = {NULL_TYPE: (lambda x, y: None),
              BOOLEAN_TYPE: (lambda value, y: value.lower() == 'true'),
              LONG_TYPE: (lambda value, y: long(value)),
              DECIMAL_TYPE: (lambda value, y: Decimal(value)),
              INTEGER_TYPE: (lambda value, y: int(value)),
              BYTES_TYPE: (lambda value, y: decode_bytearray(value)),
              DOUBLE_TYPE: (lambda value, y: float(value)),
              STRING_TYPE: (lambda value, y: unescape_new_line(value)),
              }

INPUT_CONVERTER = []


def escape_new_line(original):
    """Replaces new line characters by a backslash followed by a n.

    Backslashes are also escaped by another backslash.

    :param original: the string to escape

    :rtype: an escaped string
    """
    return smart_decode(original).replace('\\', '\\\\').replace('\r', '\\r').\
            replace('\n', '\\n')


def unescape_new_line(escaped):
    """Replaces escaped characters by unescaped characters.

    For example, double backslashes are replaced by a single backslash.

    :param escaped: the escaped string

    :rtype: the original string
    """
    escaping = False
    original = ''
    for c in escaped:
        if not escaping:
            if c == ESCAPE_CHAR:
                escaping = True
            else:
                original += c
        else:
            if c == 'n':
                original += '\n'
            elif c == 'r':
                original += '\r'
            else:
                original += c
            escaping = False

    return original


def smart_decode(s):
    if isinstance(s, unicode):
        return s
    elif isinstance(s, bytestr):
        # Should never reach this case in Python 3
        return unicode(s, 'utf-8')
    else:
        return unicode(s)


def encode_bytearray(barray):
    if isbytestr(barray):
        return bytetostr(standard_b64encode(barray))
    else:
        newbytestr = bytestr(barray)
        return bytetostr(standard_b64encode(newbytestr))


def decode_bytearray(encoded):
    new_bytes = strtobyte(encoded)
    return bytearray2([bytetoint(b) for b in standard_b64decode(new_bytes)])


def is_python_proxy(parameter):
    """Determines whether parameter is a Python Proxy, i.e., it has a Java
    internal class with an `implements` member.

    :param parameter: the object to check.
    :rtype: True if the parameter is a Python Proxy
    """
    try:
        is_proxy = len(parameter.Java.implements) > 0
    except Exception:
        is_proxy = False

    return is_proxy


def get_command_part(parameter, python_proxy_pool=None):
    """Converts a Python object into a string representation respecting the
    Py4J protocol.

    For example, the integer `1` is converted to `u'i1'`

    :param parameter: the object to convert
    :rtype: the string representing the command part
    """
    command_part = ''

    if parameter == None:
        command_part = NULL_TYPE
    elif isinstance(parameter, bool):
        command_part = BOOLEAN_TYPE + smart_decode(parameter)
    elif isinstance(parameter, Decimal):
        command_part = DECIMAL_TYPE + smart_decode(parameter)
    elif isinstance(parameter, int) and parameter <= JAVA_MAX_INT:
        command_part = INTEGER_TYPE + smart_decode(parameter)
    elif isinstance(parameter, long) or isinstance(parameter, int):
        command_part = LONG_TYPE + smart_decode(parameter)
    elif isinstance(parameter, float):
        command_part = DOUBLE_TYPE + smart_decode(parameter)
    elif isbytearray(parameter):
        command_part = BYTES_TYPE + encode_bytearray(parameter)
    elif ispython3bytestr(parameter):
        command_part = BYTES_TYPE + encode_bytearray(parameter)
    elif isinstance(parameter, basestring):
        command_part = STRING_TYPE + escape_new_line(parameter)
    elif is_python_proxy(parameter):
        command_part = PYTHON_PROXY_TYPE + python_proxy_pool.put(parameter)
        for interface in parameter.Java.implements:
            command_part += ';' + interface
    else:
        command_part = REFERENCE_TYPE + parameter._get_object_id()

    command_part += '\n'

    #print('THIS IS GOING OUT: {0}'.format(command_part))
    #print(type(command_part))
    #for c in command_part:
        #print(ord(c))

    return command_part


def get_return_value(answer, gateway_client, target_id=None, name=None):
    """Converts an answer received from the Java gateway into a Python object.

    For example, string representation of integers are converted to Python
    integer, string representation of objects are converted to JavaObject
    instances, etc.

    :param answer: the string returned by the Java gateway
    :param gateway_client: the gateway client used to communicate with the Java
        Gateway. Only necessary if the answer is a reference (e.g., object,
        list, map)
    :param target_id: the name of the object from which the answer comes from
        (e.g., *object1* in `object1.hello()`). Optional.
    :param name: the name of the member from which the answer comes from
        (e.g., *hello* in `object1.hello()`). Optional.
    """
    if is_error(answer)[0]:
        if len(answer) > 1:
            type = answer[1]
            value = OUTPUT_CONVERTER[type](answer[2:], gateway_client)
            if answer[1] == REFERENCE_TYPE:
                raise Py4JJavaError(
                    'An error occurred while calling {0}{1}{2}.\n'.
                    format(target_id, '.', name), value)
            else:
                raise Py4JError(
                    'An error occurred while calling {0}{1}{2}. Trace:\n{3}\n'.
                    format(target_id, '.', name, value))
        else:
            raise Py4JError(
                    'An error occurred while calling {0}{1}{2}'.
                    format(target_id, '.', name))
    else:
        type = answer[1]
        if type == VOID_TYPE:
            return
        else:
            return OUTPUT_CONVERTER[type](answer[2:], gateway_client)


def is_error(answer):
    if len(answer) == 0 or answer[0] != SUCCESS:
        return (True, None)
    else:
        return (False, None)


def register_output_converter(output_type, converter):
    """Registers an output converter to the list of global output converters.

    :param output_type: A Py4J type of a return object (e.g., MAP_TYPE,
        BOOLEAN_TYPE).
    :param converter: A function that takes an object_id and a gateway_client
        as parameter and that returns a Python object (like a `bool` or a
        `JavaObject` instance).
    """
    global OUTPUT_CONVERTER
    OUTPUT_CONVERTER[output_type] = converter


def register_input_converter(converter):
    """Registers an input converter to the list of global input converters.

    When initialized with `auto_convert=True`, a :class:`JavaGateway
    <py4j.java_gateway.JavaGateway>` will use the input converters on any
    parameter that is not a :class:`JavaObject <py4j.java_gateway.JavaObject>`
    or `basestring` instance.

    :param converter: A converter that declares the methods
        `can_convert(object)` and `convert(object,gateway_client)`.

    """
    global INPUT_CONVERTER
    INPUT_CONVERTER.append(converter)


class Py4JError(Exception):
    """Exception raised when a problem occurs with Py4J."""
    pass


class Py4JNetworkError(Py4JError):
    """Exception raised when a network error occurs with Py4J."""
    pass


class Py4JJavaError(Py4JError):
    """Exception raised when an exception occurs in the client code.

    The exception instance that was thrown on the Java side can be accessed
    with `Py4JJavaError.java_exception`.

    `str(py4j_java_error)` returns the error message and the stack trace
    available on the Java side (similar to printStackTrace()).
    """

    def __init__(self, msg, java_exception):
        self.args = (msg, java_exception)
        self.errmsg = msg
        self.java_exception = java_exception
        self.exception_cmd = EXCEPTION_COMMAND_NAME + REFERENCE_TYPE + \
                java_exception._target_id + '\n' + END_COMMAND_PART

    def __str__(self):
        gateway_client = self.java_exception._gateway_client
        answer = gateway_client.send_command(self.exception_cmd)
        return_value = get_return_value(answer, gateway_client, None, None)
        return '{0}: {1}'.format(self.errmsg, return_value)
