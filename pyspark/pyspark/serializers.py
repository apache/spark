import struct
import cPickle


def dump_pickle(obj):
    return cPickle.dumps(obj, 2)


load_pickle = cPickle.loads


def read_long(stream):
    length = stream.read(8)
    if length == "":
        raise EOFError
    return struct.unpack("!q", length)[0]


def read_int(stream):
    length = stream.read(4)
    if length == "":
        raise EOFError
    return struct.unpack("!i", length)[0]

def write_with_length(obj, stream):
    stream.write(struct.pack("!i", len(obj)))
    stream.write(obj)


def read_with_length(stream):
    length = read_int(stream)
    obj = stream.read(length)
    if obj == "":
        raise EOFError
    return obj
