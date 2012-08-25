import struct
import cPickle


def dump_pickle(obj):
    return cPickle.dumps(obj, 2)


load_pickle = cPickle.loads


def write_with_length(obj, stream):
    stream.write(struct.pack("!i", len(obj)))
    stream.write(obj)


def read_with_length(stream):
    length = stream.read(4)
    if length == "":
        raise EOFError
    length = struct.unpack("!i", length)[0]
    obj = stream.read(length)
    if obj == "":
        raise EOFError
    return obj
