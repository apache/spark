#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import struct
import cPickle


class Batch(object):
    """
    Used to store multiple RDD entries as a single Java object.

    This relieves us from having to explicitly track whether an RDD
    is stored as batches of objects and avoids problems when processing
    the union() of batched and unbatched RDDs (e.g. the union() of textFile()
    with another RDD).
    """
    def __init__(self, items):
        self.items = items


def batched(iterator, batchSize):
    if batchSize == -1: # unlimited batch size
        yield Batch(list(iterator))
    else:
        items = []
        count = 0
        for item in iterator:
            items.append(item)
            count += 1
            if count == batchSize:
                yield Batch(items)
                items = []
                count = 0
        if items:
            yield Batch(items)


def dump_pickle(obj):
    return cPickle.dumps(obj, 2)


load_pickle = cPickle.loads


def read_long(stream):
    length = stream.read(8)
    if length == "":
        raise EOFError
    return struct.unpack("!q", length)[0]


def write_long(value, stream):
    stream.write(struct.pack("!q", value))


def read_int(stream):
    length = stream.read(4)
    if length == "":
        raise EOFError
    return struct.unpack("!i", length)[0]


def write_int(value, stream):
    stream.write(struct.pack("!i", value))


def write_with_length(obj, stream):
    write_int(len(obj), stream)
    stream.write(obj)


def read_with_length(stream):
    length = read_int(stream)
    obj = stream.read(length)
    if obj == "":
        raise EOFError
    return obj


def read_from_pickle_file(stream):
    try:
        while True:
            obj = load_pickle(read_with_length(stream))
            if type(obj) == Batch:  # We don't care about inheritance
                for item in obj.items:
                    yield item
            else:
                yield obj
    except EOFError:
        return
