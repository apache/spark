"""
Worker that receives input from Piped RDD.
"""
import sys
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.cloudpickle import CloudPickler
from pyspark.serializers import dumps, loads, PickleSerializer
import cPickle

# Redirect stdout to stderr so that users must return values from functions.
old_stdout = sys.stdout
sys.stdout = sys.stderr


def load_function():
    return cPickle.loads(standard_b64decode(sys.stdin.readline().strip()))


def output(x):
    dumps(x, old_stdout)


def read_input():
    try:
        while True:
            yield cPickle.loads(loads(sys.stdin))
    except EOFError:
        return


def do_pipeline():
    f = load_function()
    for obj in f(read_input()):
        output(PickleSerializer.dumps(obj))


def do_shuffle_map_step():
    hashFunc = load_function()
    while True:
        try:
            pickled = loads(sys.stdin)
        except EOFError:
            return
        key = cPickle.loads(pickled)[0]
        output(str(hashFunc(key)))
        output(pickled)


def main():
    num_broadcast_variables = int(sys.stdin.readline().strip())
    for _ in range(num_broadcast_variables):
        uuid = sys.stdin.read(36)
        value = loads(sys.stdin)
        _broadcastRegistry[uuid] = Broadcast(uuid, cPickle.loads(value))
    command = sys.stdin.readline().strip()
    if command == "pipeline":
        do_pipeline()
    elif command == "shuffle_map_step":
        do_shuffle_map_step()
    else:
        raise Exception("Unsupported command %s" % command)


if __name__ == '__main__':
    main()
