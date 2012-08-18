"""
Worker that receives input from Piped RDD.
"""
import sys
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
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
            yield loads(sys.stdin)
    except EOFError:
        return

def do_combine_by_key():
    create_combiner = load_function()
    merge_value = load_function()
    merge_combiners = load_function()  # TODO: not used.
    combiners = {}
    for obj in read_input():
        (key, value) = PickleSerializer.loads(obj)
        if key not in combiners:
            combiners[key] = create_combiner(value)
        else:
            combiners[key] = merge_value(combiners[key], value)
    for (key, combiner) in combiners.iteritems():
        output(PickleSerializer.dumps((key, combiner)))


def do_map(flat=False):
    f = load_function()
    for obj in read_input():
        try:
            #from pickletools import dis
            #print repr(obj)
            #print dis(obj)
            out = f(PickleSerializer.loads(obj))
            if out is not None:
                if flat:
                    for x in out:
                        output(PickleSerializer.dumps(x))
                else:
                    output(PickleSerializer.dumps(out))
        except:
            sys.stderr.write("Error processing obj %s\n" % repr(obj))
            raise


def do_shuffle_map_step():
    for obj in read_input():
        key = PickleSerializer.loads(obj)[1]
        output(str(hash(key)))
        output(obj)


def do_reduce():
    f = load_function()
    acc = None
    for obj in read_input():
        acc = f(PickleSerializer.loads(obj), acc)
    if acc is not None:
        output(PickleSerializer.dumps(acc))


def do_echo():
    old_stdout.writelines(sys.stdin.readlines())


def main():
    command = sys.stdin.readline().strip()
    if command == "map":
        do_map(flat=False)
    elif command == "flatmap":
        do_map(flat=True)
    elif command == "combine_by_key":
        do_combine_by_key()
    elif command == "reduce":
        do_reduce()
    elif command == "shuffle_map_step":
        do_shuffle_map_step()
    elif command == "echo":
        do_echo()
    else:
        raise Exception("Unsupported command %s" % command)


if __name__ == '__main__':
    main()
