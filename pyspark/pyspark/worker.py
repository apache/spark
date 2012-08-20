"""
Worker that receives input from Piped RDD.
"""
import sys
from base64 import standard_b64decode
# CloudPickler needs to be imported so that depicklers are registered using the
# copy_reg module.
from pyspark.cloudpickle import CloudPickler
import cPickle


# Redirect stdout to stderr so that users must return values from functions.
old_stdout = sys.stdout
sys.stdout = sys.stderr


def load_function():
    return cPickle.loads(standard_b64decode(sys.stdin.readline().strip()))


def output(x):
    for line in x.split("\n"):
        old_stdout.write(line.rstrip("\r\n") + "\n")


def read_input():
    for line in sys.stdin:
        yield line.rstrip("\r\n")


def do_combine_by_key():
    create_combiner = load_function()
    merge_value = load_function()
    merge_combiners = load_function()  # TODO: not used.
    depickler = load_function()
    key_pickler = load_function()
    combiner_pickler = load_function()
    combiners = {}
    for line in read_input():
        # Discard the hashcode added in the Python combineByKey() method.
        (key, value) = depickler(line)[1]
        if key not in combiners:
            combiners[key] = create_combiner(value)
        else:
            combiners[key] = merge_value(combiners[key], value)
    for (key, combiner) in combiners.iteritems():
        output(key_pickler(key))
        output(combiner_pickler(combiner))


def do_map(map_pairs=False):
    f = load_function()
    for line in read_input():
        try:
            out = f(line)
            if out is not None:
                if map_pairs:
                    for x in out:
                        output(x)
                else:
                    output(out)
        except:
            sys.stderr.write("Error processing line '%s'\n" % line)
            raise


def do_reduce():
    f = load_function()
    dumps = load_function()
    acc = None
    for line in read_input():
        acc = f(line, acc)
    output(dumps(acc))


def do_echo():
    old_stdout.writelines(sys.stdin.readlines())


def main():
    command = sys.stdin.readline().strip()
    if command == "map":
        do_map(map_pairs=False)
    elif command == "mapPairs":
        do_map(map_pairs=True)
    elif command == "combine_by_key":
        do_combine_by_key()
    elif command == "reduce":
        do_reduce()
    elif command == "echo":
        do_echo()
    else:
        raise Exception("Unsupported command %s" % command)


if __name__ == '__main__':
    main()
