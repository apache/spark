import sys
from itertools import chain
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer

def collect(binary_file_path):
    dse = PickleSerializer()
    with open(binary_file_path, 'rb') as tempFile:
        for item in dse.load_stream(tempFile):
            yield item
def main():
    try:
        binary_file_path = sys.argv[1]
    except:
        print "Missed FilePath in argement"

    if not binary_file_path:
        return 

    counter = 0
    for rdd in chain.from_iterable(collect(binary_file_path)):
        print rdd
        counter = counter + 1
        if counter >= 10:
            print "..."
            break

if __name__ =="__main__":
    exit(main())
