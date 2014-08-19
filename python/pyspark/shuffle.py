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

import os
import sys
import platform
import shutil
import warnings
import gc
import itertools
import operator

import pyspark.heapq3 as heapq
from pyspark.serializers import BatchedSerializer, PickleSerializer, FlattedValuesSerializer

try:
    import psutil

    def get_used_memory():
        """ Return the used memory in MB """
        process = psutil.Process(os.getpid())
        if hasattr(process, "memory_info"):
            info = process.memory_info()
        else:
            info = process.get_memory_info()
        return info.rss >> 20
except ImportError:

    def get_used_memory():
        """ Return the used memory in MB """
        if platform.system() == 'Linux':
            for line in open('/proc/self/status'):
                if line.startswith('VmRSS:'):
                    return int(line.split()[1]) >> 10
        else:
            warnings.warn("Please install psutil to have better "
                          "support with spilling")
            if platform.system() == "Darwin":
                import resource
                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                return rss >> 20
            # TODO: support windows
        return 0


def _get_local_dirs(sub):
    """ Get all the directories """
    path = os.environ.get("SPARK_LOCAL_DIR", "/tmp")
    dirs = path.split(",")
    return [os.path.join(d, "python", str(os.getpid()), sub) for d in dirs]


class Aggregator(object):

    """
    Aggregator has tree functions to merge values into combiner.

    createCombiner:  (value) -> combiner
    mergeValue:      (combine, value) -> combiner
    mergeCombiners:  (combiner, combiner) -> combiner
    """

    def __init__(self, createCombiner, mergeValue, mergeCombiners):
        self.createCombiner = createCombiner
        self.mergeValue = mergeValue
        self.mergeCombiners = mergeCombiners


class SimpleAggregator(Aggregator):

    """
    SimpleAggregator is useful for the cases that combiners have
    same type with values
    """

    def __init__(self, combiner):
        Aggregator.__init__(self, lambda x: x, combiner, combiner)


class Merger(object):

    """
    Merge shuffled data together by aggregator
    """

    def __init__(self, aggregator):
        self.agg = aggregator

    def mergeValues(self, iterator):
        """ Combine the items by creator and combiner """
        raise NotImplementedError

    def mergeCombiners(self, iterator):
        """ Merge the combined items by mergeCombiner """
        raise NotImplementedError

    def iteritems(self):
        """ Return the merged items ad iterator """
        raise NotImplementedError


class InMemoryMerger(Merger):

    """
    In memory merger based on in-memory dict.
    """

    def __init__(self, aggregator):
        Merger.__init__(self, aggregator)
        self.data = {}

    def mergeValues(self, iterator):
        """ Combine the items by creator and combiner """
        # speed up attributes lookup
        d, creator = self.data, self.agg.createCombiner
        comb = self.agg.mergeValue
        for k, v in iterator:
            d[k] = comb(d[k], v) if k in d else creator(v)

    def mergeCombiners(self, iterator):
        """ Merge the combined items by mergeCombiner """
        # speed up attributes lookup
        d, comb = self.data, self.agg.mergeCombiners
        for k, v in iterator:
            d[k] = comb(d[k], v) if k in d else v

    def iteritems(self):
        """ Return the merged items ad iterator """
        return self.data.iteritems()


class ExternalMerger(Merger):

    """
    External merger will dump the aggregated data into disks when
    memory usage goes above the limit, then merge them together.

    This class works as follows:

    - It repeatedly combine the items and save them in one dict in
      memory.

    - When the used memory goes above memory limit, it will split
      the combined data into partitions by hash code, dump them
      into disk, one file per partition.

    - Then it goes through the rest of the iterator, combine items
      into different dict by hash. Until the used memory goes over
      memory limit, it dump all the dicts into disks, one file per
      dict. Repeat this again until combine all the items.

    - Before return any items, it will load each partition and
      combine them seperately. Yield them before loading next
      partition.

    - During loading a partition, if the memory goes over limit,
      it will partition the loaded data and dump them into disks
      and load them partition by partition again.

    `data` and `pdata` are used to hold the merged items in memory.
    At first, all the data are merged into `data`. Once the used
    memory goes over limit, the items in `data` are dumped indo
    disks, `data` will be cleared, all rest of items will be merged
    into `pdata` and then dumped into disks. Before returning, all
    the items in `pdata` will be dumped into disks.

    Finally, if any items were spilled into disks, each partition
    will be merged into `data` and be yielded, then cleared.

    >>> agg = SimpleAggregator(lambda x, y: x + y)
    >>> merger = ExternalMerger(agg, 10)
    >>> N = 10000
    >>> merger.mergeValues(zip(xrange(N), xrange(N)) * 10)
    >>> assert merger.spills > 0
    >>> sum(v for k,v in merger.iteritems())
    499950000

    >>> merger = ExternalMerger(agg, 10)
    >>> merger.mergeCombiners(zip(xrange(N), xrange(N)) * 10)
    >>> assert merger.spills > 0
    >>> sum(v for k,v in merger.iteritems())
    499950000
    """

    # the max total partitions created recursively
    MAX_TOTAL_PARTITIONS = 4096

    def __init__(self, aggregator, memory_limit=512, serializer=None,
                 localdirs=None, scale=1, partitions=59, batch=1000):
        Merger.__init__(self, aggregator)
        self.memory_limit = memory_limit
        # default serializer is only used for tests
        self.serializer = serializer or \
            BatchedSerializer(PickleSerializer(), 1024)
        self.localdirs = localdirs or _get_local_dirs(str(id(self)))
        # number of partitions when spill data into disks
        self.partitions = partitions
        # check the memory after # of items merged
        self.batch = batch
        # scale is used to scale down the hash of key for recursive hash map
        self.scale = scale
        # unpartitioned merged data
        self.data = {}
        # partitioned merged data, list of dicts
        self.pdata = []
        # number of chunks dumped into disks
        self.spills = 0
        # randomize the hash of key, id(o) is the address of o (aligned by 8)
        self._seed = id(self) + 7

    def _get_spill_dir(self, n):
        """ Choose one directory for spill by number n """
        return os.path.join(self.localdirs[n % len(self.localdirs)], str(n))

    def _next_limit(self):
        """
        Return the next memory limit. If the memory is not released
        after spilling, it will dump the data only when the used memory
        starts to increase.
        """
        return max(self.memory_limit, get_used_memory() * 1.05)

    def mergeValues(self, iterator):
        """ Combine the items by creator and combiner """
        # speedup attribute lookup
        creator, comb = self.agg.createCombiner, self.agg.mergeValue
        c, data, pdata, hfun, batch = 0, self.data, self.pdata, self._partition, 100
        limit = self.memory_limit

        for k, v in iterator:
            d = pdata[hfun(k)] if pdata else data
            d[k] = comb(d[k], v) if k in d else creator(v)

            c += 1
            if c >= batch:
                if get_used_memory() >= limit:
                    self._spill()
                    limit = self._next_limit()
                else:
                    batch = min(batch * 2, self.batch)
                c = 0

    def _partition(self, key):
        """ Return the partition for key """
        return hash((key, self._seed)) % self.partitions

    def mergeCombiners(self, iterator, limit=None):
        """ Merge (K,V) pair by mergeCombiner """
        if limit is None:
            limit = self.memory_limit
        # speedup attribute lookup
        comb, hfun = self.agg.mergeCombiners, self._partition
        c, data, pdata, batch = 0, self.data, self.pdata, 1
        for k, v in iterator:
            d = pdata[hfun(k)] if pdata else data
            d[k] = comb(d[k], v) if k in d else v
            if not limit:
                continue

            c += 1
            if c > batch:
                if get_used_memory() > limit:
                    self._spill()
                    limit = self._next_limit()
                    batch /= 4
                else:
                    batch = min(batch * 2, self.batch)
                c = 0

    def _spill(self):
        """
        dump already partitioned data into disks.

        It will dump the data in batch for better performance.
        """
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)

        if not self.pdata:
            # The data has not been partitioned, it will iterator the
            # dataset once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            streams = [open(os.path.join(path, str(i)), 'w')
                       for i in range(self.partitions)]

            for k, v in self.data.iteritems():
                h = self._partition(k)
                # put one item in batch, make it compatitable with load_stream
                # it will increase the memory if dump them in batch
                self.serializer.dump_stream([(k, v)], streams[h])

            for s in streams:
                s.close()

            self.data.clear()
            self.pdata.extend([{} for i in range(self.partitions)])

        else:
            for i in range(self.partitions):
                p = os.path.join(path, str(i))
                with open(p, "w") as f:
                    # dump items in batch
                    self.serializer.dump_stream(self.pdata[i].iteritems(), f)
                self.pdata[i].clear()

        self.spills += 1
        gc.collect()  # release the memory as much as possible

    def iteritems(self):
        """ Return all merged items as iterator """
        if not self.pdata and not self.spills:
            return self.data.iteritems()
        return self._external_items()

    def _external_items(self):
        """ Return all partitioned items as iterator """
        if any(self.pdata):
            self._spill()
        self.pdata = []
        hard_limit = self._next_limit()

        try:
            for i in range(self.partitions):
                for v in self._merged_items(i):
                    yield v
                self.data.clear()
                gc.collect()
                hard_limit = self._next_limit()

                # remove the merged partition
                for j in range(self.spills):
                    path = self._get_spill_dir(j)
                    os.remove(os.path.join(path, str(i)))
        finally:
            self._cleanup()

    def _merged_items(self, index, limit=0):
        self.data = {}
        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            # do not check memory during merging
            self.mergeCombiners(self.serializer.load_stream(open(p)), 0)

            # limit the total partitions
            if (self.scale * self.partitions < self.MAX_TOTAL_PARTITIONS
                    and j < self.spills - 1
                    and get_used_memory() > limit):
                self.data.clear()  # will read from disk again
                gc.collect()  # release the memory as much as possible
                return self._recursive_merged_items(index)

        return self.data.iteritems()

    def _cleanup(self):
        """ Clean up all the files in disks """
        for d in self.localdirs:
            shutil.rmtree(d, True)

    def _recursive_merged_items(self, index):
        """
        merge the partitioned items and return the as iterator

        If one partition can not be fit in memory, then them will be
        partitioned and merged recursively.
        """
        subdirs = [os.path.join(d, "parts", str(index)) for d in self.localdirs]
        m = ExternalMerger(self.agg, self.memory_limit, self.serializer, subdirs,
                           self.scale * self.partitions, self.partitions, self.batch)
        m.pdata = [{} for _ in range(self.partitions)]
        limit = self._next_limit()

        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            m.mergeCombiners(self.serializer.load_stream(open(p)), 0)

            if get_used_memory() > limit:
                m._spill()
                limit = self._next_limit()

        return m._external_items()


class ExternalSorter(object):
    """
    ExtenalSorter will divide the elements into chunks, sort them in
    memory and dump them into disks, finally merge them back.

    The spilling will only happen when the used memory goes above
    the limit.
    """
    def __init__(self, memory_limit, serializer=None):
        self.memory_limit = memory_limit
        self.local_dirs = _get_local_dirs("sort")
        self.serializer = serializer or BatchedSerializer(PickleSerializer(), 1024)

    def _get_path(self, n):
        """ Choose one directory for spill by number n """
        d = self.local_dirs[n % len(self.local_dirs)]
        if not os.path.exists(d):
            os.makedirs(d)
        return os.path.join(d, str(n))

    def sorted(self, iterator, key=None, reverse=False):
        """
        Sort the elements in iterator, do external sort when the memory
        goes above the limit.
        """
        batch = 10
        chunks, current_chunk = [], []
        iterator = iter(iterator)
        while True:
            # pick elements in batch
            chunk = list(itertools.islice(iterator, batch))
            current_chunk.extend(chunk)
            if len(chunk) < batch:
                break

            if get_used_memory() > self.memory_limit:
                # sort them inplace will save memory
                current_chunk.sort(key=key, reverse=reverse)
                path = self._get_path(len(chunks))
                with open(path, 'w') as f:
                    self.serializer.dump_stream(current_chunk, f)
                chunks.append(self.serializer.load_stream(open(path)))
                os.unlink(path)  # data will be deleted after close
                current_chunk = []
                gc.collect()

            elif not chunks:
                batch = min(batch * 2, 10000)

        current_chunk.sort(key=key, reverse=reverse)
        if not chunks:
            return current_chunk

        if current_chunk:
            chunks.append(iter(current_chunk))
        return heapq.merge(chunks, key=key, reverse=reverse)


class SameKey(object):
    """
    take the first few items which has the same expected key

    This is used by GroupByKey.

    >>> l = zip(range(2), range(2))
    >>> list(SameKey(0, [1], iter(l), GroupByKey(iter([]))))
    [1, 0]
    """
    def __init__(self, key, values, it, groupBy):
        self.key = key
        self.values = values
        self.it = it
        self.groupBy = groupBy
        self._index = 0

    def __iter__(self):
        return self

    def next(self):
        if self._index < len(self.values):
            value = self.values[self._index]
            self._index += 1
            return value

        if self.it is None:
            raise StopIteration

        key, value = self.it.next()
        if key != self.key:
            self.groupBy._next_item = (key, value)
            self.it = None
            raise StopIteration
        return value


class GroupByKey(object):
    """
    group a sorted iterator into [(k1, it1), (k2, it2), ...]

    >>> k = [i/3 for i in range(6)]
    >>> v = [[i] for i in range(6)]
    >>> g = GroupByKey(iter(zip(k, v)))
    >>> [(k, list(it)) for k, it in g]
    [(0, [0, 1, 2]), (1, [3, 4, 5])]
    """
    def __init__(self, it):
        self.it = iter(it)
        self._next_item = None
        self.current = None

    def __iter__(self):
        return self

    def next(self):
        if self._next_item is None:
            while True:
                key, value = self.it.next()
                if self.current is None or key != self.current.key:
                    break
                self.current.values.append(value)

        else:
            key, value = self._next_item
            self._next_item = None

        if self.current is not None:
            self.current.it = None
        self.current = SameKey(key, [value], self.it, self)

        return key, (v for vs in self.current for v in vs)


class ExternalGroupBy(ExternalMerger):

    """
    Group by the items by key. If any partition of them can not been
    hold in memory, it will do sort based group by.
    """
    SORT_KEY_LIMIT = 1000

    def _flatted_serializer(self):
        ser = self.serializer
        if not isinstance(ser, (BatchedSerializer, FlattedValuesSerializer)):
            ser = BatchedSerializer(ser, 1024)
        if not isinstance(ser, FlattedValuesSerializer):
            ser = FlattedValuesSerializer(ser, 20)
        return ser

    def _spill(self):
        """
        dump already partitioned data into disks.
        """
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)

        if not self.pdata:
            # The data has not been partitioned, it will iterator the
            # dataset once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            streams = [open(os.path.join(path, str(i)), 'w')
                       for i in range(self.partitions)]

            # If the number of keys is small, then the overhead of sort is small
            # sort them before dumping into disks
            self._sorted = len(self.data) < self.SORT_KEY_LIMIT
            if self._sorted:
                ser = self._flatted_serializer()
                for k in sorted(self.data.keys()):
                    v = self.data[k]
                    h = self._partition(k)
                    ser.dump_stream([(k, v)], streams[h])
                self.serializer = ser
            else:
                for k, v in self.data.iteritems():
                    h = self._partition(k)
                    self.serializer.dump_stream([(k, v)], streams[h])

            for s in streams:
                s.close()

            self.data.clear()
            self.pdata.extend([{} for i in range(self.partitions)])

        else:
            for i in range(self.partitions):
                p = os.path.join(path, str(i))
                with open(p, "w") as f:
                    # dump items in batch
                    if self._sorted:
                        self.serializer.dump_stream(
                            sorted(self.pdata[i].iteritems(), key=operator.itemgetter(0)), f)
                    else:
                        self.serializer.dump_stream(self.pdata[i].iteritems(), f)
                self.pdata[i].clear()

        self.spills += 1
        gc.collect()  # release the memory as much as possible

    def _merge_items(self, index, limit=0):
        size = sum(os.path.getsize(os.path.join(self._get_spill_dir(j), str(index)))
                   for j in range(self.spills))
        # if the memory can not hold all the partition,
        # then use sort based merge
        if (size >> 20) > self.memory_limit / 2:
            return self._sorted_items(index)

        self.data = {}
        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            # do not check memory during merging
            self.mergeCombiners(self.serializer.load_stream(open(p)), 0)
        return self.data.iteritems()

    def _sorted_items(self, index):
        """ load a partition from disk, then sort and group by key """
        def load_partition(j):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            return self.serializer.load_stream(open(p, 'r', 65536))

        disk_items = [load_partition(j) for j in range(self.spills)]

        if self._sorted:
            # all the partitions are already sorted
            sorted_items = heapq.merge(disk_items, key=operator.itemgetter(0))

        else:
            # Flatten the combined values, so it will not consume huge
            # memory during merging sort.
            ser = self._flatted_serializer()
            sorter = ExternalSorter(self.memory_limit, ser)
            sorted_items = sorter.sorted(itertools.chain(*disk_items),
                                         key=operator.itemgetter(0))

        return GroupByKey(sorted_items)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
