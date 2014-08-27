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
import random

import pyspark.heapq3 as heapq
from pyspark.serializers import BatchedSerializer, PickleSerializer

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
    path = os.environ.get("SPARK_LOCAL_DIRS", "/tmp")
    dirs = path.split(",")
    if len(dirs) > 1:
        # different order in different processes and instances
        rnd = random.Random(os.getpid() + id(dirs))
        random.shuffle(dirs, rnd.random)
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
        iterator = iter(iterator)
        # speedup attribute lookup
        creator, comb = self.agg.createCombiner, self.agg.mergeValue
        d, c, batch = self.data, 0, self.batch

        for k, v in iterator:
            d[k] = comb(d[k], v) if k in d else creator(v)

            c += 1
            if c % batch == 0 and get_used_memory() > self.memory_limit:
                self._spill()
                self._partitioned_mergeValues(iterator, self._next_limit())
                break

    def _partition(self, key):
        """ Return the partition for key """
        return hash((key, self._seed)) % self.partitions

    def _partitioned_mergeValues(self, iterator, limit=0):
        """ Partition the items by key, then combine them """
        # speedup attribute lookup
        creator, comb = self.agg.createCombiner, self.agg.mergeValue
        c, pdata, hfun, batch = 0, self.pdata, self._partition, self.batch

        for k, v in iterator:
            d = pdata[hfun(k)]
            d[k] = comb(d[k], v) if k in d else creator(v)
            if not limit:
                continue

            c += 1
            if c % batch == 0 and get_used_memory() > limit:
                self._spill()
                limit = self._next_limit()

    def mergeCombiners(self, iterator, check=True):
        """ Merge (K,V) pair by mergeCombiner """
        iterator = iter(iterator)
        # speedup attribute lookup
        d, comb, batch = self.data, self.agg.mergeCombiners, self.batch
        c = 0
        for k, v in iterator:
            d[k] = comb(d[k], v) if k in d else v
            if not check:
                continue

            c += 1
            if c % batch == 0 and get_used_memory() > self.memory_limit:
                self._spill()
                self._partitioned_mergeCombiners(iterator, self._next_limit())
                break

    def _partitioned_mergeCombiners(self, iterator, limit=0):
        """ Partition the items by key, then merge them """
        comb, pdata = self.agg.mergeCombiners, self.pdata
        c, hfun = 0, self._partition
        for k, v in iterator:
            d = pdata[hfun(k)]
            d[k] = comb(d[k], v) if k in d else v
            if not limit:
                continue

            c += 1
            if c % self.batch == 0 and get_used_memory() > limit:
                self._spill()
                limit = self._next_limit()

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
            self.pdata = [{} for i in range(self.partitions)]

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
        assert not self.data
        if any(self.pdata):
            self._spill()
        hard_limit = self._next_limit()

        try:
            for i in range(self.partitions):
                self.data = {}
                for j in range(self.spills):
                    path = self._get_spill_dir(j)
                    p = os.path.join(path, str(i))
                    # do not check memory during merging
                    self.mergeCombiners(self.serializer.load_stream(open(p)),
                                        False)

                    # limit the total partitions
                    if (self.scale * self.partitions < self.MAX_TOTAL_PARTITIONS
                            and j < self.spills - 1
                            and get_used_memory() > hard_limit):
                        self.data.clear()  # will read from disk again
                        gc.collect()  # release the memory as much as possible
                        for v in self._recursive_merged_items(i):
                            yield v
                        return

                for v in self.data.iteritems():
                    yield v
                self.data.clear()
                gc.collect()

                # remove the merged partition
                for j in range(self.spills):
                    path = self._get_spill_dir(j)
                    os.remove(os.path.join(path, str(i)))

        finally:
            self._cleanup()

    def _cleanup(self):
        """ Clean up all the files in disks """
        for d in self.localdirs:
            shutil.rmtree(d, True)

    def _recursive_merged_items(self, start):
        """
        merge the partitioned items and return the as iterator

        If one partition can not be fit in memory, then them will be
        partitioned and merged recursively.
        """
        # make sure all the data are dumps into disks.
        assert not self.data
        if any(self.pdata):
            self._spill()
        assert self.spills > 0

        for i in range(start, self.partitions):
            subdirs = [os.path.join(d, "parts", str(i))
                       for d in self.localdirs]
            m = ExternalMerger(self.agg, self.memory_limit, self.serializer,
                               subdirs, self.scale * self.partitions)
            m.pdata = [{} for _ in range(self.partitions)]
            limit = self._next_limit()

            for j in range(self.spills):
                path = self._get_spill_dir(j)
                p = os.path.join(path, str(i))
                m._partitioned_mergeCombiners(
                    self.serializer.load_stream(open(p)))

                if get_used_memory() > limit:
                    m._spill()
                    limit = self._next_limit()

            for v in m._external_items():
                yield v

            # remove the merged partition
            for j in range(self.spills):
                path = self._get_spill_dir(j)
                os.remove(os.path.join(path, str(i)))


class ExternalSorter(object):
    """
    ExtenalSorter will divide the elements into chunks, sort them in
    memory and dump them into disks, finally merge them back.

    The spilling will only happen when the used memory goes above
    the limit.

    >>> sorter = ExternalSorter(1)  # 1M
    >>> import random
    >>> l = range(1024)
    >>> random.shuffle(l)
    >>> sorted(l) == list(sorter.sorted(l))
    True
    >>> sorted(l) == list(sorter.sorted(l, key=lambda x: -x, reverse=True))
    True
    """
    def __init__(self, memory_limit, serializer=None):
        self.memory_limit = memory_limit
        self.local_dirs = _get_local_dirs("sort")
        self.serializer = serializer or BatchedSerializer(PickleSerializer(), 1024)
        self._spilled_bytes = 0

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
                self._spilled_bytes += os.path.getsize(path)
                chunks.append(self.serializer.load_stream(open(path)))
                current_chunk = []

            elif not chunks:
                batch = min(batch * 2, 10000)

        current_chunk.sort(key=key, reverse=reverse)
        if not chunks:
            return current_chunk

        if current_chunk:
            chunks.append(iter(current_chunk))

        return heapq.merge(chunks, key=key, reverse=reverse)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
