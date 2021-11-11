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
import platform
import shutil
import warnings
import gc
import itertools
import operator
import random
import sys

import heapq
from pyspark.serializers import (
    BatchedSerializer,
    PickleSerializer,
    FlattenedValuesSerializer,
    CompressedSerializer,
    AutoBatchedSerializer,
)
from pyspark.util import fail_on_stopiteration  # type: ignore


try:
    import psutil

    process = None

    def get_used_memory():
        """Return the used memory in MiB"""
        global process
        if process is None or process._pid != os.getpid():
            process = psutil.Process(os.getpid())
        if hasattr(process, "memory_info"):
            info = process.memory_info()
        else:
            info = process.get_memory_info()
        return info.rss >> 20


except ImportError:

    def get_used_memory():
        """Return the used memory in MiB"""
        if platform.system() == "Linux":
            for line in open("/proc/self/status"):
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) >> 10

        else:
            warnings.warn("Please install psutil to have better " "support with spilling")
            if platform.system() == "Darwin":
                import resource

                rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                return rss >> 20
            # TODO: support windows

        return 0


def _get_local_dirs(sub):
    """Get all the directories"""
    path = os.environ.get("SPARK_LOCAL_DIRS", "/tmp")
    dirs = path.split(",")
    if len(dirs) > 1:
        # different order in different processes and instances
        rnd = random.Random(os.getpid() + id(dirs))
        random.shuffle(dirs, rnd.random)
    return [os.path.join(d, "python", str(os.getpid()), sub) for d in dirs]


# global stats
MemoryBytesSpilled = 0
DiskBytesSpilled = 0


class Aggregator(object):

    """
    Aggregator has tree functions to merge values into combiner.

    createCombiner:  (value) -> combiner
    mergeValue:      (combine, value) -> combiner
    mergeCombiners:  (combiner, combiner) -> combiner
    """

    def __init__(self, createCombiner, mergeValue, mergeCombiners):
        self.createCombiner = fail_on_stopiteration(createCombiner)
        self.mergeValue = fail_on_stopiteration(mergeValue)
        self.mergeCombiners = fail_on_stopiteration(mergeCombiners)


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
        """Combine the items by creator and combiner"""
        raise NotImplementedError

    def mergeCombiners(self, iterator):
        """Merge the combined items by mergeCombiner"""
        raise NotImplementedError

    def items(self):
        """Return the merged items ad iterator"""
        raise NotImplementedError


def _compressed_serializer(self, serializer=None):
    # always use PickleSerializer to simplify implementation
    ser = PickleSerializer()
    return AutoBatchedSerializer(CompressedSerializer(ser))


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
      combine them separately. Yield them before loading next
      partition.

    - During loading a partition, if the memory goes over limit,
      it will partition the loaded data and dump them into disks
      and load them partition by partition again.

    `data` and `pdata` are used to hold the merged items in memory.
    At first, all the data are merged into `data`. Once the used
    memory goes over limit, the items in `data` are dumped into
    disks, `data` will be cleared, all rest of items will be merged
    into `pdata` and then dumped into disks. Before returning, all
    the items in `pdata` will be dumped into disks.

    Finally, if any items were spilled into disks, each partition
    will be merged into `data` and be yielded, then cleared.

    Examples
    --------
    >>> agg = SimpleAggregator(lambda x, y: x + y)
    >>> merger = ExternalMerger(agg, 10)
    >>> N = 10000
    >>> merger.mergeValues(zip(range(N), range(N)))
    >>> assert merger.spills > 0
    >>> sum(v for k,v in merger.items())
    49995000

    >>> merger = ExternalMerger(agg, 10)
    >>> merger.mergeCombiners(zip(range(N), range(N)))
    >>> assert merger.spills > 0
    >>> sum(v for k,v in merger.items())
    49995000
    """

    # the max total partitions created recursively
    MAX_TOTAL_PARTITIONS = 4096

    def __init__(
        self,
        aggregator,
        memory_limit=512,
        serializer=None,
        localdirs=None,
        scale=1,
        partitions=59,
        batch=1000,
    ):
        Merger.__init__(self, aggregator)
        self.memory_limit = memory_limit
        self.serializer = _compressed_serializer(serializer)
        self.localdirs = localdirs or _get_local_dirs(str(id(self)))
        # number of partitions when spill data into disks
        self.partitions = partitions
        # check the memory after # of items merged
        self.batch = batch
        # scale is used to scale down the hash of key for recursive hash map
        self.scale = scale
        # un-partitioned merged data
        self.data = {}
        # partitioned merged data, list of dicts
        self.pdata = []
        # number of chunks dumped into disks
        self.spills = 0
        # randomize the hash of key, id(o) is the address of o (aligned by 8)
        self._seed = id(self) + 7

    def _get_spill_dir(self, n):
        """Choose one directory for spill by number n"""
        return os.path.join(self.localdirs[n % len(self.localdirs)], str(n))

    def _next_limit(self):
        """
        Return the next memory limit. If the memory is not released
        after spilling, it will dump the data only when the used memory
        starts to increase.
        """
        return max(self.memory_limit, get_used_memory() * 1.05)

    def mergeValues(self, iterator):
        """Combine the items by creator and combiner"""
        # speedup attribute lookup
        creator, comb = self.agg.createCombiner, self.agg.mergeValue
        c, data, pdata, hfun, batch = 0, self.data, self.pdata, self._partition, self.batch
        limit = self.memory_limit

        for k, v in iterator:
            d = pdata[hfun(k)] if pdata else data
            d[k] = comb(d[k], v) if k in d else creator(v)

            c += 1
            if c >= batch:
                if get_used_memory() >= limit:
                    self._spill()
                    limit = self._next_limit()
                    batch /= 2
                    c = 0
                else:
                    batch *= 1.5

        if get_used_memory() >= limit:
            self._spill()

    def _partition(self, key):
        """Return the partition for key"""
        return hash((key, self._seed)) % self.partitions

    def _object_size(self, obj):
        """How much of memory for this obj, assume that all the objects
        consume similar bytes of memory
        """
        return 1

    def mergeCombiners(self, iterator, limit=None):
        """Merge (K,V) pair by mergeCombiner"""
        if limit is None:
            limit = self.memory_limit
        # speedup attribute lookup
        comb, hfun, objsize = self.agg.mergeCombiners, self._partition, self._object_size
        c, data, pdata, batch = 0, self.data, self.pdata, self.batch
        for k, v in iterator:
            d = pdata[hfun(k)] if pdata else data
            d[k] = comb(d[k], v) if k in d else v
            if not limit:
                continue

            c += objsize(v)
            if c > batch:
                if get_used_memory() > limit:
                    self._spill()
                    limit = self._next_limit()
                    batch /= 2
                    c = 0
                else:
                    batch *= 1.5

        if limit and get_used_memory() >= limit:
            self._spill()

    def _spill(self):
        """
        dump already partitioned data into disks.

        It will dump the data in batch for better performance.
        """
        global MemoryBytesSpilled, DiskBytesSpilled
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)

        used_memory = get_used_memory()
        if not self.pdata:
            # The data has not been partitioned, it will iterator the
            # dataset once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            streams = [open(os.path.join(path, str(i)), "wb") for i in range(self.partitions)]

            for k, v in self.data.items():
                h = self._partition(k)
                # put one item in batch, make it compatible with load_stream
                # it will increase the memory if dump them in batch
                self.serializer.dump_stream([(k, v)], streams[h])

            for s in streams:
                DiskBytesSpilled += s.tell()
                s.close()

            self.data.clear()
            self.pdata.extend([{} for i in range(self.partitions)])

        else:
            for i in range(self.partitions):
                p = os.path.join(path, str(i))
                with open(p, "wb") as f:
                    # dump items in batch
                    self.serializer.dump_stream(iter(self.pdata[i].items()), f)
                self.pdata[i].clear()
                DiskBytesSpilled += os.path.getsize(p)

        self.spills += 1
        gc.collect()  # release the memory as much as possible
        MemoryBytesSpilled += max(used_memory - get_used_memory(), 0) << 20

    def items(self):
        """Return all merged items as iterator"""
        if not self.pdata and not self.spills:
            return iter(self.data.items())
        return self._external_items()

    def _external_items(self):
        """Return all partitioned items as iterator"""
        assert not self.data
        if any(self.pdata):
            self._spill()
        # disable partitioning and spilling when merge combiners from disk
        self.pdata = []

        try:
            for i in range(self.partitions):
                for v in self._merged_items(i):
                    yield v
                self.data.clear()

                # remove the merged partition
                for j in range(self.spills):
                    path = self._get_spill_dir(j)
                    os.remove(os.path.join(path, str(i)))
        finally:
            self._cleanup()

    def _merged_items(self, index):
        self.data = {}
        limit = self._next_limit()
        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            # do not check memory during merging
            with open(p, "rb") as f:
                self.mergeCombiners(self.serializer.load_stream(f), 0)

            # limit the total partitions
            if (
                self.scale * self.partitions < self.MAX_TOTAL_PARTITIONS
                and j < self.spills - 1
                and get_used_memory() > limit
            ):
                self.data.clear()  # will read from disk again
                gc.collect()  # release the memory as much as possible
                return self._recursive_merged_items(index)

        return self.data.items()

    def _recursive_merged_items(self, index):
        """
        merge the partitioned items and return the as iterator

        If one partition can not be fit in memory, then them will be
        partitioned and merged recursively.
        """
        subdirs = [os.path.join(d, "parts", str(index)) for d in self.localdirs]
        m = ExternalMerger(
            self.agg,
            self.memory_limit,
            self.serializer,
            subdirs,
            self.scale * self.partitions,
            self.partitions,
            self.batch,
        )
        m.pdata = [{} for _ in range(self.partitions)]
        limit = self._next_limit()

        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            with open(p, "rb") as f:
                m.mergeCombiners(self.serializer.load_stream(f), 0)

            if get_used_memory() > limit:
                m._spill()
                limit = self._next_limit()

        return m._external_items()

    def _cleanup(self):
        """Clean up all the files in disks"""
        for d in self.localdirs:
            shutil.rmtree(d, True)


class ExternalSorter(object):
    """
    ExternalSorter will divide the elements into chunks, sort them in
    memory and dump them into disks, finally merge them back.

    The spilling will only happen when the used memory goes above
    the limit.

    Examples
    --------
    >>> sorter = ExternalSorter(1)  # 1M
    >>> import random
    >>> l = list(range(1024))
    >>> random.shuffle(l)
    >>> sorted(l) == list(sorter.sorted(l))
    True
    >>> sorted(l) == list(sorter.sorted(l, key=lambda x: -x, reverse=True))
    True
    """

    def __init__(self, memory_limit, serializer=None):
        self.memory_limit = memory_limit
        self.local_dirs = _get_local_dirs("sort")
        self.serializer = _compressed_serializer(serializer)

    def _get_path(self, n):
        """Choose one directory for spill by number n"""
        d = self.local_dirs[n % len(self.local_dirs)]
        if not os.path.exists(d):
            os.makedirs(d)
        return os.path.join(d, str(n))

    def _next_limit(self):
        """
        Return the next memory limit. If the memory is not released
        after spilling, it will dump the data only when the used memory
        starts to increase.
        """
        return max(self.memory_limit, get_used_memory() * 1.05)

    def sorted(self, iterator, key=None, reverse=False):
        """
        Sort the elements in iterator, do external sort when the memory
        goes above the limit.
        """
        global MemoryBytesSpilled, DiskBytesSpilled
        batch, limit = 100, self._next_limit()
        chunks, current_chunk = [], []
        iterator = iter(iterator)
        while True:
            # pick elements in batch
            chunk = list(itertools.islice(iterator, batch))
            current_chunk.extend(chunk)
            if len(chunk) < batch:
                break

            used_memory = get_used_memory()
            if used_memory > limit:
                # sort them inplace will save memory
                current_chunk.sort(key=key, reverse=reverse)
                path = self._get_path(len(chunks))
                with open(path, "wb") as f:
                    self.serializer.dump_stream(current_chunk, f)

                def load(f):
                    for v in self.serializer.load_stream(f):
                        yield v
                    # close the file explicit once we consume all the items
                    # to avoid ResourceWarning in Python3
                    f.close()

                chunks.append(load(open(path, "rb")))
                current_chunk = []
                MemoryBytesSpilled += max(used_memory - get_used_memory(), 0) << 20
                DiskBytesSpilled += os.path.getsize(path)
                os.unlink(path)  # data will be deleted after close

            elif not chunks:
                batch = min(int(batch * 1.5), 10000)

        current_chunk.sort(key=key, reverse=reverse)
        if not chunks:
            return current_chunk

        if current_chunk:
            chunks.append(iter(current_chunk))

        return heapq.merge(*chunks, key=key, reverse=reverse)


class ExternalList(object):
    """
    ExternalList can have many items which cannot be hold in memory in
    the same time.

    Examples
    --------
    >>> l = ExternalList(list(range(100)))
    >>> len(l)
    100
    >>> l.append(10)
    >>> len(l)
    101
    >>> for i in range(20240):
    ...     l.append(i)
    >>> len(l)
    20341
    >>> import pickle
    >>> l2 = pickle.loads(pickle.dumps(l))
    >>> len(l2)
    20341
    >>> list(l2)[100]
    10
    """

    LIMIT = 10240

    def __init__(self, values):
        self.values = values
        self.count = len(values)
        self._file = None
        self._ser = None

    def __getstate__(self):
        if self._file is not None:
            self._file.flush()
            with os.fdopen(os.dup(self._file.fileno()), "rb") as f:
                f.seek(0)
                serialized = f.read()
        else:
            serialized = b""
        return self.values, self.count, serialized

    def __setstate__(self, item):
        self.values, self.count, serialized = item
        if serialized:
            self._open_file()
            self._file.write(serialized)
        else:
            self._file = None
            self._ser = None

    def __iter__(self):
        if self._file is not None:
            self._file.flush()
            # read all items from disks first
            with os.fdopen(os.dup(self._file.fileno()), "rb") as f:
                f.seek(0)
                for v in self._ser.load_stream(f):
                    yield v

        for v in self.values:
            yield v

    def __len__(self):
        return self.count

    def append(self, value):
        self.values.append(value)
        self.count += 1
        # dump them into disk if the key is huge
        if len(self.values) >= self.LIMIT:
            self._spill()

    def _open_file(self):
        dirs = _get_local_dirs("objects")
        d = dirs[id(self) % len(dirs)]
        if not os.path.exists(d):
            os.makedirs(d)
        p = os.path.join(d, str(id(self)))
        self._file = open(p, "w+b", 65536)
        self._ser = BatchedSerializer(CompressedSerializer(PickleSerializer()), 1024)
        os.unlink(p)

    def __del__(self):
        if self._file:
            self._file.close()
            self._file = None

    def _spill(self):
        """dump the values into disk"""
        global MemoryBytesSpilled, DiskBytesSpilled
        if self._file is None:
            self._open_file()

        used_memory = get_used_memory()
        pos = self._file.tell()
        self._ser.dump_stream(self.values, self._file)
        self.values = []
        gc.collect()
        DiskBytesSpilled += self._file.tell() - pos
        MemoryBytesSpilled += max(used_memory - get_used_memory(), 0) << 20


class ExternalListOfList(ExternalList):
    """
    An external list for list.

    Examples
    --------
    >>> l = ExternalListOfList([[i, i] for i in range(100)])
    >>> len(l)
    200
    >>> l.append(range(10))
    >>> len(l)
    210
    >>> len(list(l))
    210
    """

    def __init__(self, values):
        ExternalList.__init__(self, values)
        self.count = sum(len(i) for i in values)

    def append(self, value):
        ExternalList.append(self, value)
        # already counted 1 in ExternalList.append
        self.count += len(value) - 1

    def __iter__(self):
        for values in ExternalList.__iter__(self):
            for v in values:
                yield v


class GroupByKey(object):
    """
    Group a sorted iterator as [(k1, it1), (k2, it2), ...]

    Examples
    --------
    >>> k = [i // 3 for i in range(6)]
    >>> v = [[i] for i in range(6)]
    >>> g = GroupByKey(zip(k, v))
    >>> [(k, list(it)) for k, it in g]
    [(0, [0, 1, 2]), (1, [3, 4, 5])]
    """

    def __init__(self, iterator):
        self.iterator = iterator

    def __iter__(self):
        key, values = None, None
        for k, v in self.iterator:
            if values is not None and k == key:
                values.append(v)
            else:
                if values is not None:
                    yield (key, values)
                key = k
                values = ExternalListOfList([v])
        if values is not None:
            yield (key, values)


class ExternalGroupBy(ExternalMerger):

    """
    Group by the items by key. If any partition of them can not been
    hold in memory, it will do sort based group by.

    This class works as follows:

    - It repeatedly group the items by key and save them in one dict in
      memory.

    - When the used memory goes above memory limit, it will split
      the combined data into partitions by hash code, dump them
      into disk, one file per partition. If the number of keys
      in one partitions is smaller than 1000, it will sort them
      by key before dumping into disk.

    - Then it goes through the rest of the iterator, group items
      by key into different dict by hash. Until the used memory goes over
      memory limit, it dump all the dicts into disks, one file per
      dict. Repeat this again until combine all the items. It
      also will try to sort the items by key in each partition
      before dumping into disks.

    - It will yield the grouped items partitions by partitions.
      If the data in one partitions can be hold in memory, then it
      will load and combine them in memory and yield.

    - If the dataset in one partition cannot be hold in memory,
      it will sort them first. If all the files are already sorted,
      it merge them by heap.merge(), so it will do external sort
      for all the files.

    - After sorting, `GroupByKey` class will put all the continuous
      items with the same key as a group, yield the values as
      an iterator.
    """

    SORT_KEY_LIMIT = 1000

    def flattened_serializer(self):
        assert isinstance(self.serializer, BatchedSerializer)
        ser = self.serializer
        return FlattenedValuesSerializer(ser, 20)

    def _object_size(self, obj):
        return len(obj)

    def _spill(self):
        """
        dump already partitioned data into disks.
        """
        global MemoryBytesSpilled, DiskBytesSpilled
        path = self._get_spill_dir(self.spills)
        if not os.path.exists(path):
            os.makedirs(path)

        used_memory = get_used_memory()
        if not self.pdata:
            # The data has not been partitioned, it will iterator the
            # data once, write them into different files, has no
            # additional memory. It only called when the memory goes
            # above limit at the first time.

            # open all the files for writing
            streams = [open(os.path.join(path, str(i)), "wb") for i in range(self.partitions)]

            # If the number of keys is small, then the overhead of sort is small
            # sort them before dumping into disks
            self._sorted = len(self.data) < self.SORT_KEY_LIMIT
            if self._sorted:
                self.serializer = self.flattened_serializer()
                for k in sorted(self.data.keys()):
                    h = self._partition(k)
                    self.serializer.dump_stream([(k, self.data[k])], streams[h])
            else:
                for k, v in self.data.items():
                    h = self._partition(k)
                    self.serializer.dump_stream([(k, v)], streams[h])

            for s in streams:
                DiskBytesSpilled += s.tell()
                s.close()

            self.data.clear()
            # self.pdata is cached in `mergeValues` and `mergeCombiners`
            self.pdata.extend([{} for i in range(self.partitions)])

        else:
            for i in range(self.partitions):
                p = os.path.join(path, str(i))
                with open(p, "wb") as f:
                    # dump items in batch
                    if self._sorted:
                        # sort by key only (stable)
                        sorted_items = sorted(self.pdata[i].items(), key=operator.itemgetter(0))
                        self.serializer.dump_stream(sorted_items, f)
                    else:
                        self.serializer.dump_stream(self.pdata[i].items(), f)
                self.pdata[i].clear()
                DiskBytesSpilled += os.path.getsize(p)

        self.spills += 1
        gc.collect()  # release the memory as much as possible
        MemoryBytesSpilled += max(used_memory - get_used_memory(), 0) << 20

    def _merged_items(self, index):
        size = sum(
            os.path.getsize(os.path.join(self._get_spill_dir(j), str(index)))
            for j in range(self.spills)
        )
        # if the memory can not hold all the partition,
        # then use sort based merge. Because of compression,
        # the data on disks will be much smaller than needed memory
        if size >= self.memory_limit << 17:  # * 1M / 8
            return self._merge_sorted_items(index)

        self.data = {}
        for j in range(self.spills):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            # do not check memory during merging
            with open(p, "rb") as f:
                self.mergeCombiners(self.serializer.load_stream(f), 0)
        return self.data.items()

    def _merge_sorted_items(self, index):
        """load a partition from disk, then sort and group by key"""

        def load_partition(j):
            path = self._get_spill_dir(j)
            p = os.path.join(path, str(index))
            with open(p, "rb", 65536) as f:
                for v in self.serializer.load_stream(f):
                    yield v

        disk_items = [load_partition(j) for j in range(self.spills)]

        if self._sorted:
            # all the partitions are already sorted
            sorted_items = heapq.merge(*disk_items, key=operator.itemgetter(0))

        else:
            # Flatten the combined values, so it will not consume huge
            # memory during merging sort.
            ser = self.flattened_serializer()
            sorter = ExternalSorter(self.memory_limit, ser)
            sorted_items = sorter.sorted(itertools.chain(*disk_items), key=operator.itemgetter(0))
        return ((k, vs) for k, vs in GroupByKey(sorted_items))


if __name__ == "__main__":
    import doctest

    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
