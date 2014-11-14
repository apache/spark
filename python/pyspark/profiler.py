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

import cProfile
import pstats
import os
import atexit
from pyspark.accumulators import PStatsParam


class ProfilerCollector(object):
    """
    This class keeps track of different profilers on a per
    stage basis. Also this is used to create new profilers for
    the different stages.
    """

    def __init__(self, profiler):
        self.profilers = []
        self.profile_dump_path = None
        self.profiler = profiler if profiler else BasicProfiler

    def add_profiler(self, id, profiler):
        if not self.profilers:
            if self.profile_dump_path:
                atexit.register(self.dump_profiles)
            else:
                atexit.register(self.show_profiles)

        self.profilers.append([id, profiler, False])

    def dump_profiles(self):
        for id, profiler, _ in self.profilers:
            profiler.dump(id, self.profile_dump_path)
        self.profilers = []

    def show_profiles(self):
        """ Print the profile stats to stdout """
        for i, (id, profiler, showed) in enumerate(self.profilers):
            if not showed and profiler:
                profiler.show(id)
                # mark it as showed
                self.profilers[i][2] = True

    def new_profiler(self, ctx):
        return self.profiler(ctx)


class BasicProfiler(object):
    """

    :: DeveloperApi ::

    PySpark supports custom profilers, this is to allow for different profilers to
    be used as well as outputting to different formats than what is provided in the
    BasicProfiler.

    A custom profiler has to define or inherit the following methods:
        profile - will produce a system profile of some sort.
        show - shows collected profiles for this profiler in a readable format
        dump - dumps the profiles to a path
        add - adds a profile to the existing accumulated profile

    The profiler class is chosen when creating a SparkContext

    >>> from pyspark.context import SparkContext
    >>> from pyspark.conf import SparkConf
    >>> from pyspark.profiler import BasicProfiler
    >>> class MyCustomProfiler(BasicProfiler):
    ...     def show(self, id):
    ...         print "My custom profiles for RDD:%s" % id
    ...
    >>> conf = SparkConf().set("spark.python.profile", "true")
    >>> sc = SparkContext('local', 'test', conf=conf, profiler=MyCustomProfiler)
    >>> sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10)
    [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
    >>> sc.show_profiles()
    My custom profiles for RDD:1
    My custom profiles for RDD:2
    >>> sc.stop()
    """

    def __init__(self, ctx):
        self._new_profile_accumulator(ctx)

    def profile(self, to_profile):
        """ Runs and profiles the method to_profile passed in. A profile object is returned. """
        pr = cProfile.Profile()
        pr.runcall(to_profile)
        st = pstats.Stats(pr)
        st.stream = None  # make it picklable
        st.strip_dirs()
        return st

    def show(self, id):
        """ Print the profile stats to stdout, id is the RDD id """
        stats = self._accumulator.value
        if stats:
            print "=" * 60
            print "Profile of RDD<id=%d>" % id
            print "=" * 60
            stats.sort_stats("time", "cumulative").print_stats()

    def dump(self, id, path):
        """ Dump the profile into path, id is the RDD id """
        if not os.path.exists(path):
            os.makedirs(path)
        stats = self._accumulator.value
        if stats:
            p = os.path.join(path, "rdd_%d.pstats" % id)
            stats.dump_stats(p)

    def _new_profile_accumulator(self, ctx):
        """
        Creates a new accumulator for combining the profiles of different
        partitions of a stage
        """
        self._accumulator = ctx.accumulator(None, PStatsParam)

    def add(self, accum_value):
        """ Adds a new profile to the existing accumulated value """
        self._accumulator.add(accum_value)
