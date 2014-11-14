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

"""
>>> from pyspark.context import SparkContext
>>> from pyspark.conf import SparkConf
>>> from pyspark.profiler import BasicProfiler
>>> class MyCustomProfiler(BasicProfiler):
...     @staticmethod
...     def show_profiles(profilers):
...             print "My custom profiles"
...
>>> conf = SparkConf().set("spark.python.profile", "true")
>>> sc = SparkContext('local', 'test', conf=conf, profiler=MyCustomProfiler)
>>> sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10)
[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
>>> sc.show_profiles()
My custom profiles
>>> sc.stop()
"""


import cProfile
import pstats
import os
from pyspark.accumulators import PStatsParam


class BasicProfiler(object):
    """

    :: DeveloperApi ::

    PySpark supports custom profilers, this is to allow for different profilers to
    be used as well as outputting to different formats than what is provided in the
    BasicProfiler.

    A custom profiler has to define the following static methods:
        profile - will produce a system profile of some sort.
        show_profiles - shows all collected profiles in a readable format
        dump_profiles - dumps the provided profiles to a path

    and the following instance methods:
        new_profile_accumulator - produces a new accumulator that can be used to combine the
            profiles of partitions on a per stage basis
        add - adds a profile to the existing accumulated profile

    The profiler class is chosen when creating L{SparkContext}:
    """

    @staticmethod
    def profile(to_profile):
        """ Runs and profiles the method to_profile passed in. A profile object is returned. """
        pr = cProfile.Profile()
        pr.runcall(to_profile)
        st = pstats.Stats(pr)
        st.stream = None  # make it picklable
        st.strip_dirs()
        return st

    @staticmethod
    def show_profiles(profilers):
        """ Print the profile stats to stdout """
        for i, (id, profiler, showed) in enumerate(profilers):
            stats = profiler._accumulator.value
            if not showed and stats:
                print "=" * 60
                print "Profile of RDD<id=%d>" % id
                print "=" * 60
                stats.sort_stats("time", "cumulative").print_stats()
                # mark it as showed
                profilers[i][2] = True

    @staticmethod
    def dump_profiles(path, profilers):
        """ Dump the profilers stats into directory `path` """
        if not os.path.exists(path):
            os.makedirs(path)
        for id, profiler, _ in profilers:
            stats = profiler._accumulator.value
            if stats:
                p = os.path.join(path, "rdd_%d.pstats" % id)
                stats.dump_stats(p)

    def new_profile_accumulator(self, ctx):
        """
        Creates a new accumulator for combining the profiles of different
        partitions of a stage
        """
        self._accumulator = ctx.accumulator(None, PStatsParam)

    def add(self, accum_value):
        """ Adds a new profile to the existing accumulated value """
        self._accumulator.add(accum_value)
