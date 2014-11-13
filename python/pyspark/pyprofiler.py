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
PySpark supports custom profilers, this is to allow for different profilers to
be used as well as outputting to different formats than what is provided in the
BasicProfiler.

The profiler class is chosen when creating L{SparkContext}:
NOTE: This has no effect if `spark.python.profile` is not set.
>>> from pyspark.context import SparkContext
>>> from pyspark.serializers import MarshalSerializer
>>> sc = SparkContext('local', 'test', profiler=MyCustomProfiler)
>>> sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10)
[0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
>>> sc.show_profiles()
>>> sc.stop()

"""


import cProfile
import pstats
import os
from pyspark.accumulators import PStatsParam


class BasicProfiler(object):

    @staticmethod
    def profile(to_profile):
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
        self._accumulator = ctx.accumulator(None, PStatsParam)

    def add(self, accum_value):
        self._accumulator.add(accum_value)
