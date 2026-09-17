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

from pyspark.sql.tests.test_python_worker_env import PREFIX, PythonWorkerEnvMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class PythonWorkerEnvParityTests(PythonWorkerEnvMixin, ReusedConnectTestCase):
    # Write-time rejection is shared behaviour now that it lives in `RuntimeConfig.set`, so the
    # mixin covers it. What remains Connect-specific is the read-back asymmetry below: the config
    # RPC redacts on every read, while classic returns the value.

    def test_a_secret_looking_name_cannot_be_read_back(self):
        # The config RPC withholds any value whose key matches spark.redaction.regex, and the
        # default pattern covers "token". So a variable named like a secret still reaches the
        # worker but reads back as absent -- unlike classic, which returns it. Pinned here so the
        # asymmetry is a recorded decision rather than a surprise.
        self.spark.conf.set(PREFIX + "API_TOKEN", "abc")
        try:
            self.assertIsNone(self.spark.conf.get(PREFIX + "API_TOKEN", None))
            self.assertEqual(self._read_in_worker("API_TOKEN"), "abc")
        finally:
            self.spark.conf.unset(PREFIX + "API_TOKEN")

    def test_env_var_reaches_a_foreach_batch_worker(self):
        """`foreachBatch` runs in a worker started by the server for the whole query.

        The worker cannot return a value, so it records what it observed in a file. A file source
        with `availableNow` keeps the query bounded rather than relying on wall-clock timing.
        """
        import os
        import shutil
        import tempfile

        source_dir = tempfile.mkdtemp()
        observed_dir = tempfile.mkdtemp()
        self._set_env("BATCH_SETTING", "batched")
        try:
            with open(os.path.join(source_dir, "input.txt"), "w") as handle:
                handle.write("a row\n")

            def record(batch_df, batch_id):
                import os

                path = os.path.join(observed_dir, "observed-{}.txt".format(batch_id))
                with open(path, "w") as out:
                    out.write(os.environ.get("BATCH_SETTING", "<unset>"))

            query = (
                self.spark.readStream.format("text")
                .load(source_dir)
                .writeStream.foreachBatch(record)
                .trigger(availableNow=True)
                .start()
            )
            try:
                query.awaitTermination(timeout=120)
            finally:
                query.stop()

            observed = sorted(os.listdir(observed_dir))
            self.assertNotEqual(observed, [], "foreachBatch never ran a batch")
            with open(os.path.join(observed_dir, observed[0])) as handle:
                self.assertEqual(handle.read(), "batched")
        finally:
            self._unset_env("BATCH_SETTING")
            shutil.rmtree(source_dir, ignore_errors=True)
            shutil.rmtree(observed_dir, ignore_errors=True)

    # There is deliberately no streaming-listener test here. PySpark's `addListener` appends to a
    # client-side `StreamingQueryListenerBus`: the server streams events back and the callback runs
    # in the client process, not in a worker Spark launched, so no session environment applies to
    # it. The server-side `PythonStreamingQueryListener` does launch a worker and does receive the
    # environment, but it is reached only by the `add_listener` command that sends a pickled
    # listener to the server, which PySpark no longer uses -- so it cannot be driven from here.


if __name__ == "__main__":
    from pyspark.testing import main

    main()
