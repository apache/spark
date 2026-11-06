#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run with Java 22+ and local spark-submit from a build that includes PR #59424."""

import argparse
import json
from pathlib import Path
import tempfile

from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession


def committed_rows(root):
    manifest = json.loads((root / "manifest.json").read_text())
    assert manifest["version"] == 1, manifest
    result = []
    for entry in manifest["files"]:
        rows = [json.loads(line) for line in (root / entry["path"]).read_text().splitlines()]
        assert len(rows) == entry["rows"], entry
        result.extend((row["id"], row["label"]) for row in rows)
    return sorted(result)


def assert_clean(root):
    assert not (root / ".write-lock").exists(), "The operation lock was not released"
    staging = root / ".staging"
    assert not staging.exists() or not list(staging.iterdir()), "Staged files were not cleaned"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    package = args.package.resolve(strict=True)

    spark = (
        SparkSession.builder.appName("Rust native batch sink smoke")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.task.maxFailures", "1")
        .getOrCreate()
    )
    try:
        if not spark.sparkContext.master.startswith("local"):
            raise ValueError(
                "This smoke script uses driver-local temporary files; use --master local"
            )
        spark.addArtifact(str(package))

        def data(start, end):
            return spark.range(start, end, numPartitions=2).selectExpr(
                "id", "concat('row-', id) AS label"
            )

        with tempfile.TemporaryDirectory(prefix="rust-native-sink-") as directory:
            root = Path(directory)

            def write(frame, mode):
                frame.write.format("rust_sdk_sink").mode(mode).save(str(root))
                assert_clean(root)

            write(data(0, 5), "append")
            assert committed_rows(root) == [(i, f"row-{i}") for i in range(5)]
            write(data(5, 9), "append")
            assert committed_rows(root) == [(i, f"row-{i}") for i in range(9)]

            old_manifest = json.loads((root / "manifest.json").read_text())
            write(data(20, 23), "overwrite")
            assert committed_rows(root) == [(i, f"row-{i}") for i in range(20, 23)]
            assert all((root / entry["path"]).exists() for entry in old_manifest["files"])

            # No input shuffle: the failure happens inside the write job, after
            # the driver has created the coordinator and its staging directory.
            before = (root / "manifest.json").read_bytes()
            previous_operations = sorted(path.name for path in (root / "objects").iterdir())
            failing = spark.range(0, 8, numPartitions=2).selectExpr(
                "id",
                "if(id >= 4, cast(raise_error('intentional native sink input failure') AS STRING), "
                "concat('row-', id)) AS label",
            )
            try:
                failing.write.format("rust_sdk_sink").mode("overwrite").save(str(root))
            except Exception as error:
                assert "intentional native sink input failure" in str(error), str(error)
            else:
                raise AssertionError("The input error should abort the overwrite")
            assert (root / "manifest.json").read_bytes() == before
            assert sorted(path.name for path in (root / "objects").iterdir()) == previous_operations
            assert committed_rows(root) == [(i, f"row-{i}") for i in range(20, 23)]
            assert_clean(root)

            for mode in ("append", "overwrite"):
                try:
                    (
                        data(0, 1)
                        .write.format("rust_sdk_sink")
                        .mode(mode)
                        .option("readonly", "true")
                        .save(str(root))
                    )
                except AnalysisException as error:
                    assert error.getCondition() == "UNSUPPORTED_FEATURE.TABLE_OPERATION", str(error)
                else:
                    raise AssertionError(f"readonly=true should reject {mode}")
            assert (root / "manifest.json").read_bytes() == before
            assert_clean(root)

        # A streaming write records the epoch of each micro-batch that it commits: a restart
        # without new input commits nothing, and new input commits the next epoch.
        with tempfile.TemporaryDirectory(prefix="rust-native-sink-stream-") as directory:
            base = Path(directory)
            source, output = base / "input", base / "output"
            source.mkdir()

            def add_input(name, ids):
                lines = "".join(json.dumps({"id": i, "label": f"row-{i}"}) + "\n" for i in ids)
                (source / f"{name}.json").write_text(lines)

            def run_stream():
                query = (
                    spark.readStream.schema("id BIGINT, label STRING")
                    .json(str(source))
                    .writeStream.format("rust_sdk_sink")
                    .option("checkpointLocation", str(base / "checkpoint"))
                    .trigger(availableNow=True)
                    .start(str(output))
                )
                query.awaitTermination()
                assert_clean(output)
                return json.loads((output / "manifest.json").read_text())

            add_input("first", range(3))
            first = run_stream()
            assert list(first["epochs"].values()) == [0], first
            assert committed_rows(output) == [(i, f"row-{i}") for i in range(3)]
            assert run_stream() == first
            add_input("second", range(3, 5))
            second = run_stream()
            assert second["epochs"] == {query: 1 for query in first["epochs"]}, second
            assert committed_rows(output) == [(i, f"row-{i}") for i in range(5)]
        print("Rust native sink Spark smoke checks passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
