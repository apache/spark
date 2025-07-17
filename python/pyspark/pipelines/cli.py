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
Implementation of spark-pipelines CLI.

Example usage:
    $ bin/spark-pipelines run --spec /path/to/pipeline.yaml
"""
from contextlib import contextmanager
import argparse
import importlib.util
import os
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, List, Mapping, Optional, Sequence

from pyspark.errors import PySparkException, PySparkTypeError
from pyspark.sql import SparkSession
from pyspark.pipelines.graph_element_registry import (
    graph_element_registration_context,
    GraphElementRegistry,
)
from pyspark.pipelines.init_cli import init
from pyspark.pipelines.logging_utils import log_with_curr_timestamp
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import (
    create_dataflow_graph,
    start_run,
    handle_pipeline_events,
)

PIPELINE_SPEC_FILE_NAMES = ["pipeline.yaml", "pipeline.yml"]


@dataclass(frozen=True)
class DefinitionsGlob:
    """A glob pattern for finding pipeline definitions files."""

    include: str


@dataclass(frozen=True)
class PipelineSpec:
    """Spec for a pipeline.

    :param name: The name of the pipeline.
    :param catalog: The default catalog to use for the pipeline.
    :param database: The default database to use for the pipeline.
    :param configuration: A dictionary of Spark configuration properties to set for the pipeline.
    :param definitions: A list of glob patterns for finding pipeline definitions files.
    """

    name: str
    catalog: Optional[str]
    database: Optional[str]
    configuration: Mapping[str, str]
    definitions: Sequence[DefinitionsGlob]


def find_pipeline_spec(current_dir: Path) -> Path:
    """Looks in the current directory and its ancestors for a pipeline spec file."""
    while True:
        try:
            candidates = [
                current_dir / spec_file_name for spec_file_name in PIPELINE_SPEC_FILE_NAMES
            ]
            found_files = [candidate for candidate in candidates if candidate.is_file()]
            if len(found_files) == 1:
                return found_files[0]
            elif len(found_files) > 1:
                raise PySparkException(
                    errorClass="MULTIPLE_PIPELINE_SPEC_FILES_FOUND",
                    messageParameters={"dir_path": str(current_dir)},
                )
        except PermissionError:
            raise PySparkException(
                errorClass="PIPELINE_SPEC_FILE_NOT_FOUND",
                messageParameters={"dir_path": str(current_dir)},
            )

        if current_dir.parent == current_dir or not current_dir.parent.exists():
            raise PySparkException(
                errorClass="PIPELINE_SPEC_FILE_NOT_FOUND",
                messageParameters={"dir_path": str(current_dir)},
            )

        current_dir = current_dir.parent


def load_pipeline_spec(spec_path: Path) -> PipelineSpec:
    """Load the pipeline spec from a YAML file at the given path."""
    with spec_path.open("r") as f:
        return unpack_pipeline_spec(yaml.safe_load(f))


def unpack_pipeline_spec(spec_data: Mapping[str, Any]) -> PipelineSpec:
    ALLOWED_FIELDS = {"name", "catalog", "database", "schema", "configuration", "definitions"}
    REQUIRED_FIELDS = ["name"]
    for key in spec_data.keys():
        if key not in ALLOWED_FIELDS:
            raise PySparkException(
                errorClass="PIPELINE_SPEC_UNEXPECTED_FIELD", messageParameters={"field_name": key}
            )

    for key in REQUIRED_FIELDS:
        if key not in spec_data:
            raise PySparkException(
                errorClass="PIPELINE_SPEC_MISSING_REQUIRED_FIELD",
                messageParameters={"field_name": key},
            )

    return PipelineSpec(
        name=spec_data["name"],
        catalog=spec_data.get("catalog"),
        database=spec_data.get("database", spec_data.get("schema")),
        configuration=validate_str_dict(spec_data.get("configuration", {}), "configuration"),
        definitions=[
            DefinitionsGlob(include=entry["glob"]["include"])
            for entry in spec_data.get("definitions", [])
        ],
    )


def validate_str_dict(d: Mapping[str, str], field_name: str) -> Mapping[str, str]:
    """Raises an error if the dictionary is not a mapping of strings to strings."""
    if not isinstance(d, dict):
        raise PySparkTypeError(
            errorClass="PIPELINE_SPEC_FIELD_NOT_DICT",
            messageParameters={"field_name": field_name, "field_type": type(d).__name__},
        )

    for key, value in d.items():
        if not isinstance(key, str):
            raise PySparkTypeError(
                errorClass="PIPELINE_SPEC_DICT_KEY_NOT_STRING",
                messageParameters={"field_name": field_name, "key_type": type(key).__name__},
            )
        if not isinstance(value, str):
            raise PySparkTypeError(
                errorClass="PIPELINE_SPEC_DICT_VALUE_NOT_STRING",
                messageParameters={
                    "field_name": field_name,
                    "key_name": key,
                    "value_type": type(value).__name__,
                },
            )

    return d


def register_definitions(
    spec_path: Path, registry: GraphElementRegistry, spec: PipelineSpec
) -> None:
    """Register the graph element definitions in the pipeline spec with the given registry.
    - Looks for Python files matching the glob patterns in the spec and imports them.
    - Looks for SQL files matching the blob patterns in the spec and registers thems.
    """
    path = spec_path.parent
    with change_dir(path):
        with graph_element_registration_context(registry):
            log_with_curr_timestamp(f"Loading definitions. Root directory: '{path}'.")
            for definition_glob in spec.definitions:
                glob_expression = definition_glob.include
                matching_files = [p for p in path.glob(glob_expression) if p.is_file()]
                log_with_curr_timestamp(
                    f"Found {len(matching_files)} files matching glob '{glob_expression}'"
                )
                for file in matching_files:
                    if file.suffix == ".py":
                        log_with_curr_timestamp(f"Importing {file}...")
                        module_spec = importlib.util.spec_from_file_location(file.stem, str(file))
                        assert module_spec is not None, f"Could not find module spec for {file}"
                        module = importlib.util.module_from_spec(module_spec)
                        assert (
                            module_spec.loader is not None
                        ), f"Module spec has no loader for {file}"
                        module_spec.loader.exec_module(module)
                    elif file.suffix == ".sql":
                        log_with_curr_timestamp(f"Registering SQL file {file}...")
                        with file.open("r") as f:
                            sql = f.read()
                        file_path_relative_to_spec = file.relative_to(spec_path.parent)
                        registry.register_sql(sql, file_path_relative_to_spec)
                    else:
                        raise PySparkException(
                            errorClass="PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION",
                            messageParameters={"file_path": str(file)},
                        )


@contextmanager
def change_dir(path: Path) -> Generator[None, None, None]:
    """Change the current working directory to the given path and restore it on close()."""
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def run(
    spec_path: Path,
    full_refresh: Sequence[str],
    full_refresh_all: bool,
    refresh: Sequence[str],
    dry: bool,
) -> None:
    """Run the pipeline defined with the given spec.

    :param spec_path: Path to the pipeline specification file.
    :param full_refresh: List of datasets to reset and recompute.
    :param full_refresh_all: Perform a full graph reset and recompute.
    :param refresh: List of datasets to update.
    """
    # Validate conflicting arguments
    if full_refresh_all:
        if full_refresh:
            raise PySparkException(
                errorClass="CONFLICTING_PIPELINE_REFRESH_OPTIONS",
                messageParameters={
                    "conflicting_option": "--full_refresh",
                },
            )
        if refresh:
            raise PySparkException(
                errorClass="CONFLICTING_PIPELINE_REFRESH_OPTIONS",
                messageParameters={
                    "conflicting_option": "--refresh",
                },
            )

    log_with_curr_timestamp(f"Loading pipeline spec from {spec_path}...")
    spec = load_pipeline_spec(spec_path)

    log_with_curr_timestamp("Creating Spark session...")
    spark_builder = SparkSession.builder
    for key, value in spec.configuration.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()

    log_with_curr_timestamp("Creating dataflow graph...")
    dataflow_graph_id = create_dataflow_graph(
        spark,
        default_catalog=spec.catalog,
        default_database=spec.database,
        sql_conf=spec.configuration,
    )

    log_with_curr_timestamp("Registering graph elements...")
    registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
    register_definitions(spec_path, registry, spec)

    log_with_curr_timestamp("Starting run...")
    result_iter = start_run(
        spark,
        dataflow_graph_id,
        full_refresh=full_refresh,
        full_refresh_all=full_refresh_all,
        refresh=refresh,
        dry=dry,
    )
    try:
        handle_pipeline_events(result_iter)
    finally:
        spark.stop()


def parse_table_list(value: str) -> List[str]:
    """Parse a comma-separated list of table names, handling whitespace."""
    return [table.strip() for table in value.split(",") if table.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # "run" subcommand
    run_parser = subparsers.add_parser(
        "run",
        help="Run a pipeline. If no refresh options specified, "
        "a default incremental update is performed.",
    )
    run_parser.add_argument("--spec", help="Path to the pipeline spec.")
    run_parser.add_argument(
        "--full-refresh",
        type=parse_table_list,
        action="extend",
        help="List of datasets to reset and recompute (comma-separated).",
        default=[],
    )
    run_parser.add_argument(
        "--full-refresh-all", action="store_true", help="Perform a full graph reset and recompute."
    )
    run_parser.add_argument(
        "--refresh",
        type=parse_table_list,
        action="extend",
        help="List of datasets to update (comma-separated).",
        default=[],
    )

    # "dry-run" subcommand
    dry_run_parser = subparsers.add_parser(
        "dry-run",
        help="Launch a run that just validates the graph and checks for errors.",
    )
    dry_run_parser.add_argument("--spec", help="Path to the pipeline spec.")

    # "init" subcommand
    init_parser = subparsers.add_parser(
        "init",
        help="Generates a simple pipeline project, including a spec file and example definitions.",
    )
    init_parser.add_argument(
        "--name",
        help="Name of the project. A directory with this name will be created underneath the "
        "current directory.",
        required=True,
    )

    args = parser.parse_args()
    assert args.command in ["run", "dry-run", "init"]

    if args.command in ["run", "dry-run"]:
        if args.spec is not None:
            spec_path = Path(args.spec)
            if not spec_path.is_file():
                raise PySparkException(
                    errorClass="PIPELINE_SPEC_FILE_DOES_NOT_EXIST",
                    messageParameters={"spec_path": args.spec},
                )
        else:
            spec_path = find_pipeline_spec(Path.cwd())

        if args.command == "run":
            run(
                spec_path=spec_path,
                full_refresh=args.full_refresh,
                full_refresh_all=args.full_refresh_all,
                refresh=args.refresh,
                dry=args.command == "dry-run",
            )
        else:
            assert args.command == "dry-run"
            run(
                spec_path=spec_path,
                full_refresh=[],
                full_refresh_all=False,
                refresh=[],
                dry=True,
            )
    elif args.command == "init":
        init(args.name)
