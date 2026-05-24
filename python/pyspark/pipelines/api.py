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
from typing import Callable, Dict, List, Literal, Optional, Union, overload

from pyspark.errors import PySparkTypeError
from pyspark.pipelines.graph_element_registry import get_active_graph_element_registry
from pyspark.pipelines.type_error_utils import validate_optional_list_of_str_arg
from pyspark.pipelines.flow import AutoCdcFlow, Flow, QueryFunction
from pyspark.pipelines.source_code_location import (
    get_caller_source_code_location,
)
from pyspark.pipelines.output import (
    MaterializedView,
    StreamingTable,
    TemporaryView,
    Sink,
)
from pyspark.sql import Column
from pyspark.sql.types import StructType


def append_flow(
    *,
    target: str,
    name: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
) -> Callable[[QueryFunction], None]:
    """
    Return a decorator on a query function to define a flow in a pipeline.

    :param name: The name of the flow. If unspecified, the query function's name will be used.
    :param target: The name of the dataset this flow writes to. Must be specified.
    :param spark_conf: A dict whose keys are the conf names and values are the conf values. \
        These confs will be set when the flow is executed; they can override confs set for the \
        destination, for the pipeline, or on the cluster.
    """
    if name is not None and type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )

    source_code_location = get_caller_source_code_location(stacklevel=1)

    if spark_conf is None:
        spark_conf = {}

    def outer(func: QueryFunction) -> None:
        query_name = name if name is not None else func.__name__
        flow = Flow(
            name=query_name,
            target=target,
            spark_conf=spark_conf,
            source_code_location=source_code_location,
            func=func,
        )
        get_active_graph_element_registry().register_flow(flow)

    return outer


def _validate_stored_dataset_args(
    name: Optional[str],
    table_properties: Optional[Dict[str, str]],
    partition_cols: Optional[List[str]],
    cluster_by: Optional[List[str]],
) -> None:
    if name is not None and type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )
    if table_properties is not None and not isinstance(table_properties, dict):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "dict",
                "arg_name": "table_properties",
                "arg_type": type(table_properties).__name__,
            },
        )
    validate_optional_list_of_str_arg(arg_name="partition_cols", arg_value=partition_cols)
    validate_optional_list_of_str_arg(arg_name="cluster_by", arg_value=cluster_by)


@overload
def table(query_function: QueryFunction) -> None: ...


@overload
def table(
    *,
    query_function: None = None,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[Union[StructType, str]] = None,
) -> Callable[[QueryFunction], None]: ...


def table(
    query_function: Optional[QueryFunction] = None,
    *,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[Union[StructType, str]] = None,
    format: Optional[str] = None,
) -> Union[Callable[[QueryFunction], None], None]:
    """
    (Return a) decorator to define a table in the pipeline and mark a function as the table's query
    function.

    @table can be used with or without parameters. If called without parameters, Python will
    implicitly pass the decorated query function as the query_function param. If called with
    parameters, @table will return a decorator that is applied on the decorated query function.

    :param query_function: The table's query function. This parameter should not be explicitly \
        passed by users. This is passed implicitly by Python if the decorator is called without \
        parameters.
    :param name: The name of the dataset. If unspecified, the query function's name will be used.
    :param comment: Description of the dataset.
    :param spark_conf: A dict whose keys are the conf names and values are the conf values. \
        These confs will be set when the query for the dataset is executed and they can override \
        confs set for the pipeline or on the cluster.
    :param table_properties: A dict where the keys are the property names and the values are the \
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param cluster_by: A list containing the column names of the cluster columns.
    :param schema: Explicit Spark SQL schema to materialize this table with. Supports either a \
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    :param format: The format of the table, e.g. "parquet".
    """
    _validate_stored_dataset_args(name, table_properties, partition_cols, cluster_by)

    source_code_location = get_caller_source_code_location(stacklevel=1)

    def outer(
        decorated: QueryFunction,
    ) -> None:
        _validate_decorated(decorated, "table")

        resolved_name = name or decorated.__name__
        registry = get_active_graph_element_registry()
        registry.register_output(
            StreamingTable(
                comment=comment,
                name=resolved_name,
                table_properties=table_properties or {},
                partition_cols=partition_cols,
                cluster_by=cluster_by,
                schema=schema,
                source_code_location=source_code_location,
                format=format,
            )
        )
        registry.register_flow(
            Flow(
                name=resolved_name,
                target=resolved_name,
                spark_conf=spark_conf or {},
                source_code_location=source_code_location,
                func=decorated,
            )
        )

    if query_function is not None:
        # Case where the decorator is called without parameters, e.g.:
        #   @table
        #   def query_fn():
        #     return ...

        outer(query_function)
        return None
    else:
        # Case where the decorator is called with parameters, e.g.:
        #   @table(name="tbl")
        #   def query_fn():
        #     return ...

        return outer


@overload
def materialized_view(query_function: QueryFunction) -> None: ...


@overload
def materialized_view(
    *,
    query_function: None = None,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[Union[StructType, str]] = None,
) -> Callable[[QueryFunction], None]: ...


def materialized_view(
    query_function: Optional[QueryFunction] = None,
    *,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[Union[StructType, str]] = None,
    format: Optional[str] = None,
) -> Union[Callable[[QueryFunction], None], None]:
    """
    (Return a) decorator to define a materialized view in the pipeline and mark a function as the
    materialized view's query function.

    @materialized_view can be used with or without parameters. If called without parameters, Python
    will implicitly pass the decorated query function as the query_function param. If called with
    parameters, it will return a decorator that is applied on the decorated query function.

    :param query_function: The table's query function. This parameter should not be explicitly \
        passed by users. This is passed implicitly by Python if the decorator is called without \
        parameters.
    :param name: The name of the dataset. If unspecified, the query function's name will be used.
    :param comment: Description of the dataset.
    :param spark_conf: A dict whose keys are the conf names and values are the conf values. \
        These confs will be set when the query for the dataset is executed and they can override \
        confs set for the pipeline or on the cluster.
    :param table_properties: A dict where the keys are the property names and the values are the \
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param cluster_by: A list containing the column names of the cluster columns.
    :param schema: Explicit Spark SQL schema to materialize this table with. Supports either a \
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    :param format: The format of the table, e.g. "parquet".
    """
    _validate_stored_dataset_args(name, table_properties, partition_cols, cluster_by)

    source_code_location = get_caller_source_code_location(stacklevel=1)

    def outer(
        decorated: QueryFunction,
    ) -> None:
        _validate_decorated(decorated, "materialized_view")

        resolved_name = name or decorated.__name__
        registry = get_active_graph_element_registry()
        registry.register_output(
            MaterializedView(
                comment=comment,
                name=resolved_name,
                table_properties=table_properties or {},
                partition_cols=partition_cols,
                cluster_by=cluster_by,
                schema=schema,
                source_code_location=source_code_location,
                format=format,
            )
        )
        registry.register_flow(
            Flow(
                name=resolved_name,
                target=resolved_name,
                spark_conf=spark_conf or {},
                source_code_location=source_code_location,
                func=decorated,
            )
        )

    if query_function is not None:
        # Case where the decorator is called without parameters, e.g.:
        #   @materialized_view
        #   def query_fn():
        #     return ...

        outer(query_function)
        return None
    else:
        # Case where the decorator is called with parameters, e.g.:
        #   @materialized_view(name="tbl")
        #   def query_fn():
        #     return ...

        return outer


@overload
def temporary_view(
    query_function: QueryFunction,
) -> None: ...


@overload
def temporary_view(
    *,
    query_function: None = None,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
) -> Callable[[QueryFunction], None]: ...


def temporary_view(
    query_function: Optional[QueryFunction] = None,
    *,
    name: Optional[str] = None,
    comment: Optional[str] = None,
    spark_conf: Optional[Dict[str, str]] = None,
) -> Union[Callable[[QueryFunction], None], None]:
    """
    (Return a) decorator to define a view in the pipeline and mark a function as the view's query
    function.

    @view can be used with or without parameters. If called without parameters, Python will
    implicitly pass the decorated query function as the query_function param. If called with
    parameters, @view will return a decorator that is applied on the decorated query function.

    :param query_function: The view's query function. This parameter should not be explicitly \
        passed by users. This is passed implicitly by Python if the decorator is called without \
        parameters.
    :param name: The name of the dataset. If unspecified, the query function's name will be used.
    :param comment: Description of the dataset.
    :param spark_conf: A dict whose keys are the conf names and values are the conf values. \
        These confs will be set when the query for the dataset is executed and they can override \
        confs set for the pipeline or on the cluster.
    """
    if name is not None and type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )

    source_code_location = get_caller_source_code_location(stacklevel=1)

    def outer(decorated: QueryFunction) -> None:
        _validate_decorated(decorated, "temporary_view")

        resolved_name = name or decorated.__name__
        registry = get_active_graph_element_registry()
        registry.register_output(
            TemporaryView(
                comment=comment,
                name=resolved_name,
                source_code_location=source_code_location,
            )
        )
        registry.register_flow(
            Flow(
                target=resolved_name,
                func=decorated,
                spark_conf=spark_conf or {},
                name=resolved_name,
                source_code_location=source_code_location,
            )
        )

    if query_function is not None:
        # Case where the decorator is called without parameters, e.g.:
        #   @temporary_view
        #   def query_fn():
        #     return ...

        outer(query_function)
        return None
    else:
        # Case where the decorator is called with parameters, e.g.:
        #   @temporary_view(name="tbl")
        #   def query_fn():
        #     return ...

        return outer


def _validate_decorated(decorated: QueryFunction, decorator_name: str) -> None:
    if not callable(decorated):
        raise PySparkTypeError(
            errorClass="DECORATOR_ARGUMENT_NOT_CALLABLE",
            messageParameters={
                "decorator_name": decorator_name,
                "example_usage": f"@{decorator_name}(name='{decorator_name}_a')",
            },
        )


def create_streaming_table(
    name: str,
    *,
    comment: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
    partition_cols: Optional[List[str]] = None,
    cluster_by: Optional[List[str]] = None,
    schema: Optional[Union[StructType, str]] = None,
    format: Optional[str] = None,
) -> None:
    """
    Creates a table that can be targeted by append flows.

    Example:
        create_streaming_table("target")

    :param name: The name of the table.
    :param comment: Description of the table.
    :param table_properties: A dict where the keys are the property names and the values are the \
        property values. These properties will be set on the table.
    :param partition_cols: A list containing the column names of the partition columns.
    :param cluster_by: A list containing the column names of the cluster columns.
    :param schema: Explicit Spark SQL schema to materialize this table with. Supports either a \
        Pyspark StructType or a SQL DDL string, such as "a INT, b STRING".
    :param format: The format of the table, e.g. "parquet".
    """
    if type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )
    if table_properties is not None and not isinstance(table_properties, dict):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "dict",
                "arg_name": "table_properties",
                "arg_type": type(table_properties).__name__,
            },
        )
    validate_optional_list_of_str_arg(arg_name="partition_cols", arg_value=partition_cols)
    validate_optional_list_of_str_arg(arg_name="cluster_by", arg_value=cluster_by)

    source_code_location = get_caller_source_code_location(stacklevel=1)

    table = StreamingTable(
        name=name,
        comment=comment,
        source_code_location=source_code_location,
        table_properties=table_properties or {},
        partition_cols=partition_cols,
        cluster_by=cluster_by,
        schema=schema,
        format=format,
    )
    get_active_graph_element_registry().register_output(table)


def create_sink(
    name: str,
    format: str,
    options: Optional[Dict[str, str]] = None,
) -> None:
    """
    Creates a sink that can be targeted by streaming flows, providing a generic destination
    for flows to send data external to the pipeline.

    :param name: The name of the sink.
    :param format: The format of the sink, e.g. "parquet".
    :param options: A dict where the keys are the property names and the values are the
        property values. These properties will be set on the sink.
    """
    if type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )
    if type(format) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "format",
                "expected_type": "str",
                "arg_type": type(format).__name__,
            },
        )
    if options is not None and not isinstance(options, dict):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "expected_type": "dict",
                "arg_name": "options",
                "arg_type": type(options).__name__,
            },
        )
    sink = Sink(
        name=name,
        format=format,
        options=options or {},
        source_code_location=get_caller_source_code_location(stacklevel=1),
        comment=None,
    )
    get_active_graph_element_registry().register_output(sink)


def create_auto_cdc_flow(
    target: str,
    source: str,
    keys: Union[List[str], List[Column]],
    sequence_by: Union[str, Column],
    apply_as_deletes: Optional[Union[str, Column]] = None,
    column_list: Optional[Union[List[str], List[Column]]] = None,
    except_column_list: Optional[Union[List[str], List[Column]]] = None,
    stored_as_scd_type: Optional[Literal[1, "1"]] = None,
    name: Optional[str] = None,
) -> None:
    """
    Create an Auto CDC flow into the target table from the Change Data Capture (CDC) source.
    Target table must have already been created using create_streaming_table function. Only one
    of column_list and except_column_list can be specified.

    Example:
        create_auto_cdc_flow(
            target="target",
            source="source",
            keys=["key"],
            sequence_by="sequence_expr",
            column_list=["key", "value"],
        )

    Note that for keys, sequence_by, column_list, and except_column_list the arguments have to
    be column identifiers without qualifiers, e.g. they cannot be col("sourceTable.keyId").

    :param target: The name of the target table that receives the Auto CDC flow.
    :param source: The name of the CDC source to stream from.
    :param keys: The column or combination of columns that uniquely identify a row in the source \
        data. This is used to identify which CDC events apply to specific records in the target \
        table. These keys also identify records in the target table, e.g., if there exists a record \
        for given keys and the CDC source has an UPSERT operation for the same keys, we will update \
        the existing record. At least one key must be provided. This should be a list of column \
        identifiers without qualifiers, expressed as either Python strings or PySpark Columns.
    :param sequence_by: An expression that we use to order the source data. This can be expressed \
        as either a SQL expression string or a PySpark Column.
    :param apply_as_deletes: Delete condition for the merged operation. This should be a string of \
        expression e.g. "operation = 'DELETE'"
    :param column_list: Columns that will be included in the output table. This should be a list \
        of column identifiers without qualifiers, expressed as either Python strings or PySpark \
        Column. Only one of column_list and except_column_list can be specified.
    :param except_column_list: Columns that will be excluded in the output table. This should be a \
        list of column identifiers without qualifiers, expressed as either Python strings or \
        PySpark Column. Only one of column_list and except_column_list can be specified. When this \
        is specified, all columns in the dataframe of the target table except those in this list \
        will be in the output table.
    :param stored_as_scd_type: The SCD type for the target table. Only 1 (or "1") is supported. \
        When not specified the server default applies.
    :param name: The name of the flow for this create_auto_cdc_flow command. When unspecified \
        this will build a "default flow" with name equal to the target name.
    """
    from pyspark.sql.connect.functions.builtin import expr as _connect_expr

    if type(target) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "target",
                "expected_type": "str",
                "arg_type": type(target).__name__,
            },
        )
    if type(source) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "source",
                "expected_type": "str",
                "arg_type": type(source).__name__,
            },
        )
    if name is not None and type(name) is not str:
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "name",
                "expected_type": "str",
                "arg_type": type(name).__name__,
            },
        )

    if name is None:
        name = target

    keys = _normalize_column_list(arg_name="keys", column_list=keys)
    column_list = _normalize_optional_column_list(arg_name="column_list", column_list=column_list)
    except_column_list = _normalize_optional_column_list(
        arg_name="except_column_list", column_list=except_column_list
    )

    if isinstance(sequence_by, str):
        sequence_by = _connect_expr(sequence_by)
    elif not isinstance(sequence_by, Column):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "sequence_by",
                "expected_type": "str or Column",
                "arg_type": type(sequence_by).__name__,
            },
        )

    if isinstance(apply_as_deletes, str):
        apply_as_deletes = _connect_expr(apply_as_deletes)
    elif apply_as_deletes is not None and not isinstance(apply_as_deletes, Column):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "apply_as_deletes",
                "expected_type": "str or Column",
                "arg_type": type(apply_as_deletes).__name__,
            },
        )

    if stored_as_scd_type is not None and str(stored_as_scd_type) != "1":
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": "stored_as_scd_type",
                "expected_type": "Literal[1, '1']",
                "arg_type": type(stored_as_scd_type).__name__,
            },
        )

    source_code_location = get_caller_source_code_location(stacklevel=1)

    flow = AutoCdcFlow(
        name=name,
        target=target,
        source=source,
        keys=keys,
        sequence_by=sequence_by,
        apply_as_deletes=apply_as_deletes,
        column_list=column_list,
        except_column_list=except_column_list,
        stored_as_scd_type=stored_as_scd_type,
        source_code_location=source_code_location,
    )

    get_active_graph_element_registry().register_auto_cdc_flow(flow)


def _normalize_optional_column_list(
    arg_name: str,
    column_list: Optional[Union[List[str], List[Column]]],
) -> Optional[List[Column]]:
    if column_list is None:
        return None
    return _normalize_column_list(arg_name=arg_name, column_list=column_list)


def _normalize_column_list(
    arg_name: str,
    column_list: Union[List[str], List[Column]],
) -> List[Column]:
    from pyspark.sql.connect.functions.builtin import col as _connect_col

    if not isinstance(column_list, list):
        raise PySparkTypeError(
            errorClass="NOT_EXPECTED_TYPE",
            messageParameters={
                "arg_name": arg_name,
                "expected_type": "list[str] or list[Column]",
                "arg_type": type(column_list).__name__,
            },
        )

    normalized: List[Column] = []

    for column in column_list:
        if isinstance(column, str):
            normalized.append(_connect_col(column))
        elif isinstance(column, Column):
            normalized.append(column)
        else:
            raise PySparkTypeError(
                errorClass="NOT_EXPECTED_TYPE",
                messageParameters={
                    "arg_name": arg_name,
                    "expected_type": "list[str] or list[Column]",
                    "arg_type": type(column).__name__,
                },
            )

    return normalized
