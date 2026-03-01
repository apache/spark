from pyspark.sql import SparkSession
from pyspark import pipelines as dp
from pyspark.pipelines.spark_connect_graph_element_registry import (
    SparkConnectGraphElementRegistry,
)
from pyspark.pipelines.spark_connect_pipeline import create_dataflow_graph
from pyspark.pipelines.spark_connect_pipeline import start_run, handle_pipeline_events
import threading

def setup(server_port: str, session_identifier: str) -> tuple[SparkSession, SparkConnectGraphElementRegistry]:
    spark = SparkSession.builder \
        .remote(f"sc://localhost:{server_port}") \
        .config("spark.connect.grpc.channel.timeout", "5s") \
        .config("spark.custom.identifier", session_identifier) \
        .create()

    dataflow_graph_id = create_dataflow_graph(
        spark,
        default_catalog=None,
        default_database=None,
        sql_conf={},
    )

    registry = SparkConnectGraphElementRegistry(spark, dataflow_graph_id)
    return spark, registry


def run_and_handle_signals(
    spark: SparkSession,
    registry: SparkConnectGraphElementRegistry,
    storage_root: str,
) -> None:
    result_iter = start_run(
        spark,
        dataflow_graph_id=registry.dataflow_graph_id,
        full_refresh=None,
        full_refresh_all=False,
        refresh=None,
        dry=True,
        storage=storage_root
    )
    thread = threading.Thread(target=handle_pipeline_events, args=(result_iter,), daemon=True)
    thread.start()

    registry.register_signalled_query_functions()
