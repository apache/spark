{
    "split_index": int,
    "python_version": str,
    "task_context": {
        "conn_info": str | int | None,
        "secret": str | None,
        "stage_id": int,
        "partition_id": int,
        "attempt_number": int,
        "task_attempt_id": int,
        "cpus": int,
        "resources": {"name": str, "addresses": str},
        "local_properties": {"key": str, "value": str},
    },
    "eval_type": int,
    "runner_conf": [{"key": str, "value": str}],
    "eval_conf": [{"key": str, "value": str}],
    "spark_files": {"spark_files_dir": str, "python_includes": [str]},
    "broadcasts": {
        "conn_info": str | int | None,
        "secret": str | None,
        "broadcast_variables": [
            {
                "bid": int,
                "path": str | None,
            }
        ],
    },
    "udfs": [
        {
            "args": [int],
            "kwargs": {"key": str, "value": int},
            "udf": {"func": bytes, "return_type": bytes},
            "result_id": int,
        }
    ],
}
