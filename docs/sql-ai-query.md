---
layout: global
title: Experimental AI query prototype
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

This prototype queries Databricks or Amazon Bedrock from a DataFrame through an iterator Pandas
UDF. Register a model alias with `register_model` and use that alias in `ai_query`.
`register_sql_function` makes the same execution path callable from SQL text. This is not a
native SQL function yet -- see [SQL text](#sql-text) for what that means in practice. Spark
Connect and streaming are unvalidated.

## Prerequisites

Install Pandas UDF dependencies (`pandas` and `pyarrow`) on the Python workers. The provider SDK
resolves credentials on each worker using the environment, workload identity, or a named profile.

* Databricks requires `databricks-sdk` and `requests`, a chat serving endpoint, and OAuth access.
  The default authentication is `oauth-m2m`. Local workers can use an authenticated CLI profile
  with `auth_type="databricks-cli"`.
* Bedrock requires `boto3` with Converse support and an AWS identity with `bedrock:InvokeModel`
  permission for the model or inference profile. The AWS SDK resolves the region and credentials.
  Use `region` and `profile` to select a specific configuration.

## Query a foundation model directly

```python
from pyspark.sql.ai import ai_query, register_model
from pyspark.sql.functions import col, concat, lit

register_model(
    "databricks-summary",
    "databricks-gpt-oss-20b",
    provider="databricks",
    profile="my-workspace",
    auth_type="databricks-cli",
)
register_model(
    "bedrock-summary",
    "amazon.nova-micro-v1:0",
    provider="bedrock",
    region="us-east-1",
)

documents = spark.createDataFrame([("A document to summarize.",)], ["description"])
prompt = concat(lit("Summarize in one sentence: "), col("description"))
result = documents.select(
    ai_query("databricks-summary", prompt).alias("databricks_summary"),
    ai_query("bedrock-summary", prompt).alias("bedrock_summary"),
)
```

Each provider receives a chat message per non-null request and returns text. Both accept
`max_tokens` and `temperature`. Null requests produce null.

## SQL text

```python
from pyspark.sql.ai import register_sql_function

register_sql_function()
documents.createOrReplaceTempView("documents")
spark.sql(
    "SELECT ai_query('databricks-summary', description) AS summary FROM documents"
).show()
```

`register_sql_function` registers a SQL function (`ai_query` by default) backed by the exact
same registry, batching, retries, and provider clients as the Column API above -- SQL and
`ai_query()` execute through identical code, not two implementations. The endpoint can also be
a column rather than a literal, so one query can route different rows to different registered
models.

This takes a snapshot of the aliases registered so far. A `register_model` call after
`register_sql_function` does not change it; call `register_sql_function` again to pick up
later registrations.

This is not a native SQL function like `from_json`, which resolves and validates its schema
argument during query analysis, before anything runs. Here, because a SQL argument is a
column value rather than a Python-side parameter, the alias is resolved inside the UDF
instead, once per distinct value in each Arrow batch. An unregistered alias therefore fails
once a task actually executes it, not when the query is analyzed. Making this validate at
analysis time, the way `from_json` does, needs a Catalyst-level piece this prototype does not
have yet; that is an open design question, not something decided here.

## Execution and retries

Aliases belong to a Spark session. Registration requires an active or default session.
Use `session=spark` with both `register_model` and `ai_query` to select another session.
Each column captures the registration available at construction time. Invocation errors fail the task by default. Set
`on_error="null"` to return null for known inference and transient failures. Invalid input,
client initialization, authentication, and permission errors still fail the task.

Concurrency is bounded per active Python task. For example, 20 tasks with
`max_concurrency=4` can issue up to 80 concurrent requests. The worker invokes the model
for each non-null row in an Arrow batch before yielding the batch. A preview such as
`show(1)` can therefore invoke the model for more rows than it displays. Apply a limit
to the input before adding the inference column when previewing a workload.

Spark can repeat billable calls when retrying a failed task. Persist results to reuse them
across downstream jobs.

For Databricks, `timeout` is a per-row budget across authentication, HTTP attempts, and
retry delays. Each HTTP attempt uses the budget remaining after authentication. Successful
responses received after expiry are rejected with `AI_QUERY_INVOCATION_FAILED.TIMEOUT`,
which produces null with `on_error="null"`. Authentication and in-progress response downloads
can delay timeout reporting beyond the budget. The HTTP timeout limits connection and read
inactivity, so a response that keeps delivering bytes can continue downloading past expiry.
Bedrock uses the SDK's connection and read timeouts.

## Validation

The suite checks registration, authentication, HTTP errors and retries, response parsing,
concurrency, nulls, and cleanup. An integration test exercises the DataFrame API through
a Spark Python worker and Arrow against a local server. The suite runs without cloud credentials.

```sh
python/run-tests --testnames pyspark.sql.tests.test_ai_query
```

Run the opt-in live check with these prototype modules installed in both the driver and worker
Python packages. For a pip-installed Spark distribution, the worker package is `python/lib/pyspark.zip`
under the installed `pyspark` package. Use an isolated Spark installation for this check.

```sh
python dev/ai_query_e2e.py \
  --provider databricks \
  --profile my-workspace \
  --auth-type databricks-cli \
  --model-id databricks-gpt-oss-20b \
  --report /tmp/ai-query-e2e.json

python dev/ai_query_e2e.py \
  --provider bedrock \
  --region us-east-1 \
  --model-id amazon.nova-micro-v1:0 \
  --report /tmp/ai-query-bedrock-e2e.json
```

The check launches Spark locally, asserts an `ArrowEvalPython` plan, and verifies the responses
for two synthetic prompts and one null.

## References

* [Databricks Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/)
* [Amazon Bedrock Converse](https://docs.aws.amazon.com/boto3/latest/reference/services/bedrock-runtime/client/converse.html)
* [OAuth M2M authentication](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)
