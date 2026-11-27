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

This prototype invokes Databricks or Amazon Bedrock models through an iterator Pandas UDF.
Register a model with `register_model`, then call `ai_query` from a DataFrame or register it
for SQL with `register_sql_function`. Spark Connect and streaming are unvalidated.

## Prerequisites

Install the wheel built from this branch with its `ai` extra on the driver and workers:
`pip install "path/to/pyspark-<version>-py3-none-any.whl[ai]"`. The extra includes Pandas UDF
dependencies and the provider SDKs. Workers resolve credentials through the environment,
workload identity, or a named profile.

* Databricks requires `databricks-sdk` and `requests`, a chat serving endpoint, and OAuth access.
  The default authentication is `oauth-m2m`. Local workers can use an authenticated CLI profile
  with `auth_type="databricks-cli"`.
* Bedrock requires `boto3` with Converse support and an AWS identity with `bedrock:InvokeModel`
  permission for the model or inference profile. The AWS SDK resolves the region and credentials.
  Use `region` and `profile` to select a specific configuration.

## Query a foundation model directly

The runnable example accepts the provider, model, and credential profile as command-line
arguments and creates a DataFrame of synthetic prompts.

{% include_example imports python/sql/ai_query.py %}

{% include_example registration python/sql/ai_query.py %}

{% include_example column python/sql/ai_query.py %}

Each provider receives a chat message per non-null request and returns text. Both accept
`max_tokens` and `temperature`. Providers may return partial text when `max_tokens` is reached.
Null requests produce null.

## SQL text

{% include_example sql python/sql/ai_query.py %}

The endpoint argument can be a literal or a column, allowing rows to use different models.
With a column endpoint, row values select the provider, credential profile, and region from
the aliases captured at registration. Restrict that set with
`register_sql_function(aliases=["summarizer"])`; unknown names fail at registration.
An empty list exposes no models. Use a literal endpoint for a fixed destination.

This takes a snapshot of the aliases registered so far. A `register_model` call after
`register_sql_function` does not change it; call `register_sql_function` again to pick up
later registrations.

SQL registration requires Python setup in the same session. Null endpoints produce null.
Unknown aliases and non-string requests fail before inference within their Arrow batch.
Earlier batches may already have invoked models. Analysis-time validation remains open design work.

## Execution and retries

Aliases belong to a Spark session. Registration requires an active or default session.
Use `session=spark` with both `register_model` and `ai_query` to select another session.
Each column captures the registration available at construction time. Invocation errors fail
the task by default. Set `on_error="null"` on `ai_query` or `register_sql_function` to return
null for known inference and transient failures. For SQL, the policy applies to all calls to
the registered function. Invalid input, client initialization, authentication, and permission
errors still fail the task.

Concurrency is bounded per active Python task. For example, 20 tasks with
`max_concurrency=4` can issue up to 80 concurrent requests. Each task runs
with at most `max_concurrency` requests in flight, starting another as each completes
and preserving input order. Retries use randomized backoff and respect `Retry-After`.
The worker invokes the model for each non-null row before yielding the Arrow batch. A preview such as
`show(1)` can therefore invoke the model for more rows than it displays. Apply a limit
to the input before adding the inference column when previewing a workload.

Spark can repeat billable calls when retrying a failed task. Persist results to reuse them
across downstream jobs.

For Databricks, `timeout` is a per-row budget across client initialization, authentication,
HTTP attempts, and retry delays. Each HTTP attempt uses the budget remaining after
initialization and authentication. Successful responses received after expiry are rejected
with `AI_QUERY_INVOCATION_FAILED.TIMEOUT`, which produces null with `on_error="null"`.
Client initialization, authentication, and response downloads can delay timeout reporting
beyond the budget. The HTTP timeout limits connection and read
inactivity, so a response that keeps delivering bytes can continue downloading past expiry.
Bedrock uses the SDK's connection and read timeouts.

Databricks responses are limited to 16 MiB of decoded response data. The minimum urllib3
version bounds gzip decompression while reading the response. Larger responses
fail with `AI_QUERY_INVOCATION_FAILED.INVALID_RESPONSE`, or produce null with
`on_error="null"`.

## Validation

The suite checks registration, authentication, HTTP errors and retries, response parsing,
concurrency, nulls, and cleanup. An integration test exercises the DataFrame API through
a Spark Python worker and Arrow against a local server. The suite runs without cloud credentials.

```sh
python/run-tests --testnames pyspark.sql.tests.test_ai_query
```

Run the local example against a simulated Bedrock Converse endpoint. It uses synthetic
credentials and the real SDK, Spark workers, and Arrow. It also checks mixed aliases,
registration snapshots, nulls, and unknown aliases.

```sh
python examples/src/main/python/sql/ai_query.py --provider local --report /tmp/ai-query-local.json
```

Run the live checks with these prototype modules installed in both the driver and worker
Python packages. For a pip-installed Spark distribution, the worker package is `python/lib/pyspark.zip`
under the installed `pyspark` package. Use an isolated Spark installation for this check.

```sh
python examples/src/main/python/sql/ai_query.py \
  --provider databricks \
  --profile my-workspace \
  --auth-type databricks-cli \
  --model-id databricks-gpt-oss-20b \
  --report /tmp/ai-query-e2e.json

python examples/src/main/python/sql/ai_query.py \
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
