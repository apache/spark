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
The prototype does not add a native SQL function. Spark Connect and streaming are unvalidated.

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

prompt = concat(lit("Summarize in one sentence: "), col("description"))
result = documents.select(
    ai_query("databricks-summary", prompt).alias("databricks_summary"),
    ai_query("bedrock-summary", prompt).alias("bedrock_summary"),
)
```

Each provider receives a chat message per non-null request and returns text. Both accept
`max_tokens` and `temperature`. Null requests produce null. Concurrency is bounded per active
Python task. Bedrock applies the configured timeout to socket reads per attempt. Retries and
repeated Spark actions can make additional billable calls.

## Execution and retries

Aliases are registered in the driver Python process. Each constructed column captures
the registration available at construction time. Requests must be strings or nulls.
Invalid requests and invocation errors fail the task.

Workers resolve credentials when they create provider clients. For Databricks, the SDK
refreshes authentication headers for each attempt. Transient connection errors and
HTTP 429, 500, 502, 503, and 504 responses are retried with bounded exponential backoff.
Numeric `Retry-After` headers are respected within the invocation time budget. Other HTTP
errors fail immediately and redirects are rejected. For Bedrock, the AWS SDK handles retries.

Concurrency is bounded per active Python task. For example, 20 tasks with
`max_concurrency=4` can issue up to 80 concurrent requests. A Pandas batch is processed
as individual model invocations. Persist results to reuse them across downstream jobs.

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
