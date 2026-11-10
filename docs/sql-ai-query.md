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
A `DurableAgentServer` is an optional backend for agent workflows.
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

## Query a deployed agent when needed

Use this backend when the workload needs an agent's tools, custom logic, or durable invocation
records. Deploy an AgentKit `DurableAgentServer` and grant the calling identity access to the app.
The agent chooses its model. Workers discover the app URL through the workspace SDK.

```python
from pyspark.sql.ai import ai_query, register_agent
from pyspark.sql.functions import col, concat, lit

register_agent(
    "summarizer",
    app_name="agent-bricks-summarizer",
    max_concurrency=4,
    timeout=60,
    max_retries=2,
)

# document_id must be a stable, unique source identifier, stored with the input.
result = documents.withColumn(
    "summary",
    ai_query(
        endpoint="summarizer",
        request=concat(lit("Summarize in one sentence: "), col("description")),
        invocation_id=concat(lit("documents-v1:"), col("document_id").cast("string")),
    ),
)
```

The endpoint name is registered in the driver Python process. Each constructed column captures
that configuration, so changing the registration does not change an already constructed column.
`request` and `invocation_id` accept a column or a column name. Null requests produce null without
calling Databricks. Other requests and their identifiers must be strings; identifiers cannot be
null or empty. Errors fail the task, rather than silently returning null.

The default response parser accepts a string returned by the agent, or the assistant text messages
inside an AgentKit template output. For a custom agent that returns `{"answer": "..."}`, register
with `output_field="answer"`. This selects a top-level field of the agent's output, not of the
HTTP response envelope. Non-text outputs and requests that pause for human input fail explicitly.

## Execution and retries

Each worker thread creates its own SDK client and HTTP session. Credentials are resolved locally;
SDK authentication headers are obtained again for every HTTP attempt. An agent request contains
an `input.messages` list and an independent session for its logical input. No conversation is
shared across rows.

The app name, stable input identifier, and complete request determine a UUID. Both the invocation
and session use that UUID. Transient connection errors and HTTP 429, 500, 502, 503, and 504 responses
are retried with bounded exponential backoff and the same invocation ID. Numeric `Retry-After`
headers are respected within the invocation time budget. Authentication, permission,
conflict, and other HTTP errors fail immediately. Redirects are rejected. A running invocation is
polled until it completes or the configured invocation time budget expires. Initial SDK setup and
app discovery are outside that budget; HTTP timeouts bound socket waits rather than enforcing a
hard wall-clock deadline for a response arriving continuously.

Include a dataset/job version in the input identifier. Repeating an action with the same identifier
and request reuses the invocation while Databricks retains its record. Changing the request creates
a new invocation. Do not use random UUID expressions or partition/task IDs as stable source IDs.
Two rows with the same identifier and request intentionally share one invocation. Distinct logical
rows must have distinct source identifiers, even if their text is identical.

Databricks idempotency does not guarantee exactly-once external tool effects. The server's retention
window limits invocation deduplication, and agent crash recovery can replay tool calls. Use agents
whose tools can tolerate retries. This API does not provide cancellation of remote runs when Spark
cancels a task or the local request times out.

Concurrency is bounded **per active Python task**, not per job or workspace. For example, 20 tasks
with `max_concurrency=4` can issue up to 80 concurrent requests. A Pandas batch is processed as
individual HTTP invocations; this is not a Databricks batch inference API. Materialize the selected
input before inference when strict control of the input row set matters, and persist results for
reuse across downstream jobs. All non-null request contents are sent to the configured agent.

## Validation

The unit suite checks the HTTP protocol against a local server, worker-side OAuth initialization,
stable IDs, retry and polling behavior, response parsing, concurrency, row ordering, nulls, cleanup,
and UDF configuration capture. It requires no Databricks credentials or Spark JVM:

```sh
PYTHONPATH=python .venv/bin/python -m unittest pyspark.sql.tests.test_ai_query -v
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
for two synthetic prompts and one null. It does not create a Databricks app or exercise the
agent backend's live invocation or recovery behavior.

## References

* [Databricks agent server](https://docs.databricks.com/aws/en/agents/deploy/agent-server)
* [Databricks Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/)
* [Amazon Bedrock Converse](https://docs.aws.amazon.com/boto3/latest/reference/services/bedrock-runtime/client/converse.html)
* [Query deployed agents](https://docs.databricks.com/aws/en/agents/query-llms)
* [OAuth M2M authentication](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m)

The Databricks Agent Bricks CLI and custom agent APIs are experimental. Keep SDK compatibility
and the invocation contract under review before using this prototype in production.
