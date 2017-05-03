---
layout: global
title: REST Interface
description: Spark provides spark driver to submit, delete and query in Cluster Mode.
---

### Spark configuration options

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td>spark.master.rest.port</td>
    <td>6066</td>
    <td>Port for REST Interface. This is used for spark driver to submit, delete and query.</td>
  </tr>
</table>

## REST API

<table class="table">
  <tr><th>URL And Meaning</th><th>Request Method And Body</th></tr>
  <tr>
    <td>URL:
    <br>
    &emsp;<code>/v1/submissions/create</code>
    <br>
    Meaning:
    <br>
    &emsp;Submit a corresponding driver to the Master with parameters specified in the request.
    <br>
    &emsp;The specific parameter value setting needs to refer to the Spark parameter list
    </td>
    <td>Method:POST
    <br>
    Body:
    <br>
    {
    <br>
    &emsp;"action": "CreateSubmissionRequest",
    <br>
    &emsp;"appArgs": [
    <br>
    &emsp;&emsp;"myAppArgument"
    <br>
    &emsp;],
    <br>
    &emsp;"appResource": "",
    <br>
    &emsp;"clientSparkVersion": "",
    <br>
    &emsp;"environmentVariables": {
    <br>
    &emsp;&emsp;"SPARK_ENV_LOADED": "1"
    <br>
    &emsp;},
    <br>
    &emsp;"mainClass": "",
    <br>
    &emsp;"sparkProperties":
    <br>
    &emsp;{
    <br>
    &emsp;&emsp;"spark.jars": "",
    <br>
    &emsp;&emsp;"spark.driver.supervise": "",
    <br>
    &emsp;&emsp;"spark.app.name": "",
    <br>
    &emsp;&emsp;"spark.eventLog.enabled": "",
    <br>
    &emsp;&emsp;"spark.submit.deployMode": "cluster",
    <br>
    &emsp;&emsp;"spark.master": "spark://ip:port"
    <br>
    &emsp;}
    <br>
    }</td>
  </tr>

  <tr>
    <td>URL:
    <br>
    &emsp;<code>v1/submissions/kill/[submission-id]</code>
    <br>
    Meaning:
    <br>
    &emsp;Kill the corresponding driver to the Master with parameters specified in the request.</td>
    <td>Method:POST
    <br>
    Body:None
    </td>
  </tr>

  <tr>
    <td>URL:
    <br>
    &emsp;<code>/v1/submissions/status/[submission-id]</code>
    <br>
    Meaning:
    <br>
    &emsp;Get the status of the corresponding driver from the Master with parameters specified in the request.</td>
    <td>Method:GET
    <br>
    Body:None
    </td>
  </tr>
</table>
