---
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

# AppVeyor Guides

Currently, SparkR on Windows is being tested with [AppVeyor](https://ci.appveyor.com). This page describes how to set up AppVeyor with Spark, how to run the build, check the status and stop the build via this tool. There is the documentation for AppVeyor [here](https://www.appveyor.com/docs). Please refer this for full details.


### Setting up AppVeyor

#### Sign up AppVeyor.

- Go to https://ci.appveyor.com, and then click "SIGN UP FOR FREE".
    
  <img width="196" alt="2016-09-04 11 07 48" src="https://cloud.githubusercontent.com/assets/6477701/18228809/2c923aa4-7299-11e6-91b4-f39eff5727ba.png">

- As Apache Spark is one of open source projects, click "FREE - for open-source projects".
    
  <img width="379" alt="2016-09-04 11 07 58" src="https://cloud.githubusercontent.com/assets/6477701/18228810/2f674e5e-7299-11e6-929d-5c2dff269ddc.png">

- Click "GitHub".

  <img width="360" alt="2016-09-04 11 08 10" src="https://cloud.githubusercontent.com/assets/6477701/18228811/344263a0-7299-11e6-90b7-9b1c7b6b8b01.png">


#### After signing up, go to profile to link GitHub and AppVeyor.

- Click your account and then click "Profile".

  <img width="204" alt="2016-09-04 11 09 43" src="https://cloud.githubusercontent.com/assets/6477701/18228803/12a4b810-7299-11e6-9140-5cfc277297b1.png">

- Enable the link with GitHub via clicking "Link GitHub account".

  <img width="256" alt="2016-09-04 11 09 52" src="https://cloud.githubusercontent.com/assets/6477701/18228808/23861584-7299-11e6-9352-640a9c747c83.png">

- Click "Authorize application" in GitHub site.

<img width="491" alt="2016-09-04 11 10 05" src="https://cloud.githubusercontent.com/assets/6477701/18228814/5cc239e0-7299-11e6-8aeb-71305e22d930.png">


#### Add a project, Spark to enable the builds.

- Go to the PROJECTS menu.

  <img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

- Click "NEW PROJECT" to add Spark.
  
  <img width="144" alt="2016-08-30 12 16 35" src="https://cloud.githubusercontent.com/assets/6477701/18075026/3ee57bc6-6eac-11e6-826e-5dd09aeb0e7c.png">

- Since we will use GitHub here, click the "GITHUB" button and then click "Authorize GitHub" so that AppVeyor can access the GitHub logs (e.g. commits).
    
  <img width="517" alt="2016-09-04 11 10 22" src="https://cloud.githubusercontent.com/assets/6477701/18228819/9a4d5722-7299-11e6-900c-c5ff6b0450b1.png">

- Click "Authorize application" from GitHub (the above step will pop up this page).

  <img width="484" alt="2016-09-04 11 10 27" src="https://cloud.githubusercontent.com/assets/6477701/18228820/a7cfce02-7299-11e6-8ec0-1dd7807eecb7.png">

- Come back to https://ci.appveyor.com/projects/new and then adds "spark".

  <img width="738" alt="2016-09-04 11 10 36" src="https://cloud.githubusercontent.com/assets/6477701/18228821/b4b35918-7299-11e6-968d-233f18bc2cc7.png">


#### Check if any event supposed to run the build actually triggers the build. 

- Click "PROJECTS" menu.

  <img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

- Click Spark project.

  <img width="707" alt="2016-09-04 11 22 37" src="https://cloud.githubusercontent.com/assets/6477701/18228828/5174cad4-729a-11e6-8737-bb7b9e0703c8.png">


### Checking the status, restarting and stopping the build 

- Click "PROJECTS" menu.

  <img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

- Locate "spark" and click it.

  <img width="707" alt="2016-09-04 11 22 37" src="https://cloud.githubusercontent.com/assets/6477701/18228828/5174cad4-729a-11e6-8737-bb7b9e0703c8.png">

- Here, we can check the status of current build. Also, "HISTORY" shows the past build history.

  <img width="709" alt="2016-09-04 11 23 24" src="https://cloud.githubusercontent.com/assets/6477701/18228825/01b4763e-729a-11e6-8486-1429a88d2bdd.png">

- If the build is stopped, "RE-BUILD COMMIT" button appears. Click this button to restart the build.

  <img width="176" alt="2016-08-30 12 29 41" src="https://cloud.githubusercontent.com/assets/6477701/18075336/de618b52-6eae-11e6-8f01-e4ce48963087.png">

- If the build is running, "CANCEL BUILD" button appears. Click this button to cancel the current build.

  <img width="158" alt="2016-08-30 1 11 13" src="https://cloud.githubusercontent.com/assets/6477701/18075806/4de68564-6eb3-11e6-855b-ee22918767f9.png">


### Specifying the branch for building and setting the build schedule

Note: It seems the configurations in UI and `appveyor.yml` are  mutually exclusive according to the [documentation](https://www.appveyor.com/docs/build-configuration/#configuring-build).


- Click the settings button on the right.

  <img width="1010" alt="2016-08-30 1 19 12" src="https://cloud.githubusercontent.com/assets/6477701/18075954/65d1aefa-6eb4-11e6-9a45-b9a9295f5085.png">

- Set the default branch to build as above.

  <img width="422" alt="2016-08-30 12 42 25" src="https://cloud.githubusercontent.com/assets/6477701/18075416/8fac36c8-6eaf-11e6-9262-797a2a66fec4.png">

- Specify the branch in order to exclude the builds in other branches.

  <img width="358" alt="2016-08-30 12 42 33" src="https://cloud.githubusercontent.com/assets/6477701/18075421/97b17734-6eaf-11e6-8b19-bc1dca840c96.png">

- Set the Crontab expression to regularly start the build. AppVeyor uses Crontab expression, [atifaziz/NCrontab](https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression). Please refer the examples [here](https://github.com/atifaziz/NCrontab/wiki/Crontab-Examples).


  <img width="471" alt="2016-08-30 12 42 43" src="https://cloud.githubusercontent.com/assets/6477701/18075450/d4ef256a-6eaf-11e6-8e41-74e38dac8ca0.png">


### Filtering commits and Pull Requests

Currently, AppVeyor is only used for SparkR. So, the build is only triggered when R codes are changed.

This is specified in `.appveyor.yml` as below:

```
only_commits:
  files:
    - R/
```

Please refer https://www.appveyor.com/docs/how-to/filtering-commits for more details.


### Checking the full log of the build

Currently, the console in AppVeyor does not print full details. This can be manually checked. For example, AppVeyor shows the failed tests as below in console

```
Failed -------------------------------------------------------------------------
1. Error: union on two RDDs (@test_binary_function.R#38) -----------------------
1: textFile(sc, fileName) at C:/projects/spark/R/lib/SparkR/tests/testthat/test_binary_function.R:38
2: callJMethod(sc, "textFile", path, getMinPartitions(sc, minPartitions))
3: invokeJava(isStatic = FALSE, objId$id, methodName, ...)
4: stop(readString(conn))
```

After downloading the log by clicking the log button as below:

![2016-09-08 11 37 17](https://cloud.githubusercontent.com/assets/6477701/18335227/b07d0782-75b8-11e6-94da-1b88cd2a2402.png)

the details can be checked as below (e.g. exceptions)

```
Failed -------------------------------------------------------------------------
1. Error: spark.lda with text input (@test_mllib.R#655) ------------------------
 org.apache.spark.sql.AnalysisException: Path does not exist: file:/C:/projects/spark/R/lib/SparkR/tests/testthat/data/mllib/sample_lda_data.txt;
    at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$12.apply(DataSource.scala:376)
    at org.apache.spark.sql.execution.datasources.DataSource$$anonfun$12.apply(DataSource.scala:365)
    at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
    at scala.collection.TraversableLike$$anonfun$flatMap$1.apply(TraversableLike.scala:241)
    ...

 1: read.text("data/mllib/sample_lda_data.txt") at C:/projects/spark/R/lib/SparkR/tests/testthat/test_mllib.R:655
 2: dispatchFunc("read.text(path)", x, ...)
 3: f(x, ...)
 4: callJMethod(read, "text", paths)
 5: invokeJava(isStatic = FALSE, objId$id, methodName, ...)
 6: stop(readString(conn))
```
