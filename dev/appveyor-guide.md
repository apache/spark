# Appveyor Guides

Currently, SparkR on Windows is being tested with [Appveyor](https://ci.appveyor.com). This page describes how to set up Appveyor with Spark, how to run the build, check the status and stop the build via this tool. There is the documenation for Appveyor [here](https://www.appveyor.com/docs). Please refer this for full details.


### Setting up Appveyor

#### Sign up Appveyor.
    
<img width="196" alt="2016-09-04 11 07 48" src="https://cloud.githubusercontent.com/assets/6477701/18228809/2c923aa4-7299-11e6-91b4-f39eff5727ba.png">

Go to https://ci.appveyor.com, and then click "SIGN UP FOR FREE".
    
<img width="379" alt="2016-09-04 11 07 58" src="https://cloud.githubusercontent.com/assets/6477701/18228810/2f674e5e-7299-11e6-929d-5c2dff269ddc.png">

As Apache Spark is one of open source projects, click "FREE - for open-source projects".

<img width="360" alt="2016-09-04 11 08 10" src="https://cloud.githubusercontent.com/assets/6477701/18228811/344263a0-7299-11e6-90b7-9b1c7b6b8b01.png">

Click "Github".


#### After signing up, go to profile to link Github and Appveyor.

<img width="204" alt="2016-09-04 11 09 43" src="https://cloud.githubusercontent.com/assets/6477701/18228803/12a4b810-7299-11e6-9140-5cfc277297b1.png">

Click your account and then click "Profile".

<img width="256" alt="2016-09-04 11 09 52" src="https://cloud.githubusercontent.com/assets/6477701/18228808/23861584-7299-11e6-9352-640a9c747c83.png">

Enable the link with GitHub via clicking "Link Github account".

<img width="491" alt="2016-09-04 11 10 05" src="https://cloud.githubusercontent.com/assets/6477701/18228814/5cc239e0-7299-11e6-8aeb-71305e22d930.png">

Click "Authorize application" in Github site.


#### Add a project, Spark to enable the builds.

<img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

Go to the PROJECTS menu.
    
<img width="144" alt="2016-08-30 12 16 35" src="https://cloud.githubusercontent.com/assets/6477701/18075026/3ee57bc6-6eac-11e6-826e-5dd09aeb0e7c.png">
    
Click "NEW PROJECT" to add Spark.
    
<img width="517" alt="2016-09-04 11 10 22" src="https://cloud.githubusercontent.com/assets/6477701/18228819/9a4d5722-7299-11e6-900c-c5ff6b0450b1.png">

Since we will use Github here, click the "GITHUB" button and then click "Authorize Github" so that Appveyor can access to the Github logs (e.g. commits).

<img width="484" alt="2016-09-04 11 10 27" src="https://cloud.githubusercontent.com/assets/6477701/18228820/a7cfce02-7299-11e6-8ec0-1dd7807eecb7.png">
    
Click "Authorize application" from Github (the above step will pop up this page.)
    
<img width="738" alt="2016-09-04 11 10 36" src="https://cloud.githubusercontent.com/assets/6477701/18228821/b4b35918-7299-11e6-968d-233f18bc2cc7.png">

Come back to https://ci.appveyor.com/projects/new and then adds "spark".


#### Check if any event supposed to run the build actually triggers the build. 

<img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

Click "PROJECTS" menu.

<img width="707" alt="2016-09-04 11 22 37" src="https://cloud.githubusercontent.com/assets/6477701/18228828/5174cad4-729a-11e6-8737-bb7b9e0703c8.png">

Click Spark project.



### Checking the status, restarting and stopping the build 

<img width="97" alt="2016-08-30 12 16 31" src="https://cloud.githubusercontent.com/assets/6477701/18075017/2e572ffc-6eac-11e6-8e72-1531c81717a0.png">

Click "PROJECTS" menu.

<img width="707" alt="2016-09-04 11 22 37" src="https://cloud.githubusercontent.com/assets/6477701/18228828/5174cad4-729a-11e6-8737-bb7b9e0703c8.png">

Locate "spark" and click it.

<img width="709" alt="2016-09-04 11 23 24" src="https://cloud.githubusercontent.com/assets/6477701/18228825/01b4763e-729a-11e6-8486-1429a88d2bdd.png">

Here, we can check the status of current build. Also, "HISTORY" shows the past build history.

<img width="176" alt="2016-08-30 12 29 41" src="https://cloud.githubusercontent.com/assets/6477701/18075336/de618b52-6eae-11e6-8f01-e4ce48963087.png">

If the build is stopped, "RE-BUILD COMMIT" button appears. Click this button to restart the build.

<img width="158" alt="2016-08-30 1 11 13" src="https://cloud.githubusercontent.com/assets/6477701/18075806/4de68564-6eb3-11e6-855b-ee22918767f9.png">

If the build is running, "CANCEL BUILD" buttom appears. Click this button top cancel the current build.


### Specifying the branch for building and setting the build schedule

<img width="1010" alt="2016-08-30 1 19 12" src="https://cloud.githubusercontent.com/assets/6477701/18075954/65d1aefa-6eb4-11e6-9a45-b9a9295f5085.png">

Click the settings button on the right.
   
<img width="422" alt="2016-08-30 12 42 25" src="https://cloud.githubusercontent.com/assets/6477701/18075416/8fac36c8-6eaf-11e6-9262-797a2a66fec4.png">

Set the default branch to build as above.

<img width="358" alt="2016-08-30 12 42 33" src="https://cloud.githubusercontent.com/assets/6477701/18075421/97b17734-6eaf-11e6-8b19-bc1dca840c96.png">

Specify the branch in order to exclude the builds in other branches.

<img width="471" alt="2016-08-30 12 42 43" src="https://cloud.githubusercontent.com/assets/6477701/18075450/d4ef256a-6eaf-11e6-8e41-74e38dac8ca0.png">

Set the Crontab expression to regularly start the build. Appveyor uses Crontab expression, [atifaziz/NCrontab](https://github.com/atifaziz/NCrontab/wiki/Crontab-Expression). Please refer the examples [here](https://github.com/atifaziz/NCrontab/wiki/Crontab-Examples).



### Filtering commits and Pull Requests

Currently, Appveyor is only used for SparkR. So, the build is only triggered when R codes are changed.

This is specified in `.appveyor.yml` as below:

```
only_commits:
  files:
    - R/
```

Please refer https://www.appveyor.com/docs/how-to/filtering-commits for more details.
