<#
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
#>

# create tools directory outside of REEF directory
$up = (Get-Item -Path ".." -Verbose).FullName
$tools = "$up\tools"
if (!(Test-Path $tools))
{
    New-Item -ItemType Directory -Force -Path $tools | Out-Null
}

# ========================== maven
Push-Location $tools

$mavenVer = "3.3.9"
Start-FileDownload "https://archive.apache.org/dist/maven/maven-3/$mavenVer/binaries/apache-maven-$mavenVer-bin.zip" "maven.zip"

# extract
Invoke-Expression "7z.exe x maven.zip" | Out-Null

# add maven to environment variables
$env:Path += ";$tools\apache-maven-$mavenVer\bin"
$env:M2_HOME = "$tools\apache-maven-$mavenVer"
$env:MAVEN_OPTS = "-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

Pop-Location

# ========================== Hadoop bin package 
$hadoopVer = "2.6.0"
$hadoopPath = "$tools\hadoop"
if (!(Test-Path $hadoopPath))
{
    New-Item -ItemType Directory -Force -Path $hadoopPath | Out-Null
}
Push-Location $hadoopPath

Start-FileDownload "https://github.com/steveloughran/winutils/archive/master.zip" "winutils-master.zip"

# extract
Invoke-Expression "7z.exe x winutils-master.zip" | Out-Null

# add hadoop bin to environment variables
$env:HADOOP_HOME = "$hadoopPath/hadoop-$hadoopVer"

Pop-Location

