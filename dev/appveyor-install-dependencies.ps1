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

$CRAN = "https://cloud.r-project.org"

Function InstallR {
  if ( -not(Test-Path Env:\R_ARCH) ) {
    $arch = "x64"
  }
  Else {
    $arch = $env:R_ARCH
  }

  $urlPath = ""
  $latestVer = $(ConvertFrom-JSON $(Invoke-WebRequest https://rversions.r-pkg.org/r-release-win).Content).version
  If ($rVer -ne $latestVer) {
    $urlPath = ("old/" + $rVer + "/")
  }

  $rurl = $CRAN + "/bin/windows/base/" + $urlPath + "R-" + $rVer + "-win.exe"

  # Downloading R
  Start-FileDownload $rurl "R-win.exe"

  # Running R installer
  Start-Process -FilePath .\R-win.exe -ArgumentList "/VERYSILENT /DIR=C:\R" -NoNewWindow -Wait

  $RDrive = "C:"
  echo "R is now available on drive $RDrive"

  $env:PATH = $RDrive + '\R\bin\' + $arch + ';' + 'C:\MinGW\msys\1.0\bin;' + $env:PATH

  # Testing R installation
  Rscript -e "sessionInfo()"
}

Function InstallRtools {
  $rtoolsver = $rToolsVer.Split('.')[0..1] -Join ''
  $rtoolsurl = $CRAN + "/bin/windows/Rtools/rtools$rtoolsver-x86_64.exe"

  # Downloading Rtools
  Start-FileDownload $rtoolsurl "Rtools-current.exe"

  # Running Rtools installer
  Start-Process -FilePath .\Rtools-current.exe -ArgumentList /VERYSILENT -NoNewWindow -Wait

  $RtoolsDrive = "C:"
  echo "Rtools is now available on drive $RtoolsDrive"

  if ( -not(Test-Path Env:\GCC_PATH) ) {
    $gccPath = "gcc-4.6.3"
  }
  Else {
    $gccPath = $env:GCC_PATH
  }
  $env:PATH = $RtoolsDrive + '\Rtools40\bin;' + $RtoolsDrive + '\Rtools40\mingw64\bin;' + $RtoolsDrive + '\Rtools40\' + $gccPath + '\bin;' + $env:PATH
  $env:BINPREF=$RtoolsDrive + '/Rtools40/mingw$(WIN)/bin/'
}

# create tools directory outside of Spark directory
$up = (Get-Item -Path ".." -Verbose).FullName
$tools = "$up\tools"
if (!(Test-Path $tools)) {
    New-Item -ItemType Directory -Force -Path $tools | Out-Null
}

# ========================== Maven
# Push-Location $tools
#
# $mavenVer = "3.8.6"
# Start-FileDownload "https://archive.apache.org/dist/maven/maven-3/$mavenVer/binaries/apache-maven-$mavenVer-bin.zip" "maven.zip"
#
# # extract
# Invoke-Expression "7z.exe x maven.zip"
#
# # add maven to environment variables
# $env:PATH = "$tools\apache-maven-$mavenVer\bin;" + $env:PATH
# $env:M2_HOME = "$tools\apache-maven-$mavenVer"
# $env:MAVEN_OPTS = "-Xmx2g -XX:ReservedCodeCacheSize=1g"
#
# Pop-Location

# ========================== SBT
Push-Location $tools

$sbtVer = "1.6.2"
Start-FileDownload "https://github.com/sbt/sbt/releases/download/v$sbtVer/sbt-$sbtVer.zip" "sbt.zip"

# extract
Invoke-Expression "7z.exe x sbt.zip"

# add sbt to environment variables
$env:PATH = "$tools\sbt\bin;" + $env:PATH

Pop-Location

# ========================== Hadoop bin package
# This must match the version at https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.0
$hadoopVer = "3.2.0"
$hadoopPath = "$tools\hadoop"
if (!(Test-Path $hadoopPath)) {
    New-Item -ItemType Directory -Force -Path $hadoopPath | Out-Null
}
Push-Location $hadoopPath

Start-FileDownload "https://codeload.github.com/cdarlint/winutils/zip/master" "winutils-master.zip"

# extract
Invoke-Expression "7z.exe x winutils-master.zip"

# add hadoop bin to environment variables
$env:HADOOP_HOME = "$hadoopPath\winutils-master\hadoop-$hadoopVer"
$env:PATH = "$env:HADOOP_HOME\bin;" + $env:PATH

Pop-Location

# ========================== R
$rVer = "4.2.0"
$rToolsVer = "4.0.2"

InstallR
InstallRtools

$env:R_LIBS_USER = 'c:\RLibrary'
if ( -not(Test-Path $env:R_LIBS_USER) ) {
  mkdir $env:R_LIBS_USER
}

