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

# Found at http://zduck.com/2012/powershell-batch-files-exit-codes/
Function Exec
{
    [CmdletBinding()]
    param (
        [Parameter(Position=0, Mandatory=1)]
        [scriptblock]$Command,
        [Parameter(Position=1, Mandatory=0)]
        [string]$ErrorMessage = "Execution of command failed.`n$Command"
    )
    $ErrorActionPreference = "Continue"
    & $Command 2>&1 | %{ "$_" }
    if ($LastExitCode -ne 0) {
        throw "Exec: $ErrorMessage`nExit code: $LastExitCode"
    }
}

Function Progress
{
    [CmdletBinding()]
    param (
        [Parameter(Position=0, Mandatory=0)]
        [string]$Message = ""
    )

    $ProgressMessage = '== ' + (Get-Date) + ': ' + $Message

    Write-Host $ProgressMessage -ForegroundColor Magenta
}

Function InstallR {
  [CmdletBinding()]
  Param()

  if ( -not(Test-Path Env:\R_VERSION) ) {
    $version = "patched"
  }
  Else {
    $version = $env:R_VERSION
  }

  if ( -not(Test-Path Env:\R_ARCH) ) {
    $arch = "i386"
  }
  Else {
    $arch = $env:R_ARCH
  }

  Progress ("Version: " + $version)

  If ($version -eq "devel") {
    $url_path = ""
    $version = "devel"
  }
  ElseIf (($version -eq "stable") -or ($version -eq "release")) {
    $url_path = ""
    $version = $(ConvertFrom-JSON $(Invoke-WebRequest http://rversions.r-pkg.org/r-release).Content).version
    If ($version -eq "3.2.4") {
      $version = "3.2.4revised"
    }
  }
  ElseIf ($version -eq "patched") {
    $url_path = ""
    $version = $(ConvertFrom-JSON $(Invoke-WebRequest http://rversions.r-pkg.org/r-release).Content).version + "patched"
  }
  ElseIf ($version -eq "oldrel") {
    $version = $(ConvertFrom-JSON $(Invoke-WebRequest http://rversions.r-pkg.org/r-oldrel).Content).version
    $url_path = ("old/" + $version + "/")
  }
  Else {
      $url_path = ("old/" + $version + "/")
  }

  Progress ("URL path: " + $url_path)

  $rurl = $CRAN + "/bin/windows/base/" + $url_path + "R-" + $version + "-win.exe"

  Progress ("Downloading R from: " + $rurl)
  Exec { bash -c ("curl --silent -o ../R-win.exe -L " + $rurl) }

  Progress "Running R installer"
  Start-Process -FilePath ..\R-win.exe -ArgumentList "/VERYSILENT /DIR=C:\R" -NoNewWindow -Wait

  $RDrive = "C:"
  echo "R is now available on drive $RDrive"

  Progress "Setting PATH"
  $env:PATH = $RDrive + '\R\bin\' + $arch + ';' + 'C:\MinGW\msys\1.0\bin;' + $env:PATH

  Progress "Testing R installation"
  Rscript -e "sessionInfo()"
}

Function InstallRtools {
  if ( -not(Test-Path Env:\RTOOLS_VERSION) ) {
    Progress "Determining Rtools version"
    $rtoolsver = $(Invoke-WebRequest ($CRAN + "/bin/windows/Rtools/VERSION.txt")).Content.Split(' ')[2].Split('.')[0..1] -Join ''
  }
  Else {
    $rtoolsver = $env:RTOOLS_VERSION
  }

  $rtoolsurl = $CRAN + "/bin/windows/Rtools/Rtools$rtoolsver.exe"

  Progress ("Downloading Rtools from: " + $rtoolsurl)
  bash -c ("curl --silent -o ../Rtools-current.exe -L " + $rtoolsurl)

  Progress "Running Rtools installer"
  Start-Process -FilePath ..\Rtools-current.exe -ArgumentList /VERYSILENT -NoNewWindow -Wait

  $RtoolsDrive = "C:"
  echo "Rtools is now available on drive $RtoolsDrive"

  Progress "Setting PATH"
  if ( -not(Test-Path Env:\GCC_PATH) ) {
    $gcc_path = "gcc-4.6.3"
  }
  Else {
    $gcc_path = $env:GCC_PATH
  }
  $env:PATH = $RtoolsDrive + '\Rtools\bin;' + $RtoolsDrive + '\Rtools\MinGW\bin;' + $RtoolsDrive + '\Rtools\' + $gcc_path + '\bin;' + $env:PATH
  $env:BINPREF=$RtoolsDrive + '/Rtools/mingw_$(WIN)/bin/'
}

# create tools directory outside of Spark directory
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
Invoke-Expression "7z.exe x maven.zip"

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
Invoke-Expression "7z.exe x winutils-master.zip"

# add hadoop bin to environment variables
$env:HADOOP_HOME = "$hadoopPath/winutils-master/hadoop-$hadoopVer"

Pop-Location

# ========================== R
$env:PATH = "C:\Program Files (x86)\Git\bin;" + $env:PATH
InstallR
if ((Test-Path "src") -or ($env:USE_RTOOLS)) {
  InstallRtools
}
Else {
  Progress "Skipping download of Rtools because src/ directory is missing."
}
$env:PATH.Split(";")
$env:R_LIBS_USER = 'c:\RLibrary'
if ( -not(Test-Path $env:R_LIBS_USER) ) {
  mkdir $env:R_LIBS_USER
}

