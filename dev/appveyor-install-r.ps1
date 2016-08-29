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

[CmdletBinding()]
Param()

Progress "Bootstrap: Start"

Progress "Adding GnuWin32 tools to PATH"
$env:PATH = "C:\Program Files (x86)\Git\bin;" + $env:PATH

Progress "Setting time zone"
tzutil /g
tzutil /s "GMT Standard Time"
tzutil /g

InstallR

if ((Test-Path "src") -or ($env:USE_RTOOLS)) {
  InstallRtools
}
Else {
  Progress "Skipping download of Rtools because src/ directory is missing."
}

$env:PATH.Split(";")

Progress "Setting R_LIBS_USER"
$env:R_LIBS_USER = 'c:\RLibrary'
if ( -not(Test-Path $env:R_LIBS_USER) ) {
  mkdir $env:R_LIBS_USER
}

Progress "Bootstrap: Done"
