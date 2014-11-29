# When creating new tests for Spark SQL Hive, the HADOOP_CLASSPATH must contain the hive jars so
# that we can run Hive to generate the golden answer.  This is not required for normal development
# or testing.
if ("$env:HIVE_HOME" -ne ""){
  $hive_lib = Join-Path "$env:HIVE_HOME" "lib"
  if (Test-Path $hive_lib){
    Get-ChildItem $hive_lib | ForEach-Object {
      $env:HADOOP_CLASSPATH += ":{1}" -F $env:HADOOP_CLASSPATH, $_
    }
  }
}

$base_dir = Split-Path $script:myInvocation.MyCommand.path -Parent
$script_name = Split-Path $script:myInvocation.MyCommand.path -Leaf
. (Join-Path $base_dir "sbt-launch-lib.ps1")

$noshare_opts = "-Dsbt.global.base=project\.sbtboot -Dsbt.boot.directory=project\.boot -Dsbt.ivy.home=project\.ivy"
$sbt_opts_file = "sbtconfig.txt"
$etc_sbt_opts_file = ""

function usage(){
  Write-Host @"
Usage: $script_name [options]

  -h | -help         print this message
  -v | -verbose      this runner is chattier
  -d | -debug        set sbt log level to debug
  -no-colors         disable ANSI color codes
  -sbt-create        start sbt even if current directory contains no sbt project
  -sbt-dir   <path>  path to global settings/plugins directory (default: ~/.sbt)
  -sbt-boot  <path>  path to shared boot directory (default: ~/.sbt/boot in 0.11 series)
  -ivy       <path>  path to local Ivy repository (default: ~/.ivy2)
  -mem    <integer>  set memory options (default: $sbt_mem, which is "$(get_mem_opts $sbt_mem)")
  -no-share          use all local caches; no sharing
  -no-global         uses global caches, but does not use global ~/.sbt directory.
  -jvm-debug <port>  Turn on JVM debugging, open at the given port.
  -batch             Disable interactive mode
                     (This is ignored in Windows)

  # sbt version (default: from project/build.properties if present, else latest release)
  -sbt-version  <version>   use the specified version of sbt
  -sbt-jar      <path>      use the specified jar as the sbt launcher
  -sbt-rc                   use an RC version of sbt
  -sbt-snapshot             use a snapshot version of sbt

  # java version (default: java from PATH, or JAVA_HOME)
  -java-home <path>         alternate JAVA_HOME

  # jvm options and output control
  JAVA_OPTS          environment variable, if unset uses "$java_opts"
  SBT_OPTS           environment variable, if unset uses "$default_sbt_opts"
  sbtconfig.txt      if this file exists in the current directory, it is
                     prepended to the runner args
  /etc/sbt/sbtopts   if this file exists, it is prepended to the runner args
                     (This is not available in Windows)
  -Dkey=val          pass -Dkey=val directly to the java runtime
  -J-X               pass option -X directly to the java runtime
                     (-J is stripped)
  -S-X               add -X to sbt's scalacOptions (-S is stripped)
  -PmavenProfiles    Enable a maven profile for the build.

In the case of duplicated or conflicting options, the order above
shows precedence: JAVA_OPTS lowest, command line options highest.
"@
}

function process_my_args($_args){
  while ($_args.Count -ne 0){
    switch -CaseSensitive ($_args[0]){
      "-no-colors"     {addJava "-Dsbt.log.noformat=true"; shift $_args 1}
      "-no-share"      {addJava $noshare_opts; shift $_args 1}
      "-no-global"     {$str="-Dsbt.global.base=" + (pwd) + "\project\.sbtboot"; addJava $str; shift $_args 1}
      "-sbt-boot"      {require_arg "path" $_args; $str="-Dsbt.boot.directory="+$_args[1]; addJava $str; shift $_args 2}
      "-sbt-dir"       {require_arg "path" $_args; $str="-Dsbt.global.base="+$_args[1]; addJava $str; shift $_args 2}
      "-debug-inc"     {addJava "-Dxsbt.inc.debug=true"; shift $_args 1}
      "-batch"         {Write-Host "-batch is ignored in Windows"; shift $_args 1}

      "-sbt-create"    {$global:sbt_create=1; shift $_args 1}

      default          {addResidual $_args[0]; shift $_args 1}
    }
  }
}

function loadConfigFile($file, $_args){
  Get-Content $file | ForEach-Object {
    $_.Trim() -replace '^#.*$' -split " "
  } | Where-Object {
    $_.Length -gt 0
  } | ForEach-Object{
    $_args.Add($_)
  }
}

# In PowerShell, it is difficult to operate $args (which is PowerShell array) like bash way.
# So, we convert it to List<String> in .NET Framework first.
$argslist = New-Object 'System.Collections.Generic.List[System.String]'

# if sbtconfig.txt files exist, prepend their contents to $argslist so it can be processed by this runner
if(is_file $etc_sbt_opts_file){
  loadConfigFile $etc_sbt_opts_file $argslist
}
if(is_file $sbt_opts_file){
  loadConfigFile $sbt_opts_file $argslist
}

$args | ForEach-Object {
  $argslist.Add($_)
}
run $argslist
