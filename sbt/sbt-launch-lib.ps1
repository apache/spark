function is_file($path){
  if($null -eq $path -or $path.Length -eq 0){
    return $false
  }
  Test-Path $path -PathType Leaf
}

$residual_argslist = New-Object 'System.Collections.Generic.List[System.String]'
$java_args = @()
$sbt_commands = @()
$maven_profiles = @()

$java_cmd = "java.exe"
if($null -ne $env:JAVA_HOME){
  $java_cmd_candidate = Join-Path (Join-Path $env:JAVA_HOME "bin") "java.exe"
  if(is_file $java_cmd_candidate){
    Write-Host "Using %JAVA_HOME% as default JAVA_HOME."
    Write-Host "Note, this will be overridden by -java-home if it is set."
    $global:java_cmd = $java_cmd_candidate
  }
}

function echoerr(){
  Write-Host $args
}
function vlog(){
  if($verbose -eq 1 -or $debug -eq 1){
    echoerr $args
  }
}
function dlog(){
  if($debug -eq 1){
    echoerr $args
  }
}

function web_download($url, $out){
  $result = $false
  try{
    Invoke-WebRequest -Uri $url -OutFile $out -ErrorAction Continue
    $result = $?
  }catch [System.Management.Automation.CommandNotFoundException]{
    # Invoke-WebRequest is available with PowerShell 3.0 or over.
    # If CommandNotFoundException, we use System.Net.WebClient in .NET Framework.
    (New-Object System.Net.WebClient).DownloadFile($url, $out)
    $result = $true
  }
  $result
}

function acquire_sbt_jar($version){
  $sbt_version = "0.13.7"
  $url1 = "http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$sbt_version/sbt-launch.jar"
  $url2 = "http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/$sbt_version/sbt-launch.jar"
  $jar = "sbt\sbt-launch-$sbt_version.jar"

  $global:sbt_jar = $jar

  if(-not (is_file $sbt_jar)){
    # Download sbt launch jar if it hasn't been downloaded yet
    Write-Host "Attempting to fetch sbt"
    $jar_dl="{0}.part" -F ${jar}
    $result = $false
    if(-not (is_file $jar)){
      $result = web_download $url1 $jar_dl
      Move-Item $jar_dl $jar
    }
    if(-not (is_file $jar)){
      $result = web_download $url2 $jar_dl
      Move-Item $jar_dl $jar
    }
    if(-not (is_file $jar)){
      # We failed to download
      Write-Host "Our attempt to download sbt locally to $jar failed. Please install sbt manually from http://www.scala-sbt.org/"
      exit
    }
    Write-Host "Launching sbt from $jar"
  }
  $true
}

function execRunner($cmd, $cmdargs){
  vlog "# With %SBT_MAVEN_PROFILES% : " $env:SBT_MAVEN_PROFILES
  vlog "# Executing command line: " $cmd $cmdargs
  try{
    Start-Process $cmd $cmdargs -Wait -NoNewWindow
  }catch [InvalidOperationException]{
    Write-Host ("> " + $_.ToString())
    Write-Host (">> " + $_.InvocationInfo.PositionMessage)
    Write-Host ("'{0}' cannot be executed." -F $cmd)
  }
}

function addJava($param){
  dlog "[addJava] arg = $param"
  $global:java_args += $param
}
function enableProfile($param){
  dlog "[enableProfile] arg = $param"
  $global:maven_profiles += $param
  $env:SBT_MAVEN_PROFILES=($global:maven_profiles -join " ")
}
function addSbt($param){
  dlog "[addSbt] arg = $param"
  $global:sbt_commands += $param
}
function addResidual($param){
  dlog "[residual] arg = $param"
  $global:residual_argslist.Add($param)
}
function addDebugger($param){
  addJava "-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=$param"
}

# a ham-fisted attempt to move some memory settings in concert
# so they need not be dicked around with individually.
function get_mem_opts([int]$mem=2048){
  if($mem -eq 0){ $mem = 2048 }
  $perm = $mem / 4
  if($perm -lt 256){ $perm = 256 }
  if($perm -gt 4096){ $perm = 4096 }
  $codecache = $perm / 2

  "-Xms{0}m -Xmx{0}m -XX:MaxPermSize={1}m -XX:ReservedCodeCacheSize={2}m" -F $mem, $perm, $codecache
}

function shift($_args, $num){
  for($i=0; $i -lt $num; $i++){
    $_args.RemoveAt(0);
  }
}
function require_arg($type, $_args){
  if($_args.Count -lt 2 -or $_args[1].StartsWith("-")){
    Write-Host $_args[0] " requires <$type> argument"
    exit
  }
}
function process_args($_args){
  while ($_args.Count -ne 0){
    switch -regex -CaseSensitive ($_args[0]){
      "^-h|^-help"     {usage; exit}
      "^-v|^-verbose"  {$global:verbose=1; shift $_args 1}
      "^-d|^-debug"    {$global:debug=1; shift $_args 1}

      "^-ivy"          {require_arg "path" $_args; $str="-Dsbt.ivy.home="+$_args[1]; addJava $str; shift $_args 2}
      "^-mem"          {require_arg "integer" $_args; $global:sbt_mem=$_args[1]; shift $_args 2}
      "^-jvm\-debug"   {require_arg "port" $_args; $str=$_args[1]; addDebugger $str; shift $_args 2}
      "-batch"         {Write-Host "-batch is ignored in Windows"; shift $_args 1}

      "^-sbt\-jar"     {require_arg "path" $_args; $global:sbt_jar=$_args[1]; shift $_args 2}
      "^-sbt\-version" {require_arg "version" $_args; $global:sbt_version=$_args[1]; shift $_args 2}
      "^-java\-home"   {require_arg "path" $_args; $global:java_cmd=$_args[1]+"\bin\java"; $env:JAVA_HOME=$_args[1]; shift $_args 2}

      "^-D.*"          {$str=$_args[0]; addJava $str; shift $_args 1}
      "^-J.*"          {$str=$_args[0].Substring(2); addJava $str; shift $_args 1}
      "^-P.*"          {$str=$_args[0]; enableProfile $str; shift $_args 1}
      default          {addResidual $_args[0]; shift $_args 1}
    }
  }

  $tmp_argslist = New-Object 'System.Collections.Generic.List[System.String]'
  $tmp_argslist.AddRange($residual_argslist)
  $residual_argslist.Clear()
  try{
    process_my_args $tmp_argslist
  }catch [System.Management.Automation.CommandNotFoundException]{
    # If CommandNotFoundException, perhaps "process_my_args" function doesn't exist.
    $global:residual_argslist = $tmp_argslist
  }
}

function run($_args){
  # no jar? download it.
  if(-not (is_file $sbt_jar)){
    if(-not (acquire_sbt_jar $sbt_version)){
      # still no jar? uh-oh.
      echo "Download failed. Obtain the sbt-launch.jar manually and place it at $sbt_jar"
      exit 1
    }
  }

  # process the combined args
  process_args $_args

  # run sbt
  $java_cmdargs = "{0} {1} {2} {3} {4} {5} {6}" -F `
     $env:SBT_OPTS, `
     (get_mem_opts $sbt_mem).ToString(), `
     $java_opts, `
     ($java_args -join " "), `
     "-jar $sbt_jar", `
     ($sbt_commands -join " "), `
     ($residual_argslist -join " ")
  execRunner $java_cmd $java_cmdargs
}
