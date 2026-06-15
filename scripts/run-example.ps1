$ErrorActionPreference = "Stop"

function Show-Usage {
  Write-Host @"
Usage: scripts/run-example.ps1 <example> [role]   (Java SDK)

  role is one of: worker (default), starter

Split labs ship a standalone Worker and starter. Run them in two terminals:
  scripts/run-example.ps1 async worker    # terminal 1: long-lived Worker
  scripts/run-example.ps1 async starter   # terminal 2: starts one Workflow

Examples:
  scripts/run-example.ps1 hello
  scripts/run-example.ps1 async
  scripts/run-example.ps1 approval
  scripts/run-example.ps1 schedules
  scripts/run-example.ps1 testing
  scripts/run-example.ps1 saga
  scripts/run-example.ps1 retries
  scripts/run-example.ps1 child
  scripts/run-example.ps1 replay
  scripts/run-example.ps1 continue
"@
}

if ($args.Count -lt 1 -or $args.Count -gt 2) {
  Show-Usage
  exit 2
}

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$example = $args[0]
$role = if ($args.Count -ge 2) { $args[1] } else { "worker" }
if ($role -notin @("worker", "starter")) {
  Write-Error "Unknown role: $role (use worker or starter)"
  exit 2
}
$dir = $null
$mode = "exec"
$needsTemporal = $true
$mainClass = $null

switch ($example) {
  { $_ -in @("hello", "01", "01-hello-temporal") } {
    $dir = "examples/runnable/01-hello-temporal"
    break
  }
  { $_ -in @("async", "parallel", "02", "02-async-parallel-activities") } {
    $dir = "examples/runnable/02-async-parallel-activities"
    break
  }
  { $_ -in @("approval", "signals", "updates", "03", "03-signals-queries-updates") } {
    $dir = "examples/runnable/03-signals-queries-updates"
    break
  }
  { $_ -in @("schedules", "04", "04-schedules") } {
    $dir = "examples/runnable/04-schedules"
    $mode = "compile"
    $mainClass = "training.temporal.schedules.CreateSchedule"
    break
  }
  { $_ -in @("kafka", "05", "05-kafka-bridge") } {
    $dir = "examples/runnable/05-kafka-bridge"
    $mainClass = "training.temporal.kafka.KafkaWorker"
    break
  }
  { $_ -in @("testing", "test", "06", "06-testing") } {
    $dir = "examples/runnable/06-testing"
    $mode = "test"
    $needsTemporal = $false
    break
  }
  { $_ -in @("saga", "07", "07-saga") } {
    $dir = "examples/runnable/07-saga"
    $mainClass = "training.temporal.saga.SagaWorker"
    break
  }
  { $_ -in @("aws", "containers", "08", "08-aws-containers") } {
    $dir = "examples/runnable/08-aws-containers"
    $mainClass = "training.temporal.aws.WorkerMain"
    break
  }
  { $_ -in @("retries", "heartbeat", "heartbeats", "09", "09-retries-heartbeats") } {
    $dir = "examples/runnable/09-retries-heartbeats"
    break
  }
  { $_ -in @("child", "children", "10", "10-child-workflows") } {
    $dir = "examples/runnable/10-child-workflows"
    break
  }
  { $_ -in @("replay", "determinism", "11", "11-determinism-replay") } {
    $dir = "examples/runnable/11-determinism-replay"
    $mode = "test"
    $needsTemporal = $false
    break
  }
  { $_ -in @("continue", "continueasnew", "can", "12", "12-continue-as-new") } {
    $dir = "examples/runnable/12-continue-as-new"
    break
  }
  default {
    Write-Error "Unknown runnable example: $example"
    Show-Usage
    exit 2
  }
}

if ($null -eq (Get-Command mvn -ErrorAction SilentlyContinue)) {
  Write-Error "Maven is required. See Setup.md."
  exit 1
}

if ($needsTemporal) {
  $connection = Test-NetConnection -ComputerName 127.0.0.1 -Port 7233 -WarningAction SilentlyContinue
  if (!$connection.TcpTestSucceeded) {
    Write-Error "Temporal is not reachable at 127.0.0.1:7233. Start it with: scripts/start-temporal.ps1"
    exit 1
  }
}

# Migrated labs keep the Maven project under java/; prefer it when present.
$projectDir = Join-Path $root $dir
if (Test-Path (Join-Path $projectDir "java")) {
  $projectDir = Join-Path $projectDir "java"
}

# For split labs the Worker is the pom's default mainClass (a *Worker class);
# the starter is the matching *Starter class.
if ($role -eq "starter") {
  $workerClass = $mainClass
  if (-not $workerClass) {
    $pom = Join-Path $projectDir "pom.xml"
    $match = Select-String -Path $pom -Pattern '<mainClass>(.*)</mainClass>' | Select-Object -First 1
    if ($match) { $workerClass = $match.Matches[0].Groups[1].Value }
  }
  if ($workerClass) { $mainClass = ($workerClass -replace 'Worker$', 'Starter') }
}

Push-Location $projectDir
try {
  if ($mode -eq "exec") {
    if ($mainClass) {
      mvn -q compile exec:java "-Dexec.mainClass=$mainClass"
    } else {
      mvn -q compile exec:java
    }
  } elseif ($mode -eq "test") {
    mvn -q test
  } elseif ($mainClass) {
    mvn -q compile exec:java "-Dexec.mainClass=$mainClass"
  } else {
    mvn -q -DskipTests compile
  }
} finally {
  Pop-Location
}
