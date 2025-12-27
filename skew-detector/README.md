# Spark Scheduling Skew Demo

This project demonstrates a **driver-side scheduling skew detector** for Apache Spark, implemented as a custom `SparkListener`.  
It shows how Spark can be extended to automatically detect **load imbalance / data skew** and emit a **machine-readable event** suitable for automated remediation.

The demo includes:

- A Spark job that deliberately creates a skewed workload.
- A `SparkListener` that detects skew in task execution times.
- A simple external “self-healing” script that reacts to skew events.

---

## 1. Motivation

Scheduling-related performance issues such as **data skew** and **load imbalance** remain difficult to diagnose automatically in open-source systems like Spark.

Current limitations:

- detection of skew requires **manual inspection** of the Spark UI and logs;
- Spark does not emit a clear **real-time signal** that:  
  *“this stage is suffering from scheduling-related performance problems”*;
- automated remediation tools cannot act without such a signal.

This project implements a small but practical step towards **automated remediation**:

> A lightweight driver-side skew detector that emits a structured event whenever scheduling-related issues occur.

---

## 2. How it works

### 2.1. SkewDetectionListener (core component)

The listener:

1. Subscribes to Spark events:
   - `SparkListenerTaskEnd`
   - `SparkListenerStageCompleted`
2. Collects per-task execution durations.
3. On stage completion:
   - computes:
     - mean task duration,
     - variance of task durations;
   - if:  
     **variance > mean × threshold**  
     → considers the stage skewed.
4. Emits:
   - a human-readable warning, and  
   - a machine-readable event:

     ```
     SCHEDULING_SKEW_DETECTED stageId=... tasks=... meanMs=... variance=... factor=...
     ```

This event can be consumed by external controllers (Kubernetes operators, alerting systems, etc.)

---

### 2.2. SkewDemoJob (Spark job with real skew)

The demo job creates a highly skewed dataset:

- key `"HOT_KEY"` appears **1,000,000** times,
- keys `"a"`, `"b"`, `"c"`, `"d"`, `"e"` appear once.

A `reduceByKey` operation produces:

- one extremely heavy task,
- many trivial tasks.

This reliably triggers **task duration skew**.

---

### 2.3. watch_skew.sh (self-healing demo)

This small script:

- reads Spark logs from `stdin`,
- prints them unchanged,
- and whenever it sees `SCHEDULING_SKEW_DETECTED`,  
  prints an additional message:

[SELF-HEALING DEMO] Detected scheduling skew event!
Here we could automatically trigger remediation logic.


This simulates how an external self-healing system could react.

---

## 3. Project structure
```yml
spark-scheduling-skew-demo/
  pom.xml
  src/
    main/
      java/
        org/example/spark/skew/
          SkewDetectionListener.java
          SkewDemoJob.java
  watch_skew.sh
```
---

## 4. Build instructions

Requirements:

- Java 11+
- Maven
- Apache Spark 3.x (e.g. spark-3.5.x-bin-hadoop3)

Build:

```bash
mvn clean package
```
## 5. Run the demo
5.1 Basic run
```bash
/path/to/spark/bin/spark-submit \
  --class org.example.spark.skew.SkewDemoJob \
  --master "local[*]" \
  --conf spark.extraListeners=org.example.spark.skew.SkewDetectionListener \
  target/spark-scheduling-skew-demo-1.0-SNAPSHOT.jar
```
Expected output includes:
aggregation results:
```puthon-repl
HOT_KEY -> 1000000
a -> 1
...
```
skew warning:
```yaml
WARN Scheduling skew detected in stage 1: ...
```
machine-readable event:
```yaml
SCHEDULING_SKEW_DETECTED stageId=1 tasks=100 ...
```
5.2 Run with self-healing demo
Make script executable:
```bash
chmod +x watch_skew.sh
```
Run:
```bash
/path/to/spark/bin/spark-submit \
  --class org.example.spark.skew.SkewDemoJob \
  --master "local[*]" \
  --conf spark.extraListeners=org.example.spark.skew.SkewDetectionListener \
  target/spark-scheduling-skew-demo-1.0-SNAPSHOT.jar \
  2>&1 | ./watch_skew.sh
```
You will see the self-healing message:
```python-repl
>>> [SELF-HEALING DEMO] Detected scheduling skew event!
>>> Here we could automatically trigger remediation logic.
```
## 6. Why this matters (link to automated remediation)
This contribution addresses a known challenge in open-source distributed systems:
>Spark does not provide real-time, machine-readable detection of ?scheduling-related performance issues.

This project fills that gap:
- automatic detection of skew → no manual UI inspection;
- structured events → easy integration with monitoring / operators;
- safe, driver-side logic → no changes to the scheduler needed.

It provides the missing building block needed to implement:
- dynamic repartitioning,
- auto-tuning of Spark configs,
- intelligent speculative execution,
- job resubmission policies,
- Kubernetes operators for self-healing pipelines.

## 7. Future improvements
Export skew data via Spark’s MetricsSystem.
Add Prometheus metrics for integration with alerting systems.
Use advanced statistical methods for skew detection.
Implement a real Kubernetes operator that reacts to skew events.
Integrate with adaptive execution (AQE) studies.
