<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# LibMPD

LibMPD is a precision-capped Java decimal floating-point engine for evaluating
General Decimal Arithmetic / IEEE 754-style DECFLOAT support in Spark.

It stores values as a decimal coefficient in base `10^9` limbs plus a separate
exponent, following the in-memory shape of libmpdec (`mpd_t`), not IEEE BID or
DPD interchange encodings. Extreme-precision paths (FNT/FFT multiply, unbounded
precision) are intentionally omitted. The hard cap is 100 significant digits,
which covers IEEE-style formats through Decimal256 (`p = 70`) and DECFLOAT-like
`k = 9` (`p = 79`).

The module is an internal foundation. No Spark execution path depends on it
yet, and Maven / SBT publication is disabled until a consumer lands.

## Scope

Supported quiet operations:

- add, subtract, multiply, divide
- compare (numeric)
- square root
- fused multiply-add (exact mul+add then one round)
- quantize / round-to-precision
- parse / toString
- non-finite values: +/-Inf, qNaN, sNaN, signed zero
- context: precision, Emin/Emax, clamp, rounding, status flags

Not in this cut: transcendental `exp` / `ln` / `log10` / `pow`, trig, or
arbitrary-precision FFT multiply.

## Run the harness

```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
  build/sbt -Dsbt.override.build.repos=true \
  "libmpd/Test/runMain org.mpdec.MpdBenchmark quick"
```

Profiles: `quick`, `iter`, `full`. Optional `p=16`, `p=34`, or `p=70`
selects a single precision. Default tables cover 16, 34, and 70 digits versus
`BigDecimal` under a matching `MathContext`.

## Comparing with LibBID without importing its sources

Do not merge `common/libbid` into this branch for the experiment. Prefer one of:

1. **Side-by-side numbers (simplest)**  
   Run `MpdBenchmark` here and `BidKBenchmark` / Bid64 harness on
   `SPARK-59111-bidk`, same host/JVM/profile, and compare the printed tables.

2. **Test-scoped jar only**  
   Build libbid once on the bidk branch, then point this module's tests at that
   jar without adding sources:

   ```bash
   export MPD_BID_JAR=/path/to/spark-libbid_2.13-5.0.0-SNAPSHOT.jar
   JAVA_HOME=... build/sbt -Dsbt.override.build.repos=true      "libmpd/Test/runMain org.mpdec.MpdBenchmark quick bid"
   ```

   The harness uses reflection against `org.bidfp.Bid64` / `Bid128` when the
   class is present; if the jar is absent it skips the BID columns.

3. **Published test dependency**  
   Later, if libbid is published to the local Maven repo, add a
   `<scope>test</scope>` dependency on `spark-libbid_2.13` only. Still no
   source merge into this branch.

Avoid copying Bid64/Bid128 sources into `libmpd`: that re-creates the LOC
problem and couples the experiments.
