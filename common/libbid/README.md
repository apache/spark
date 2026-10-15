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

# LibBID

LibBID implements IEEE 754 decimal floating-point arithmetic using the
Binary Integer Decimal (BID) encoding. Specialized kernels cover decimal64
and decimal128. A generic BID-k path covers every packed width
`k = 32, 64, ..., 256`, including Decimal96 and Decimal160 through
Decimal256.

The module is an internal Spark foundation. No Spark execution path depends on
it yet, and its Maven and SBT publication tasks are disabled until a consumer
lands.

The Java sources are derived from Intel RDFP 2.0 Update 4 under its
BSD-3-Clause license. Generated table inputs, transformations, and checksums
are documented in [PROVENANCE.md](PROVENANCE.md).

## API semantics

`Bid64` and `Bid128` expose IEEE 754 behavior. Their natural ordering uses
IEEE `totalOrder`, which distinguishes negative zero from positive zero and
orders NaNs by their IEEE representation.

`DecFloatAdapters` exposes the SQL-facing contract: negative and positive zero
compare equal, all NaNs compare equal, and NaN sorts after finite values.
SQL grouping, joins, sorting, and hashing must use these adapters rather than
the object types' natural ordering.

## Performance compared with `BigDecimal`

The following results compare the `Bid64` object API with `BigDecimal` under
`MathContext.DECIMAL64`, and the `Bid128` object API with `BigDecimal` under
`MathContext.DECIMAL128`. All values are average latency in nanoseconds per
operation. The ratio is `BigDecimal / BID`, so a value greater than 1 means
that BID completed the benchmark faster.

| Function / workload | BID64 | BD64 | Ratio | BID128 | BD128 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 4.69 | 21.03 | 4.48x | 11.60 | 17.28 | 1.49x |
| add (mixed quantum) | 10.55 | 38.94 | 3.69x | 88.86 | 92.54 | 1.04x |
| add (full precision) | 9.64 | 43.73 | 4.54x | 67.14 | 113.85 | 1.70x |
| subtract | 4.94 | 25.76 | 5.22x | 11.58 | 26.34 | 2.27x |
| multiply (same quantum) | 17.88 | 23.47 | 1.31x | 69.44 | 158.61 | 2.28x |
| multiply (mixed quantum) | 10.54 | 10.72 | 1.02x | 31.88 | 62.76 | 1.97x |
| multiply (full precision) | 22.78 | 29.08 | 1.28x | 103.24 | 181.58 | 1.76x |
| divide (same quantum) | 20.24 | 27.52 | 1.36x | 161.05 | 155.89 | 0.97x |
| divide (mixed quantum) | 22.23 | 40.01 | 1.80x | 142.83 | 187.67 | 1.31x |
| divide (full precision) | 23.88 | 27.54 | 1.15x | 168.53 | 179.87 | 1.07x |
| square root | 154.30 | 1287.99 | 8.35x | 864.41 | 2639.21 | 3.05x |
| fused multiply-add | 119.67 | 136.99 | 1.14x | 195.11 | 247.74 | 1.27x |
| truncating remainder (`fmod`) | 10.03 | 174.07 | 17.36x | 278.49 | 327.17 | 1.17x |
| positive integral power | 12.92 | 78.52 | 6.08x | 37.88 | 111.36 | 2.94x |
| round to integral | 7.16 | 11.58 | 1.62x | 17.84 | 76.46 | 4.28x |
| scale by power of ten | 3.61 | 3.52 | 0.98x | 3.90 | 3.69 | 0.95x |
| ordered less-than | 5.90 | 4.43 | 0.75x | 10.01 | 4.59 | 0.46x |
| cohort-equal numeric comparison | 8.43 | 7.53 | 0.89x | 14.92 | 19.42 | 1.30x |

The add, multiply, and divide workloads are:

- **same quantum:** operands have similar precision and the same exponent;
- **mixed quantum:** operands have varied precision and different exponents;
- **full precision:** operands use the format's maximum precision and have
  different exponents.

The remaining arithmetic benchmarks use positive, full-precision values in
`[1, 10)`, signed full-precision FMA addends, integral powers from 2 through 5,
and decimal scale changes from -12 through 12.

The FMA comparison performs an exact `BigDecimal` multiply and add followed by
one rounding, matching fused semantics. BID `fmod` and
`BigDecimal.remainder` both use a quotient truncated toward zero. The power
comparison covers positive integral exponents. Round-to-integral compares
numeric results; `BigDecimal` does not retain BID cohorts or signed zero.

These results were collected by the full JMH profile on August 29, 2026 from
libbid-java commit `9936d04`, before the source was incorporated into Spark:

- JMH 1.37;
- OpenJDK 17.0.15;
- Intel Xeon 6975P-C;
- one benchmark thread;
- two forks;
- three 1-second warmup iterations per fork;
- five 1-second measurement iterations per fork;
- 1 GiB fixed heap with `AlwaysPreTouch`; and
- operands prepared outside the measured region.

Microbenchmark results are host- and JVM-specific. Comparisons require the
same JVM, host, benchmark inputs, and JMH settings.

The following results compare the generic `BidK` path with `BigDecimal` under
a `MathContext` of 43 digits (Decimal160) and 70 digits (Decimal256). Values
are nanoseconds per operation. The ratio is `BigDecimal / BID`, so a value
greater than 1 means BID was faster. Decimal160 add/mul/FMA/compare use the
unrolled 3-limb `Wide6` kernel; Decimal160 div/sqrt and all Decimal256
arithmetic use Knuth limbs. Combo-field unpack/pack writes the coefficient
straight into `Wide6` or a `long[]` workspace. Parse is still `DecNum`.
These numbers are not comparable to the specialized 64/128 table above.

Object `BidK` API (each call allocates a result encoding):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 54.72 | 22.21 | 0.41x | 192.68 | 25.44 | 0.13x |
| add (mixed quantum) | 220.08 | 120.11 | 0.55x | 688.28 | 166.11 | 0.24x |
| add (full precision) | 197.97 | 150.55 | 0.76x | 825.71 | 254.25 | 0.31x |
| subtract | 74.62 | 31.92 | 0.43x | 207.64 | 36.26 | 0.17x |
| multiply (same quantum) | 318.87 | 213.42 | 0.67x | 1388.18 | 347.06 | 0.25x |
| multiply (mixed quantum) | 137.14 | 86.38 | 0.63x | 464.44 | 142.08 | 0.31x |
| multiply (full precision) | 374.50 | 243.02 | 0.65x | 1529.90 | 362.53 | 0.24x |
| divide (same quantum) | 529.35 | 211.02 | 0.40x | 970.70 | 336.10 | 0.35x |
| divide (mixed quantum) | 567.77 | 236.84 | 0.42x | 991.54 | 346.40 | 0.35x |
| divide (full precision) | 555.11 | 232.22 | 0.42x | 988.46 | 349.88 | 0.35x |
| square root | 4133.88 | 3123.30 | 0.76x | 6554.41 | 5703.61 | 0.87x |
| fused multiply-add | 503.85 | 292.24 | 0.58x | 1810.38 | 435.17 | 0.24x |
| ordered less-than | 54.58 | 7.30 | 0.13x | 150.51 | 7.33 | 0.05x |
| cohort-equal numeric comparison | 54.04 | 7.32 | 0.14x | 148.76 | 7.22 | 0.05x |

`BidKRaw` into a reused `out[]` (the Spark SQL shape; no `BidK` wrapper):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 65.13 | 24.72 | 0.38x | 190.11 | 29.73 | 0.16x |
| add (mixed quantum) | 216.20 | 126.16 | 0.58x | 686.55 | 169.96 | 0.25x |
| add (full precision) | 192.19 | 149.98 | 0.78x | 811.56 | 254.77 | 0.31x |
| subtract | 66.56 | 33.57 | 0.50x | 190.40 | 35.60 | 0.19x |
| multiply (same quantum) | 309.45 | 214.67 | 0.69x | 1374.86 | 349.69 | 0.25x |
| multiply (mixed quantum) | 134.78 | 86.26 | 0.64x | 518.73 | 140.64 | 0.27x |
| multiply (full precision) | 371.34 | 243.89 | 0.66x | 1512.76 | 362.97 | 0.24x |
| divide (same quantum) | 537.79 | 205.69 | 0.38x | 967.98 | 334.68 | 0.35x |
| divide (mixed quantum) | 568.10 | 239.31 | 0.42x | 990.20 | 349.39 | 0.35x |
| divide (full precision) | 564.08 | 234.81 | 0.42x | 977.49 | 355.59 | 0.36x |
| square root | 4114.05 | 3098.17 | 0.75x | 6548.64 | 5689.21 | 0.87x |
| fused multiply-add | 500.30 | 296.41 | 0.59x | 1824.90 | 438.80 | 0.24x |
| ordered less-than | 52.81 | 7.67 | 0.15x | 152.43 | 7.82 | 0.05x |
| cohort-equal numeric comparison | 52.30 | 7.49 | 0.14x | 150.87 | 7.45 | 0.05x |

Avoiding the object wrapper does not close the gap versus `BigDecimal` on
same-quantum add. Combo-field decode into limbs still dominates; Bid64-class
combo decode would be required to beat `BigDecimal` there. Raw subtract is
the one clear win versus the object API (Decimal160 66.56 ns vs 74.62 ns).

`fmod`, integral power, round-to-integral, and scale-by-power-of-ten are
omitted because `BidK` does not implement those kernels yet.

These Decimal160/256 results were collected by `BidKBenchmark` (`full`) on
October 15, 2026 from this worktree:

- OpenJDK 17.0.15;
- one benchmark thread;
- three 1-second warmup iterations and five 1-second measurement iterations
  per method, same-JVM;
- 1 GiB fixed heap with `AlwaysPreTouch`; and
- operands prepared outside the measured region, using the same
  same/mixed/full and unit-interval generators as the JMH suite.

Run the harness with:

```
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
_JAVA_OPTIONS='-Xms1g -Xmx1g -XX:+AlwaysPreTouch' \
build/sbt -Dsbt.override.build.repos=true \
  'libbid/Test/runMain org.bidfp.BidKBenchmark full'
```

## Capabilities not provided by `BigDecimal`

`BigDecimal` is an arbitrary-precision finite decimal type. LibBID implements
the fixed-size IEEE 754 decimal floating-point model, including:

- exact 64-bit decimal64 and 128-bit decimal128 interchange encodings;
- positive and negative zero, positive and negative infinity, quiet NaN,
  signaling NaN, normal values, and subnormal values;
- canonicality checks and complete IEEE decimal classification;
- explicit sticky status flags for invalid operation, denormal operand,
  division by zero, overflow, underflow, and inexact results;
- IEEE quiet and signaling comparisons, unordered predicates, `totalOrder`,
  `totalOrderMag`, cohort equality, and `sameQuantum`;
- cohort- and quantum-aware operations `quantize`, `quantum`, and
  `quantumExponent`;
- adjacent-value operations `nextUp`, `nextDown`, and `nextAfter`;
- IEEE `minNum`, `maxNum`, magnitude variants, and positive difference;
- fused multiply-add with one rounding and IEEE status reporting;
- both IEEE remainder and truncating remainder (`fmod`);
- general decimal floating-point `pow` and `hypot`;
- exponentials `exp`, `expm1`, `exp2`, and `exp10`;
- logarithms `log`, `log1p`, `log2`, and `log10`;
- trigonometric functions `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, and
  `atan2`;
- hyperbolic functions `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, and `atanh`;
- `cbrt`, `erf`, `erfc`, `tgamma`, and `lgamma`;
- conversion between BID and DPD encodings and between decimal64 and
  decimal128; and
- conversions to and from binary32, binary64, and binary128 with explicit
  rounding and status reporting.

The library exposes immutable `Bid64` and `Bid128` objects as well as
allocation-conscious `Bid64Raw` and `Bid128Raw` kernels over encoded bits.
Generic encodings use `FormatParams` plus `BidK`. Transcendental functions
for `k <= 128` convert through decimal128 and the binary128 DPML engine.
`k > 128` libm is out of scope unless a binary256 engine is funded.

Arithmetic and conversion operations report the full IEEE status flags.
Transcendental compatibility specifies invalid-operation and division-by-zero
flags; Intel's reference transcendental vectors do not consistently specify
inexact, overflow, or underflow flags.

## Transcendental accuracy

Transcendental results use the relative ULP limits from Intel RDFP
`readtest.c`:

| Format | Nearest rounding | Directed rounding |
| --- | ---: | ---: |
| decimal64 | 0.55 ULP | 1.05 ULP |
| decimal128 | 2 ULP | 5 ULP |

Nearest rounding includes ties-to-even and ties-away. Directed rounding
includes toward-negative, toward-positive, and toward-zero. The decimal128
limits include the BID-to-binary128 and binary128-to-BID conversion steps.

NaN and infinity results must match the reference encoding exactly. The Intel
transcendental vector contract checks invalid-operation and division-by-zero
status; it does not consistently specify inexact, overflow, or underflow.
