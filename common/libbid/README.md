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
arithmetic use Knuth limbs. Combo-field unpack/pack uses Bid64-style masks on
the last packed word (sign, NaN/Inf, Form1/Form2, exponent). In-range results
that already fit in `p` digits skip `decimalDigits`. Parse is still `DecNum`.
These numbers are not comparable to the specialized 64/128 table above.

Object `BidK` API (each call allocates a result encoding):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 39.14 | 21.33 | 0.55x | 154.89 | 24.86 | 0.16x |
| add (mixed quantum) | 183.42 | 115.39 | 0.63x | 688.76 | 165.56 | 0.24x |
| add (full precision) | 164.30 | 149.23 | 0.91x | 825.98 | 252.59 | 0.31x |
| subtract | 49.89 | 31.69 | 0.64x | 184.20 | 35.51 | 0.19x |
| multiply (same quantum) | 291.30 | 215.13 | 0.74x | 1365.06 | 348.80 | 0.26x |
| multiply (mixed quantum) | 106.90 | 85.75 | 0.80x | 456.62 | 139.40 | 0.31x |
| multiply (full precision) | 347.61 | 243.63 | 0.70x | 1499.01 | 368.73 | 0.25x |
| divide (same quantum) | 490.79 | 205.97 | 0.42x | 952.76 | 338.25 | 0.36x |
| divide (mixed quantum) | 521.29 | 235.69 | 0.45x | 954.51 | 345.25 | 0.36x |
| divide (full precision) | 505.03 | 233.66 | 0.46x | 932.44 | 353.55 | 0.38x |
| square root | 4095.69 | 3096.38 | 0.76x | 6506.81 | 5662.26 | 0.87x |
| fused multiply-add | 474.28 | 292.61 | 0.62x | 1797.55 | 427.02 | 0.24x |
| ordered less-than | 40.83 | 9.06 | 0.22x | 130.82 | 9.13 | 0.07x |
| cohort-equal numeric comparison | 43.95 | 8.71 | 0.20x | 131.18 | 9.03 | 0.07x |

`BidKRaw` into a reused `out[]` (the Spark SQL shape; no `BidK` wrapper):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 41.45 | 25.05 | 0.60x | 158.40 | 27.57 | 0.17x |
| add (mixed quantum) | 179.04 | 120.73 | 0.67x | 652.93 | 169.74 | 0.26x |
| add (full precision) | 157.94 | 150.49 | 0.95x | 793.92 | 254.24 | 0.32x |
| subtract | 47.52 | 32.17 | 0.68x | 165.21 | 37.31 | 0.23x |
| multiply (same quantum) | 286.94 | 215.88 | 0.75x | 1333.28 | 351.52 | 0.26x |
| multiply (mixed quantum) | 103.95 | 86.57 | 0.83x | 432.81 | 140.33 | 0.32x |
| multiply (full precision) | 342.07 | 242.33 | 0.71x | 1465.88 | 368.21 | 0.25x |
| divide (same quantum) | 490.60 | 201.58 | 0.41x | 931.81 | 339.27 | 0.36x |
| divide (mixed quantum) | 515.61 | 236.59 | 0.46x | 947.96 | 345.59 | 0.36x |
| divide (full precision) | 501.79 | 236.11 | 0.47x | 927.79 | 354.49 | 0.38x |
| square root | 4121.60 | 3119.16 | 0.76x | 6506.57 | 5668.65 | 0.87x |
| fused multiply-add | 466.93 | 291.84 | 0.63x | 1790.20 | 427.79 | 0.24x |
| ordered less-than | 38.52 | 9.19 | 0.24x | 124.89 | 9.53 | 0.08x |
| cohort-equal numeric comparison | 38.63 | 8.86 | 0.23x | 125.14 | 9.20 | 0.07x |

Decimal160 full-precision add is now within ~10% of `BigDecimal`. Same-quantum
add is still ~1.8x slower (IEEE unpack/pack vs a `BigInteger` add). Decimal256
stays Knuth-limb bound.

`fmod`, integral power, round-to-integral, and scale-by-power-of-ten are
omitted because `BidK` does not implement those kernels yet.

These Decimal160/256 results were collected by `BidKBenchmark` (`quick`) on
October 15, 2026 from this worktree:

- OpenJDK 17.0.15;
- one benchmark thread;
- two 250 ms warmup iterations and three 250 ms measurement iterations
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
