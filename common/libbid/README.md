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
Binary Integer Decimal (BID) encoding. IEEE names each interchange format
by `k` 32-bit storage units; packed width is `32 * k` bits. Decimal32 is
`k=1`, Decimal64 is `k=2`, Decimal96 is `k=3`, Decimal128 is `k=4`,
Decimal160 is `k=5`, through Decimal256 at `k=8`. Specialized kernels cover
Decimal64 and Decimal128. A generic BID-k path covers `k=1` through `k=8`.

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
the object types' natural ordering. `BidK.sqlCompare` / `sqlEquals` /
`sqlHash` follow the same contract for BID-k; `BidK.hashCode` is payload
identity and is not SQL equality.

IEEE BID-k precision is `p = 9k-2` (16, 25, 34, 43, ...). Spark
`DecimalType` allows precision 1 through 38. No IEEE k equals 38.
Decimal128 cannot store DECIMAL(38). Decimal160 (`p=43`) is the smallest
IEEE format that can hold 38 digits; a SQL binding would still need to
quantize to scale, clamp to 38 digits, and reject Inf/NaN. This module
does not define that mapping.

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
greater than 1 means BID was faster. Decimal160 and Decimal192 use the
unrolled `Wide6` kernel; Decimal224 and Decimal256 use `Wide8`. Multiply uses
three coefficient limbs through Decimal192 and four for Decimal224/256. FMA
stays fixed-width unless product alignment can overflow.
Divide is 64-bit Knuth D on those limbs. Sqrt is Heron iteration on `Wide8`
(Decimal160) or 12-limb `Wide12` (Decimal192/224/256), not the
generic 16-limb workspace. Packed and unpacked calls use the same kernels.
Combo-field unpack/pack uses Bid64-style masks on the last packed word.
In-range results that already fit in `p` digits skip `decimalDigits`. Compare
aligns by exponent instead of counting digits. Parse is still `DecNum`.
Repeated arithmetic can stay in `BidUnpacked` (kind, sign, unbiased exponent,
four limbs) and pack only at I/O.
These numbers are not comparable to the specialized 64/128 table above.

`BidK` `k=2` and `k=4` dispatch to those specialists. `k=3` is a Decimal96
specialist (private scratch, two-limb coefficients, three-limb products,
digit-pair sqrt). `k=5` and `k=6` use Wide6; `k=7` and `k=8` use Wide8.
Knuth is retained where it is faster for k=7 multiply and as the
overflow-safe FMA fallback. Libm is Bid128 DPML for `k<=4` and unsupported
for `k>4`.

The table below is `BidKRaw` vs naked `BigDecimal` (`MathContext` precision
`p=9k-2`, `HALF_EVEN`), profile `quick`, 15 Oct 2026. Ratio is
`BigDecimal / BID` (greater than 1 means BID is faster). Units are IEEE k
(Decimal64 is `k=2`).

| Function / workload | k=2 | k=3 | k=4 | k=5 | k=6 | k=7 | k=8 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 2.24x | 1.12x | 1.53x | 0.83x | 0.87x | 0.90x | 0.94x |
| add (mixed quantum) | 2.48x | 1.07x | 1.01x | 0.66x | 0.60x | 0.50x | 0.51x |
| add (full precision) | 2.86x | 1.52x | 1.29x | 0.97x | 0.94x | 0.70x | 0.70x |
| subtract | 2.23x | 1.74x | 1.85x | 0.99x | 1.05x | 1.10x | 1.08x |
| multiply (same quantum) | 1.13x | 1.48x | 2.07x | 1.09x | 1.02x | 0.69x | 0.42x |
| multiply (mixed quantum) | 0.81x | 1.02x | 1.47x | 0.96x | 1.00x | 0.65x | 0.57x |
| multiply (full precision) | 1.13x | 1.24x | 1.55x | 0.98x | 0.88x | 0.67x | 0.41x |
| divide (same quantum) | 0.98x | 0.77x | 0.86x | 0.54x | 0.48x | 0.46x | 0.45x |
| divide (mixed quantum) | 1.40x | 0.63x | 1.03x | 0.58x | 0.55x | 0.48x | 0.48x |
| divide (full precision) | 1.04x | 0.70x | 0.86x | 0.54x | 0.49x | 0.49x | 0.48x |
| square root | 7.39x | 2.45x | 2.88x | 0.89x | 1.03x | 1.26x | 1.09x |
| fused multiply-add | 1.21x | 1.06x | 0.62x | 0.76x | 0.66x | 0.38x | 0.38x |
| ordered less-than | 0.46x | 0.81x | 0.44x | 0.66x | 0.64x | 0.65x | 0.75x |
| cohort-equal numeric comparison | 0.45x | 0.84x | 0.49x | 0.59x | 0.58x | 0.57x | 0.58x |

Run `build/sbt "libbid/Test/runMain org.bidfp.BidKBenchmark kscan quick"`
to refresh. Compare rows for `k=2` and `k=4` now call
`DecFloatAdapters` (not reflected in the compare ratios above).

Object `BidK` API (each call allocates a result encoding):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 48.04 | 36.18 | 0.75x | 52.75 | 38.54 | 0.73x |
| add (mixed quantum) | 187.28 | 119.56 | 0.64x | 366.43 | 166.98 | 0.46x |
| add (full precision) | 175.23 | 147.08 | 0.84x | 363.82 | 249.88 | 0.69x |
| subtract | 47.78 | 37.25 | 0.78x | 50.46 | 38.43 | 0.76x |
| multiply (same quantum) | 294.80 | 213.79 | 0.73x | 787.23 | 351.39 | 0.45x |
| multiply (mixed quantum) | 109.26 | 87.21 | 0.80x | 224.77 | 141.48 | 0.63x |
| multiply (full precision) | 338.43 | 244.53 | 0.72x | 850.25 | 370.06 | 0.44x |
| divide (same quantum) | 382.57 | 209.77 | 0.55x | 740.56 | 337.33 | 0.46x |
| divide (mixed quantum) | 384.74 | 231.95 | 0.60x | 702.07 | 341.64 | 0.49x |
| divide (full precision) | 394.45 | 229.84 | 0.58x | 753.79 | 349.81 | 0.46x |
| square root | 3918.49 | 3102.12 | 0.79x | 5867.63 | 5670.91 | 0.97x |
| fused multiply-add | 451.70 | 292.41 | 0.65x | 1105.27 | 433.36 | 0.39x |
| ordered less-than | 21.43 | 10.03 | 0.47x | 23.98 | 10.36 | 0.43x |
| cohort-equal numeric comparison | 20.82 | 9.59 | 0.46x | 24.11 | 10.09 | 0.42x |

`BidKRaw` into a reused `out[]` (packed Spark SQL shape; no `BidK` wrapper):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 38.51 | 29.00 | 0.75x | 39.62 | 30.42 | 0.77x |
| add (mixed quantum) | 181.03 | 122.63 | 0.68x | 331.86 | 168.61 | 0.51x |
| add (full precision) | 162.98 | 150.77 | 0.93x | 354.74 | 253.32 | 0.71x |
| subtract | 39.27 | 35.88 | 0.91x | 42.42 | 39.42 | 0.93x |
| multiply (same quantum) | 285.41 | 216.71 | 0.76x | 769.42 | 356.01 | 0.46x |
| multiply (mixed quantum) | 105.93 | 87.61 | 0.83x | 219.57 | 141.24 | 0.64x |
| multiply (full precision) | 331.05 | 244.94 | 0.74x | 838.08 | 370.21 | 0.44x |
| divide (same quantum) | 377.45 | 204.46 | 0.54x | 735.31 | 334.23 | 0.45x |
| divide (mixed quantum) | 374.99 | 231.73 | 0.62x | 693.08 | 341.83 | 0.49x |
| divide (full precision) | 385.73 | 230.22 | 0.60x | 741.01 | 350.66 | 0.47x |
| square root | 3853.61 | 3105.21 | 0.81x | 5865.70 | 5667.70 | 0.97x |
| fused multiply-add | 434.41 | 295.82 | 0.68x | 1095.54 | 433.79 | 0.40x |
| ordered less-than | 15.44 | 10.10 | 0.65x | 15.63 | 10.43 | 0.67x |
| cohort-equal numeric comparison | 14.92 | 9.61 | 0.64x | 16.92 | 10.80 | 0.64x |

`BidUnpacked` reused operands and result (unpack once; no IEEE pack per op):

| Function / workload | BID160 | BD160 | Ratio | BID256 | BD256 | Ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| add (same quantum) | 33.40 | 27.81 | 0.83x | 35.02 | 29.56 | 0.84x |
| add (mixed quantum) | 169.42 | 121.87 | 0.72x | 325.60 | 168.86 | 0.52x |
| add (full precision) | 153.02 | 150.27 | 0.98x | 346.94 | 252.36 | 0.73x |
| subtract | 33.48 | 36.37 | 1.09x | 39.10 | 39.70 | 1.02x |
| multiply (same quantum) | 274.88 | 216.26 | 0.79x | 781.61 | 355.48 | 0.45x |
| multiply (mixed quantum) | 94.81 | 86.84 | 0.92x | 211.48 | 140.80 | 0.67x |
| multiply (full precision) | 322.05 | 244.26 | 0.76x | 853.63 | 369.74 | 0.43x |
| divide (same quantum) | 367.50 | 204.64 | 0.56x | 721.24 | 333.52 | 0.46x |
| divide (mixed quantum) | 362.19 | 231.42 | 0.64x | 675.60 | 340.79 | 0.50x |
| divide (full precision) | 374.32 | 229.78 | 0.61x | 727.26 | 349.75 | 0.48x |
| square root | 3569.06 | 3107.42 | 0.87x | 5286.85 | 5659.92 | 1.07x |
| fused multiply-add | 418.55 | 295.36 | 0.71x | 1113.92 | 433.35 | 0.39x |
| ordered less-than | 17.96 | 10.07 | 0.56x | 21.59 | 10.57 | 0.49x |
| cohort-equal numeric comparison | 17.67 | 9.61 | 0.54x | 22.07 | 10.05 | 0.46x |

Unpacked same-quantum add is 33 / 35 ns (0.83x / 0.84x). Compare is
~18 / ~22 ns, still about 2x `BigDecimal`. Decimal160 divide is ~370 ns
(~0.56x). Decimal256 divide is ~0.46x. Unpacked Decimal256 sqrt is the
first BID-k row faster than `BigDecimal` (5287 vs 5660 ns, 1.07x); Heron
is still divide-bound.

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
Generic encodings use `FormatParams` plus packed `BidK`. Repeated arithmetic
can stay in reusable `BidUnpacked` values (kind, sign, unbiased exponent,
four coefficient limbs) and pack to IEEE words only at I/O. Transcendental
functions for `k<=4` (packed width 128) convert through decimal128 and the
binary128 DPML engine. `k>4` libm is out of scope unless a binary256 engine
is funded.

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
