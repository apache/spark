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

# LibBID generated data provenance

## Intel source release

The Intel-derived algorithms, tables, and vectors come from the Intel Decimal
Floating-Point Math Library 2.0 Update 4 (Intel RDFP 2.0 Update 4), downloaded
from:

[Intel Decimal Floating-Point Math Library][intel-rdfp]

The source release is BSD-3-Clause licensed. It is not the GPL-licensed GCC
copy of libbid. The Intel C distribution is not vendored in Spark.

## DPML QUAD UX tables

`tools/gen_dpml_tables.py` reads the following files from the source release's
`LIBRARY/float128` directory. The generator verifies these SHA-256 values
before parsing:

| Intel input | SHA-256 |
| --- | --- |
| `dpml_cons_x.h` | `e50dd6cf358089cc7e4b26909ebe6c8766636edf9569d93520d9d7a957e2167a` |
| `dpml_exp_x.h` | `23692f8af7e70d530d84d72a2a74219f85acca8051e3deae466a908222e9976d` |
| `dpml_log_x.h` | `1616f505e218f4fc32aedacf79cdda065c520b96047bed5b85686b6d8b5d249b` |
| `dpml_pow_x.h` | `9e1c8c681f35a6ae61cbe1e8d414ce99ca1effbe2273edd948e5ada7c5058809` |
| `dpml_cbrt_x.h` | `833361bed6e75dae604f0b731a2759ba3bc57a25e7d167548a98a4555d839706` |
| `dpml_trig_x.h` | `dc806eaf9644cc267cb2f99a512e5e06458adbdd19100cee761108b976edf40d` |
| `dpml_inv_trig_x.h` | `b915dc88faf53a20425ab5cc0b5ef1cf22644accf7c4d7af2b602c30103676e5` |
| `dpml_inv_hyper_x.h` | `20e123428492951122b8374b358766de9cc47b929c47e90825d2fde7d88fe7e2` |
| `dpml_erf_x.h` | `e734ebb7bcd4133f0e356b75d48125574fe19fe5ff83f8b7633efb145cb3e748` |
| `dpml_lgamma_x.h` | `73bac30fba98ea5e16b14bc684b303b491cc457035f2f74b5387ef4bf11e6b7a` |
| `dpml_four_over_pi.c` | `ed308d018df97d206854e5398295fbc8b7f3b5c46a27e624f935694466d6c112` |

The generator was adapted from `srielau/libbid-java` commit
`e2d8d7ba968ac0f99940470601928b8ccbd78b30`. The Spark adaptation:

1. parses Intel `DATA_*`, sign, exponent, and offset macros;
2. applies Intel's little-endian `DATA_*` layout;
3. combines adjacent unsigned 32-bit words into Java `long` bit patterns;
4. emits scalar offsets and degree constants used by the Java kernels;
5. exposes table words through an immutable `TableData` view; and
6. emits the complete Intel BSD-3-Clause header.

Run the reproducibility check against an unmodified Intel source tree:

```bash
python3 common/libbid/tools/gen_dpml_tables.py \
  --src /path/to/IntelRDFPMathLib20U4/LIBRARY/float128 \
  --check
```

`GeneratedDataChecksumTest` hashes each table as the ordered concatenation of
its words in big-endian byte order. Expected SHA-256 values are:

| Generated table | Word count | SHA-256 |
| --- | ---: | --- |
| `ConsX` | 20 | `b51093963bddb9d80d6ae4fa84d23ead6d0f052c575ba214dfb032da4722bddd` |
| `ExpX` | 169 | `162b9a8ba716e85fd1db00c2ad45c50b209f7596053f13a4a7171b52f6f4735b` |
| `LogX` | 62 | `ca7b9e324f6d3ffd54cce551cc330e13731bd9c339d364c03cbf8969c78ee8e9` |
| `PowX` | 131 | `c79bbb49b289d309536bc4c72f51891cc7f84378d817501e15238b31de8c3074` |
| `CbrtX` | 13 | `6b652b78cdd11efe8209941a14fffce44d5805b7c9e82f8ad2711e433d9c3fa8` |
| `TrigX` | 129 | `461c4295e521616113d36d1f9e5992afe695e118cabda39a20e9cbf0ddea23b1` |
| `InvTrigX` | 164 | `51651509bd5c3fda2179ba65b19895808a8e66041f7303c9fb905a0e7e4ea092` |
| `InvHyperX` | 14 | `a9ecf60dc85749d76d6213e242c95136036bb8ffe5ef9abda07c8d6057313599` |
| `ErfX` | 171 | `8ee8e332b316d9bc3a46576606b72476da1c787e77501daee8a9a9ca0b4fb657` |
| `LgammaX` | 121 | `d823484cccf35200a87d55a94fd5557b552f2612fa38b7102d657d0ff0f2303b` |
| `FourOverPi` | 263 | `16b24b3d32f0713338d7f62c235fbc7c1963686e6f1b01c92d12249b7441a8bf` |

## Other generated and reference resources

`bid128_sin_moduli.bin` is the Intel `bid_sin_table` from
`LIBRARY/src/bid128_sin.c`. Its 6,147 rows contain six unsigned 64-bit words
each, serialized in Java `DataOutput` big-endian order. The Intel source file
SHA-256 is
`738ef9c04cd1a4f8d69e35145ffcccf7e594c06ea797b3eb719f82d6f2b78e86`;
the generated resource SHA-256 is
`84a7a565d0652390c1e1dd90d2b4b612ba8a42870b0263a886621a15d61aca93`.

`src/test/resources/org/bidfp/readtest.in` is retained byte-for-byte from
Intel RDFP 2.0 Update 4 `TESTS/readtest.in`. Both source and retained resource
have SHA-256
`bb7f2ccae62f5d6d1b6261b891194668506b3291b065feda8531cb90d113abd2`.

`intel-f128-oracle.txt` was produced by Intel's `bid_f128_*` entry points with
`CALL_BY_REF=0`, `GLOBAL_RND=0`, `GLOBAL_FLAGS=0`, and
`USE_COMPILER_F128_TYPE=0`. Packed binary128 values are written high word
first. Its SHA-256 is
`65b8af10af127776f8df911bd2099b450143684a424e91b117ba09ddd7744e99`.

[intel-rdfp]:
  https://www.intel.com/content/www/us/en/developer/articles/tool/intel-decimal-floating-point-math-library.html
