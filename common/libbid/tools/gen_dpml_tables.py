#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generate Java DPML QUAD UX tables from Intel RDFP 2.0 Update 4."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_OUT = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "main"
    / "java"
    / "org"
    / "bidfp"
    / "binary128"
    / "tables"
)

INTEL_HEADER = """\
/*
 * Copyright (c) 2007-2025, Intel Corp.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   * Neither the name of Intel Corporation nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * Generated from Intel RDFP 2.0 Update 4 float128 UX table sources.
 * Do not edit by hand; regenerate with common/libbid/tools/gen_dpml_tables.py.
 */
"""

SOURCE_SHA256 = {
    "dpml_cbrt_x.h": "833361bed6e75dae604f0b731a2759ba3bc57a25e7d167548a98a4555d839706",
    "dpml_cons_x.h": "e50dd6cf358089cc7e4b26909ebe6c8766636edf9569d93520d9d7a957e2167a",
    "dpml_erf_x.h": "e734ebb7bcd4133f0e356b75d48125574fe19fe5ff83f8b7633efb145cb3e748",
    "dpml_exp_x.h": "23692f8af7e70d530d84d72a2a74219f85acca8051e3deae466a908222e9976d",
    "dpml_inv_hyper_x.h": (
        "20e123428492951122b8374b358766de9cc47b929c47e90825d2fde7d88fe7e2"
    ),
    "dpml_inv_trig_x.h": (
        "b915dc88faf53a20425ab5cc0b5ef1cf22644accf7c4d7af2b602c30103676e5"
    ),
    "dpml_lgamma_x.h": (
        "73bac30fba98ea5e16b14bc684b303b491cc457035f2f74b5387ef4bf11e6b7a"
    ),
    "dpml_log_x.h": "1616f505e218f4fc32aedacf79cdda065c520b96047bed5b85686b6d8b5d249b",
    "dpml_pow_x.h": "9e1c8c681f35a6ae61cbe1e8d414ce99ca1effbe2273edd948e5ada7c5058809",
    "dpml_trig_x.h": "dc806eaf9644cc267cb2f99a512e5e06458adbdd19100cee761108b976edf40d",
    "dpml_four_over_pi.c": (
        "ed308d018df97d206854e5398295fbc8b7f3b5c46a27e624f935694466d6c112"
    ),
}

EXPECTED_BYTES = {
    "ConsX": 160,
    "ExpX": 1352,
    "LogX": 496,
    "PowX": 1048,
    "CbrtX": 104,
    "TrigX": 1032,
    "InvTrigX": 1312,
    "InvHyperX": 112,
    "ErfX": 1368,
    "LgammaX": 968,
    "FourOverPi": 2104,
}

WORD = r"(?:0x[0-9a-fA-F]*-\d+|0x[0-9a-fA-F]+)"


@dataclass
class Table:
    name: str
    source: str
    words: list[int] = field(default_factory=list)
    defines: dict[str, int] = field(default_factory=dict)


def u32(value: int) -> int:
    return value & 0xFFFFFFFF


def parse_word(token: str) -> int:
    negative = re.fullmatch(r"0x[0-9a-fA-F]*-(\d+)", token)
    return u32(-int(negative.group(1))) if negative else u32(int(token, 16))


def parse_exponent(token: str) -> int:
    negative = re.match(r"^0*(-\d+)$", token)
    return int(negative.group(1)) if negative else int(token, 10)


def table_body(text: str) -> str:
    instantiate = re.search(r"#if\s+INSTANTIATE_TABLE\s*(.*?)\s*#endif", text, re.S)
    region = (
        instantiate.group(1)
        if instantiate and "PACKED_CONSTANT_TABLE" in instantiate.group(1)
        else text
    )
    match = re.search(
        r"(?:static\s+)?const\s+TABLE_UNION\s+\w+\s*\[\s*\]\s*=\s*\{(.*?)\};",
        region,
        re.S,
    )
    if not match:
        raise ValueError("TABLE_UNION initializer not found")
    return match.group(1)


def parse_defines(text: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, value in re.findall(r"#\s*define\s+(\w+)\s+(.+)$", text, re.M):
        offset = re.search(r"\(char \*\)\s*\w+\s*\+\s*(\d+)\)", value)
        integer = re.fullmatch(r"\s*(\d+)\s*", value)
        signed = re.search(
            r"\(\s*signed\s+(?:__int64|long long)\s*\)\s*0x([0-9a-fA-F]+)",
            value,
        )
        if offset:
            result[name] = int(offset.group(1))
        elif signed:
            result[name] = int(signed.group(1), 16)
        elif integer:
            result[name] = int(integer.group(1))
    return result


def parse_header(path: Path, name: str) -> Table:
    text = path.read_text(encoding="utf-8", errors="replace")
    body = re.sub(r"/\*.*?\*/|//.*?$", " ", table_body(text), flags=re.S | re.M)
    token = re.compile(
        rf"DATA_(?:1x2|2x2|4R|4)\s*\(([^)]*)\)|\b(POS|NEG)\b|"
        r"(?<![0-9a-fxA-F])(0*-?\d+)(?![0-9a-fxA-F])"
    )
    words: list[int] = []
    need_exponent = False
    for match in token.finditer(body):
        if match.group(1) is not None:
            words.extend(parse_word(value) for value in re.findall(WORD, match.group(1)))
            need_exponent = False
        elif match.group(2) is not None:
            words.append(0 if match.group(2) == "POS" else 0x80000000)
            need_exponent = True
        elif need_exponent:
            words.append(u32(parse_exponent(match.group(3))))
            need_exponent = False
    return Table(name, path.name, words, parse_defines(text))


def parse_four_over_pi(path: Path) -> Table:
    text = path.read_text(encoding="utf-8", errors="replace")
    match = re.search(r"__four_over_pi\s*\[\s*\]\s*=\s*\{(.*?)\};", text, re.S)
    if not match:
        raise ValueError("__four_over_pi initializer not found")
    values = [
        int(token.removesuffix("ull").removesuffix("ULL"), 16)
        for token in re.findall(r"0x[0-9a-fA-F]+(?:ull)?", match.group(1), re.I)
    ]
    words = [part for value in values for part in (u32(value), u32(value >> 32))]
    defines = {
        "FOUR_OV_PI_ZERO_PAD_LEN": 138,
        "BITS_PER_DIGIT": 64,
        "LENGTH": len(values),
    }
    names = (
        "FOUR_OV_PI_ZERO_PAD_LEN|BITS_PER_DIGIT|NUM_INDEX_BITS|"
        "NUM_OCTANT_BITS|MIN_OVERHANG"
    )
    for name, value in re.findall(rf"#\s*define\s+({names})\s+(\d+)", text):
        defines[name] = int(value)
    return Table("FourOverPi", path.name, words, defines)


def longs(words: list[int]) -> list[int]:
    return [
        words[index] | ((words[index + 1] if index + 1 < len(words) else 0) << 32)
        for index in range(0, len(words), 2)
    ]


def emit(table: Table) -> str:
    values = longs(table.words)
    lines = INTEL_HEADER.rstrip().splitlines()
    lines += [
        "package org.bidfp.binary128.tables;",
        "",
        "/**",
        f" * QUAD UX table from Intel {{@code {table.source}}}.",
        f" * Little-endian memory image as {{@code long[]}} ({len(table.words) * 4} bytes).",
        " */",
        f"public final class {table.name} {{",
        f"  private {table.name}() {{",
        "  }",
        "",
        "  /** Total table size in bytes (Intel comment offsets). */",
        f"  public static final int BYTE_LENGTH = {len(table.words) * 4};",
        "",
    ]
    def define_order(item: tuple[str, int]) -> tuple[int, str]:
        name = item[0]
        address = name.endswith("_ADDRESS") or name.endswith("_ARRAY")
        priority = 0 if "CLASS" in name or name.startswith("UX_") or address else 1
        return priority, name

    for name, value in sorted(table.defines.items(), key=define_order):
        kind = "int" if value <= 0x7FFFFFFF else "long"
        literal = str(value) if kind == "int" else f"0x{value & 0xFFFFFFFFFFFFFFFF:X}L"
        line = f"  public static final {kind} {name} = {literal};"
        if len(line) <= 100 and len(name) <= 60:
            lines.append(line)
    lines += [
        "",
        "  /** Little-endian table words (two u32s per long). */",
        "  public static final TableData TABLE = new TableData(new long[] {",
    ]
    for index in range(0, len(values), 2):
        row = ", ".join(
            f"0x{value & 0xFFFFFFFFFFFFFFFF:016X}L" for value in values[index:index + 2]
        )
        lines.append("      " + row + ("," if index + 2 < len(values) else ""))
    lines += ["  });", "}", ""]
    too_long = [line for line in lines if len(line) > 100]
    if too_long:
        raise ValueError(f"{table.name}: generated line exceeds 100 characters")
    return "\n".join(lines)


SPECS = [
    ("ConsX", "dpml_cons_x.h"),
    ("ExpX", "dpml_exp_x.h"),
    ("LogX", "dpml_log_x.h"),
    ("PowX", "dpml_pow_x.h"),
    ("CbrtX", "dpml_cbrt_x.h"),
    ("TrigX", "dpml_trig_x.h"),
    ("InvTrigX", "dpml_inv_trig_x.h"),
    ("InvHyperX", "dpml_inv_hyper_x.h"),
    ("ErfX", "dpml_erf_x.h"),
    ("LgammaX", "dpml_lgamma_x.h"),
]


def check_source(path: Path) -> None:
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != SOURCE_SHA256[path.name]:
        raise ValueError(f"{path.name}: source SHA-256 {actual} is not RDFP 2.0 Update 4")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--src",
        required=True,
        type=Path,
        help="Intel LIBRARY/float128 directory",
    )
    parser.add_argument("--out", default=DEFAULT_OUT, type=Path)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify generated files without writing",
    )
    args = parser.parse_args()

    paths = [args.src / filename for _, filename in SPECS]
    paths.append(args.src / "dpml_four_over_pi.c")
    for path in paths:
        check_source(path)
    tables = [parse_header(args.src / filename, name) for name, filename in SPECS]
    tables.append(parse_four_over_pi(args.src / "dpml_four_over_pi.c"))

    errors = []
    for table in tables:
        size = len(table.words) * 4
        if size != EXPECTED_BYTES[table.name]:
            errors.append(f"{table.name}: {size} bytes, expected {EXPECTED_BYTES[table.name]}")
        output = args.out / f"{table.name}.java"
        generated = emit(table)
        if args.check:
            if not output.is_file() or output.read_text(encoding="utf-8") != generated:
                errors.append(f"{output}: not reproducible")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(generated, encoding="utf-8")
            print(f"wrote {output}")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("DPML table verification passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
