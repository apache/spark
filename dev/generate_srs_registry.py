#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Generate the Spatial Reference System (SRS) registry for Apache Spark.

Downloads CRS definitions from the PROJ (Cartographic Projections and
Coordinate Transformations Library) GitHub repository and generates a CSV
file used by Spark's SpatialReferenceSystemCache on both the JVM and Python
sides.

PROJ is a C/C++ library (https://proj.org/) that maintains the authoritative
EPSG and ESRI CRS databases. This script extracts SRID metadata from PROJ's
SQL source files, which are plain-text INSERT statements parseable without
SQLite.

The script produces entries from the following PROJ SQL files:
  - geodetic_crs.sql     (EPSG geodetic CRS: geographic, geocentric, etc.)
  - projected_crs.sql    (EPSG projected CRS)
  - compound_crs.sql     (EPSG compound CRS)
  - vertical_crs.sql     (EPSG vertical CRS)
  - engineering_crs.sql  (EPSG engineering CRS)
  - esri.sql             (ESRI geodetic, projected, compound, vertical, engineering CRS)

Additionally, the following special entries are added:
  - SRID 0    -> SRID:0     (Spark convention: Cartesian, no defined SRS)
  - SRID 4267 -> OGC:CRS27  (OGC standardization of NAD27)
  - SRID 4269 -> OGC:CRS83  (OGC standardization of NAD83)
  - SRID 4326 -> OGC:CRS84  (OGC standardization of WGS 84)

Prerequisites:
    Python 3.9+ (no third-party packages required)

Usage:
    # Generate from default PROJ version:
    python dev/generate_srs_registry.py

    # Generate from a specific PROJ version:
    python dev/generate_srs_registry.py --proj-version 9.7.1

    # Verify the generated files:
    wc -l sql/api/src/main/resources/org/apache/spark/sql/srs_registry.csv
    wc -l python/pyspark/sql/srs_registry.csv

Upgrade workflow:
    1. Change --proj-version to the new PROJ release tag
    2. Run this script
    3. Review the diff (git diff) to see which SRIDs were added/removed
    4. Run the SRS-related tests to verify correctness
"""

import argparse
import csv
import os
import re
import ssl
import sys
import urllib.request

# Default PROJ version to download SQL files from.
DEFAULT_PROJ_VERSION = "9.7.1"

# URL template for raw SQL files from the PROJ GitHub repository.
PROJ_RAW_URL = "https://raw.githubusercontent.com/OSGeo/PROJ/{version}/data/sql/{filename}"

# PROJ SQL files to download. EPSG CRS definitions are spread across dedicated
# per-table files, while ESRI definitions are all in a single file.
PROJ_SQL_FILES = [
    "geodetic_crs.sql",
    "projected_crs.sql",
    "compound_crs.sql",
    "vertical_crs.sql",
    "engineering_crs.sql",
    "esri.sql",
]

# OGC special cases: these SRIDs are standardized under OGC rather than EPSG.
# The OGC string IDs override the EPSG ones for these SRIDs.
OGC_SPECIAL_CASES = {
    4267: "OGC:CRS27",  # NAD27
    4269: "OGC:CRS83",  # NAD83
    4326: "OGC:CRS84",  # WGS 84
}

# Output paths for the generated CSV, relative to the Spark repo root.
JAVA_RESOURCE_PATH = os.path.join(
    "sql", "api", "src", "main", "resources", "org", "apache", "spark", "sql", "srs_registry.csv"
)
PYTHON_RESOURCE_PATH = os.path.join("python", "pyspark", "sql", "srs_registry.csv")


def download_sql(version, filename):
    """Download a SQL file from the PROJ GitHub repository at a pinned version tag."""
    url = PROJ_RAW_URL.format(version=version, filename=filename)
    print(f"  Downloading {url}")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(url, context=ctx) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        print(f"ERROR: Failed to download {url}: {e}", file=sys.stderr)
        print(f"Check that PROJ version '{version}' exists as a GitHub tag.", file=sys.stderr)
        sys.exit(1)


def parse_sql_values(values_str):
    """
    Parse the comma-separated fields inside a SQL VALUES(...) clause.

    Handles SQL-quoted strings (single quotes with '' escape for literal
    apostrophes) and unquoted NULL / integer literals.

    Returns a list of string values, with NULL represented as None.
    """
    fields = []
    i = 0
    n = len(values_str)
    while i < n:
        if values_str[i] in (" ", "\t"):
            i += 1
            continue
        if values_str[i] == "'":
            # Quoted string: scan until closing quote ('' is an escaped quote).
            i += 1
            buf = []
            while i < n:
                if values_str[i] == "'" and i + 1 < n and values_str[i + 1] == "'":
                    buf.append("'")
                    i += 2
                elif values_str[i] == "'":
                    i += 1
                    break
                else:
                    buf.append(values_str[i])
                    i += 1
            fields.append("".join(buf))
        elif values_str[i : i + 4].upper() == "NULL":
            fields.append(None)
            i += 4
        else:
            # Unquoted literal (integer, etc.)
            j = i
            while j < n and values_str[j] not in (",", ")"):
                j += 1
            fields.append(values_str[i:j].strip())
            i = j
        # Skip comma separator.
        while i < n and values_str[i] in (",", " ", "\t"):
            if values_str[i] == ",":
                i += 1
                break
            i += 1
    return fields


def parse_geodetic_crs(sql_content):
    """
    Parse geodetic_crs INSERT statements from SQL content.

    The `type` field (position 4) determines whether the CRS is geographic:
        'geographic 2D', 'geographic 3D' -> geographic
        'geocentric', 'other'            -> non-geographic

    Returns a list of (srid, string_id, is_geographic) tuples,
    excluding deprecated entries and entries with non-numeric codes.
    """
    results = []
    pattern = re.compile(r'INSERT INTO "geodetic_crs" VALUES\((.+)\);', re.IGNORECASE)
    for line in sql_content.splitlines():
        match = pattern.search(line)
        if not match:
            continue
        fields = parse_sql_values(match.group(1))
        auth_name = fields[0]
        code = fields[1]
        crs_type = fields[4]
        try:
            srid = int(code)
        except (ValueError, TypeError):
            continue
        is_geographic = crs_type is not None and crs_type.startswith("geographic")
        string_id = f"{auth_name}:{code}"
        results.append((srid, string_id, is_geographic))
    return results


def parse_simple_crs(sql_content, table_name):
    """
    Parse INSERT statements for CRS tables that are always non-geographic.

    Works for projected_crs, compound_crs, and vertical_crs tables.
    Fields used: auth_name (0), code (1).

    Returns a list of (srid, string_id, is_geographic=False) tuples.
    """
    results = []
    pattern = re.compile(rf'INSERT INTO "{table_name}" VALUES\((.+)\);', re.IGNORECASE)
    for line in sql_content.splitlines():
        match = pattern.search(line)
        if not match:
            continue
        fields = parse_sql_values(match.group(1))
        auth_name = fields[0]
        code = fields[1]
        try:
            srid = int(code)
        except (ValueError, TypeError):
            continue
        string_id = f"{auth_name}:{code}"
        results.append((srid, string_id, False))
    return results


def parse_all_crs_from_sql(sql_content):
    """
    Parse all CRS types (geodetic, projected, compound, vertical) from a
    single SQL file. Used for multi-table files like esri.sql.
    """
    entries = []
    entries.extend(parse_geodetic_crs(sql_content))
    for table in ["projected_crs", "compound_crs", "vertical_crs", "engineering_crs"]:
        entries.extend(parse_simple_crs(sql_content, table))
    return entries


def write_csv(entries, proj_version, output_path):
    """Write SRS entries to a CSV file with a metadata header."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="") as f:
        f.write(
            f"# Generated by dev/generate_srs_registry.py from PROJ {proj_version}\n"
            f"# Source: https://github.com/OSGeo/PROJ/tree/{proj_version}/data/sql\n"
            f"# Do not edit manually. Re-run the script to regenerate.\n"
        )
        writer = csv.writer(f)
        writer.writerow(["srid", "string_id", "is_geographic"])
        for srid, string_id, is_geographic in sorted(entries):
            writer.writerow([srid, string_id, str(is_geographic).lower()])


def main():
    parser = argparse.ArgumentParser(
        description="Generate the SRS registry for Apache Spark from PROJ data."
    )
    parser.add_argument(
        "--proj-version",
        default=DEFAULT_PROJ_VERSION,
        help=f"PROJ release tag to download from (default: {DEFAULT_PROJ_VERSION})",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Path to the Spark repository root (auto-detected if not set)",
    )
    args = parser.parse_args()

    # Auto-detect repo root: this script lives in dev/ under the repo root.
    if args.repo_root:
        repo_root = args.repo_root
    else:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    print(f"Spark repo root: {repo_root}")
    print(f"PROJ version: {args.proj_version}")
    print()

    # Download PROJ SQL files.
    print("Downloading PROJ SQL files...")
    sql_files = {}
    for filename in PROJ_SQL_FILES:
        sql_files[filename] = download_sql(args.proj_version, filename)
    print()

    # Parse CRS entries from EPSG-specific files.
    print("Parsing CRS entries...")
    all_entries = []

    geodetic = parse_geodetic_crs(sql_files["geodetic_crs.sql"])
    print(f"  geodetic_crs.sql:  {len(geodetic)} entries")
    all_entries.extend(geodetic)

    projected = parse_simple_crs(sql_files["projected_crs.sql"], "projected_crs")
    print(f"  projected_crs.sql: {len(projected)} entries")
    all_entries.extend(projected)

    compound = parse_simple_crs(sql_files["compound_crs.sql"], "compound_crs")
    print(f"  compound_crs.sql:  {len(compound)} entries")
    all_entries.extend(compound)

    vertical = parse_simple_crs(sql_files["vertical_crs.sql"], "vertical_crs")
    print(f"  vertical_crs.sql:  {len(vertical)} entries")
    all_entries.extend(vertical)

    engineering = parse_simple_crs(sql_files["engineering_crs.sql"], "engineering_crs")
    print(f"  engineering_crs.sql: {len(engineering)} entries")
    all_entries.extend(engineering)

    # Parse ESRI entries from the combined esri.sql file.
    esri = parse_all_crs_from_sql(sql_files["esri.sql"])
    print(f"  esri.sql:          {len(esri)} entries")
    all_entries.extend(esri)

    print()

    # Deduplicate: keep the first occurrence of each SRID.
    seen = set()
    deduped = []
    duplicates = 0
    for entry in all_entries:
        if entry[0] not in seen:
            deduped.append(entry)
            seen.add(entry[0])
        else:
            duplicates += 1
    if duplicates:
        print(f"  Removed {duplicates} duplicate SRID(s)")
    all_entries = deduped

    # Add Spark-specific entry: SRID 0 (Cartesian, no defined SRS).
    all_entries.append((0, "SRID:0", False))

    # Apply OGC special case overrides: replace string IDs for standardized SRIDs.
    ogc_applied = 0
    for i, (srid, string_id, is_geographic) in enumerate(all_entries):
        if srid in OGC_SPECIAL_CASES:
            all_entries[i] = (srid, OGC_SPECIAL_CASES[srid], is_geographic)
            ogc_applied += 1
    print(f"  Applied {ogc_applied} OGC special case override(s)")

    # Count entries by authority.
    authority_counts = {}
    for _, string_id, _ in all_entries:
        auth = string_id.split(":")[0]
        authority_counts[auth] = authority_counts.get(auth, 0) + 1

    n_geographic = sum(1 for _, _, g in all_entries if g)
    n_nongeographic = len(all_entries) - n_geographic
    print()
    print(
        f"  Total: {len(all_entries)} entries "
        f"({n_geographic} geographic, {n_nongeographic} non-geographic)"
    )
    print(f"  Breakdown by authority:")
    for auth in sorted(authority_counts):
        print(f"    {auth}: {authority_counts[auth]}")
    print()

    # Write CSV to both Java and Python resource directories.
    java_path = os.path.join(repo_root, JAVA_RESOURCE_PATH)
    python_path = os.path.join(repo_root, PYTHON_RESOURCE_PATH)

    print("Writing CSV files...")
    write_csv(all_entries, args.proj_version, java_path)
    print(f"  {java_path}")
    write_csv(all_entries, args.proj_version, python_path)
    print(f"  {python_path}")
    print()

    print("Done. Verify with:")
    print(f"  wc -l {JAVA_RESOURCE_PATH}")
    print(f"  git diff {JAVA_RESOURCE_PATH}")


if __name__ == "__main__":
    main()
