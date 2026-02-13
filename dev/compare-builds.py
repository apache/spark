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
Compare SBT and Maven builds to verify they produce equivalent artifacts.

This script validates the migration from sbt-pom-reader to native SBT by
comparing JAR files, shading, and dependencies between the two build systems.

Comparison modes (mutually exclusive)
-------------------------------------
By default the script compares module JARs (class contents and sizes).
Use --shading, --assemblies-only, or --deps to switch modes.

  JARs (default)    Compare module JARs class-by-class.
  --shading         Verify shading relocation rules. For each expected
                    relocation (e.g., io/grpc/ -> org/sparkproject/io/grpc/)
                    checks that source packages are absent and target
                    packages are present in both builds.
  --assemblies-only Compare assembly JARs (size, class count, packages).
  --deps            Compare resolved dependency trees (runs Maven/SBT).
  --self-test       Run internal self-tests and exit.

Output
------
  (default)         Human-readable table to stdout.
  --json            Structured JSON to stdout (no table).
  -o FILE           Write JSON to FILE (table still shown on terminal).
  --json -o FILE    JSON to both stdout and FILE.

Filtering (JARs mode only)
--------------------------
These options only apply to the default JARs comparison mode.

  --matching-only   Only compare JARs present in both builds.
  --ignore-shaded   Ignore shaded class/service differences and bundled
                    deps in Maven fat-JAR modules (core, connect).
  --modules M1,M2   Restrict comparison to specific modules.
  -v, --verbose     Show class-level details for differing JARs.

Build
-----
  --build-maven     Run Maven build before comparing.
  --build-sbt       Run SBT build before comparing.
  --build-both      Run both builds before comparing.

Examples
--------
    # Quick validation (assumes both builds exist)
    python ./dev/compare-builds.py --matching-only --ignore-shaded -v

    # Shading verification
    python ./dev/compare-builds.py --shading

    # JSON report for CI
    python ./dev/compare-builds.py --matching-only --json -o report.json

How shading works
-----------------
Maven and SBT shade dependencies differently:

  Maven   The maven-shade-plugin embeds shaded classes directly in the
          module JAR (e.g., spark-core_2.13.jar contains org/sparkproject/).
  SBT     sbt-assembly produces a separate assembly JAR
          (e.g., spark-core-assembly-*.jar) with shaded classes.

Because of this, module JARs from Maven are larger than SBT's. Use
--ignore-shaded with the default mode to skip these expected differences,
or use --shading to verify relocation rules directly.

Relocation rules
~~~~~~~~~~~~~~~~
--shading verifies these expected relocations for each module:

  core:
    org/eclipse/jetty/     -> org/sparkproject/jetty/
    com/google/common/     -> org/sparkproject/guava/
    com/google/thirdparty/ -> org/sparkproject/guava/
    com/google/protobuf/   -> org/sparkproject/spark_core/protobuf/

  connect-client:
    com/google/common/     -> org/sparkproject/connect/guava/
    com/google/thirdparty/ -> org/sparkproject/connect/guava/
    com/google/protobuf/   -> org/sparkproject/com/google/protobuf/
    io/grpc/               -> org/sparkproject/io/grpc/
    io/netty/              -> org/sparkproject/io/netty/
    org/apache/arrow/      -> org/sparkproject/org/apache/arrow/

For each rule, both builds are checked: the source package should be
absent (relocated) and the target package should be present.
"""

import argparse
import json
import re
import subprocess
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


# Get Spark home directory
SPARK_HOME = Path(__file__).parent.parent.resolve()


@dataclass
class JarInfo:
    """Information about a JAR file."""

    path: Path
    size: int
    classes: Set[str] = field(default_factory=set)
    resources: Set[str] = field(default_factory=set)
    meta_inf: Set[str] = field(default_factory=set)
    services: Set[str] = field(default_factory=set)
    multi_release_classes: Set[str] = field(default_factory=set)

    @property
    def name(self) -> str:
        return self.path.name

    def class_count(self) -> int:
        return len(self.classes)

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "path": str(self.path.relative_to(SPARK_HOME)),
            "size": self.size,
            "class_count": self.class_count(),
            "resource_count": len(self.resources),
        }
        if self.services:
            d["services_count"] = len(self.services)
        if self.multi_release_classes:
            d["multi_release_class_count"] = len(self.multi_release_classes)
        return d


@dataclass
class ComparisonResult:
    """Result of comparing two JARs."""

    maven_jar: Optional[JarInfo]
    sbt_jar: Optional[JarInfo]
    only_in_maven: Set[str] = field(default_factory=set)
    only_in_sbt: Set[str] = field(default_factory=set)
    services_only_in_maven: Set[str] = field(default_factory=set)
    services_only_in_sbt: Set[str] = field(default_factory=set)
    size_diff_pct: float = 0.0

    @property
    def status(self) -> str:
        if self.maven_jar and not self.sbt_jar:
            return "only_maven"
        if self.sbt_jar and not self.maven_jar:
            return "only_sbt"
        if self.has_content_diff:
            return "differs"
        return "match"

    @property
    def is_match(self) -> bool:
        return (
            self.maven_jar is not None
            and self.sbt_jar is not None
            and len(self.only_in_maven) == 0
            and len(self.only_in_sbt) == 0
            and len(self.services_only_in_maven) == 0
            and len(self.services_only_in_sbt) == 0
        )

    @property
    def has_content_diff(self) -> bool:
        return (
            len(self.only_in_maven) > 0
            or len(self.only_in_sbt) > 0
            or len(self.services_only_in_maven) > 0
            or len(self.services_only_in_sbt) > 0
        )

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"status": self.status}
        if self.maven_jar:
            d["maven"] = self.maven_jar.to_dict()
        if self.sbt_jar:
            d["sbt"] = self.sbt_jar.to_dict()
        if self.maven_jar and self.sbt_jar:
            d["size_diff"] = _format_size_diff(self.maven_jar.size, self.sbt_jar.size)
        if self.only_in_maven:
            d["only_in_maven"] = _class_package_counts(self.only_in_maven)
            d["only_in_maven_count"] = len(self.only_in_maven)
        if self.only_in_sbt:
            d["only_in_sbt"] = _class_package_counts(self.only_in_sbt)
            d["only_in_sbt_count"] = len(self.only_in_sbt)
        if self.services_only_in_maven:
            d["services_only_in_maven"] = sorted(self.services_only_in_maven)
        if self.services_only_in_sbt:
            d["services_only_in_sbt"] = sorted(self.services_only_in_sbt)
        return d


def run_command(cmd: List[str], cwd: Path = SPARK_HOME) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, stderr."""
    print(f"[cmd] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def build_maven(profiles: List[str] = None) -> bool:
    """Build with Maven."""
    print("\n" + "=" * 72)
    print("Building with Maven...")
    print("=" * 72)

    cmd = [str(SPARK_HOME / "build" / "mvn"), "-DskipTests", "package"]
    if profiles:
        cmd.extend([f"-P{p}" for p in profiles])

    ret, stdout, stderr = run_command(cmd)
    if ret != 0:
        print(f"[error] Maven build failed:\n{stderr}")
        return False
    print("[ok] Maven build completed successfully")
    return True


def build_sbt() -> bool:
    """Build with SBT."""
    print("\n" + "=" * 72)
    print("Building with SBT...")
    print("=" * 72)

    cmd = [str(SPARK_HOME / "build" / "sbt"), "package"]
    ret, stdout, stderr = run_command(cmd)
    if ret != 0:
        print(f"[error] SBT build failed:\n{stderr}")
        return False
    print("[ok] SBT build completed successfully")
    return True


def get_jar_contents(jar_path: Path) -> JarInfo:
    """Extract information about a JAR file's contents."""
    info = JarInfo(path=jar_path, size=jar_path.stat().st_size)

    try:
        with zipfile.ZipFile(jar_path, "r") as zf:
            for name in zf.namelist():
                if name.endswith("/"):
                    continue  # Skip directories
                if name.startswith("META-INF/services/"):
                    info.services.add(name)
                elif name.endswith(".class"):
                    if name.startswith("META-INF/versions/"):
                        info.multi_release_classes.add(name)
                    else:
                        info.classes.add(name)
                elif name.startswith("META-INF/"):
                    info.meta_inf.add(name)
                else:
                    info.resources.add(name)
    except zipfile.BadZipFile:
        print(f"[warn] Could not read JAR: {jar_path}")

    return info


def should_skip_jar(name: str) -> bool:
    """Check if a JAR should be skipped from comparison."""
    # Skip Maven's pre-shaded "original-" JARs
    if name.startswith("original-"):
        return True
    # Skip assembly JARs (use --assemblies-only for those)
    if "-assembly" in name or name.endswith("-assembly.jar"):
        return True
    return False


def normalize_jar_name(name: str) -> str:
    """
    Normalize JAR name for comparison between Maven and SBT.

    Maven: spark-core_2.13-4.0.0-SNAPSHOT.jar
    SBT:   spark-core_2.13-4.0.0-SNAPSHOT.jar (should be same)

    Extract the artifact name (before version) for matching.
    Examples:
        spark-core_2.13-4.0.0-SNAPSHOT.jar -> spark-core_2.13
        spark-sql-kafka-0-10_2.13-4.0.0-SNAPSHOT.jar -> spark-sql-kafka-0-10_2.13
    """
    # Remove .jar extension
    base = name[:-4] if name.endswith(".jar") else name

    # Strategy 1: Use the Scala binary version suffix (_2.13, _2.12, _3, etc.)
    # as an anchor. All Spark artifacts include this suffix, and it always appears
    # between the artifact name and the build version. The suffix is _X.Y or _X,
    # and must be followed by '-' (version) or end of string.
    scala_match = re.search(r"_\d+(\.\d+)?(?=-|$)", base)
    if scala_match:
        return base[: scala_match.end()]

    # Strategy 2: Fall back to version pattern for non-Scala JARs (X.Y or X.Y.Z)
    version_match = re.search(r"-\d+\.\d+", base)
    if version_match:
        return base[: version_match.start()]

    return base


def _find_module_dirs() -> List[Path]:
    """Parse module directories from root pom.xml.

    Reads <module> elements from the root POM to get the exact list of build
    modules.  This avoids an expensive rglob("target") across the entire
    Spark tree (which would walk .git/, python/, docs/, R/, etc.).
    """
    pom_path = SPARK_HOME / "pom.xml"
    if not pom_path.exists():
        return []

    pom_text = pom_path.read_text()
    dirs: List[Path] = []
    for match in re.finditer(r"<module>(.*?)</module>", pom_text):
        module_dir = SPARK_HOME / match.group(1)
        if module_dir.is_dir():
            dirs.append(module_dir)
    return dirs


def _should_skip_jar_file(jar_path: Path) -> bool:
    """Return True if a JAR file should be excluded from comparison."""
    name = jar_path.name
    if "-tests.jar" in name or "-sources.jar" in name or "-javadoc.jar" in name:
        return True
    return should_skip_jar(name)


def find_maven_jars(modules: Optional[List[str]] = None) -> Dict[str, JarInfo]:
    """Find all JAR files from Maven build."""
    jars: Dict[str, JarInfo] = {}

    # Maven puts JARs in {module}/target/.  We parse module paths from pom.xml
    # to avoid an expensive walk of the entire source tree.
    module_dirs = _find_module_dirs()

    for module_dir in module_dirs:
        target_dir = module_dir / "target"
        if not target_dir.is_dir():
            continue

        for jar_path in target_dir.glob("*.jar"):
            if _should_skip_jar_file(jar_path):
                continue

            # Filter by module if specified
            if modules:
                if not any(m in str(jar_path) for m in modules):
                    continue

            norm_name = normalize_jar_name(jar_path.name)
            if norm_name in jars:
                prev = jars[norm_name].path
                print(
                    f"[warn] duplicate Maven JAR key '{norm_name}':"
                    f" {prev.relative_to(SPARK_HOME)} vs"
                    f" {jar_path.relative_to(SPARK_HOME)}, keeping latter"
                )
            jars[norm_name] = get_jar_contents(jar_path)

    return jars


def find_sbt_jars(modules: Optional[List[str]] = None) -> Dict[str, JarInfo]:
    """Find all JAR files from SBT build."""
    jars: Dict[str, JarInfo] = {}

    # SBT puts JARs in {module}/target/scala-X.XX/.
    module_dirs = _find_module_dirs()

    for module_dir in module_dirs:
        target_dir = module_dir / "target"
        if not target_dir.is_dir():
            continue

        for scala_dir in target_dir.glob("scala-*"):
            if not scala_dir.is_dir():
                continue

            for jar_path in scala_dir.glob("*.jar"):
                if _should_skip_jar_file(jar_path):
                    continue

                # Filter by module if specified
                if modules:
                    if not any(m in str(jar_path) for m in modules):
                        continue

                norm_name = normalize_jar_name(jar_path.name)
                if norm_name in jars:
                    prev = jars[norm_name].path
                    print(
                        f"[warn] duplicate SBT JAR key '{norm_name}':"
                        f" {prev.relative_to(SPARK_HOME)} vs"
                        f" {jar_path.relative_to(SPARK_HOME)}, keeping latter"
                    )
                jars[norm_name] = get_jar_contents(jar_path)

    return jars


# Packages that are shaded and expected to differ between Maven and SBT
SHADED_PACKAGES = {
    "org/sparkproject/",  # Shaded Jetty, Guava, etc. in Maven core
    "org/apache/spark/unused/",  # Placeholder classes
}


def is_shaded_class(class_name: str) -> bool:
    """Check if a class is from a shaded package."""
    return any(class_name.startswith(pkg) for pkg in SHADED_PACKAGES)


def _is_shaded_service(service_path: str) -> bool:
    """Check if a META-INF/services/ file references a shaded package."""
    # e.g. META-INF/services/org.sparkproject.jetty.compression.Compression
    service_name = service_path.rsplit("/", 1)[-1]
    return any(
        service_name.startswith(pkg.rstrip("/").replace("/", ".")) for pkg in SHADED_PACKAGES
    )


# Modules where Maven's shade plugin bundles dependency classes into the
# module JAR, making it a "fat JAR".  SBT keeps these as thin module JARs
# with separate assembly JARs.  When --ignore-shaded is active, extra
# Maven-only classes in these modules are expected (bundled deps) provided
# that SBT's own classes are all present in Maven.
FAT_JAR_MODULES = {"spark-core", "spark-connect-client-jvm", "spark-connect"}


def _is_fat_jar_module(norm_name: str) -> bool:
    """Check if a normalized JAR name is a known fat-JAR module."""
    base = norm_name.split("_")[0] if "_" in norm_name else norm_name
    return base in FAT_JAR_MODULES


def _format_size_diff(maven_size: int, sbt_size: int) -> str:
    """Format size difference in a human-readable way."""
    if maven_size == sbt_size:
        return "identical"
    bigger, smaller = max(maven_size, sbt_size), min(maven_size, sbt_size)
    if smaller == 0:
        return "N/A (one side is empty)"
    ratio = bigger / smaller
    label = "Maven" if maven_size > sbt_size else "SBT"
    if ratio >= 2:
        return f"{label} is {ratio:.0f}x larger"
    else:
        pct = (bigger - smaller) / smaller * 100
        return f"{label} is {pct:.1f}% larger"


def compare_jars(
    maven_jars: Dict[str, JarInfo],
    sbt_jars: Dict[str, JarInfo],
    matching_only: bool = False,
    ignore_shaded: bool = False,
) -> Dict[str, ComparisonResult]:
    """Compare JAR files from both builds."""
    results = {}

    if matching_only:
        all_names = set(maven_jars.keys()) & set(sbt_jars.keys())
    else:
        all_names = set(maven_jars.keys()) | set(sbt_jars.keys())

    for name in sorted(all_names):
        maven_jar = maven_jars.get(name)
        sbt_jar = sbt_jars.get(name)

        result = ComparisonResult(maven_jar=maven_jar, sbt_jar=sbt_jar)

        if maven_jar and sbt_jar:
            # Compare classes
            maven_classes = maven_jar.classes
            sbt_classes = sbt_jar.classes

            # Filter out shaded classes if requested
            if ignore_shaded:
                maven_classes = {c for c in maven_classes if not is_shaded_class(c)}
                sbt_classes = {c for c in sbt_classes if not is_shaded_class(c)}

            result.only_in_maven = maven_classes - sbt_classes
            result.only_in_sbt = sbt_classes - maven_classes

            # Compare META-INF/services/ (service loader configs)
            result.services_only_in_maven = maven_jar.services - sbt_jar.services
            result.services_only_in_sbt = sbt_jar.services - maven_jar.services

            if ignore_shaded:
                # Filter out service files that reference shaded packages
                result.services_only_in_maven = {
                    s for s in result.services_only_in_maven if not _is_shaded_service(s)
                }
                result.services_only_in_sbt = {
                    s for s in result.services_only_in_sbt if not _is_shaded_service(s)
                }
                # For known fat-JAR modules, Maven's shade plugin bundles
                # dependency classes and their services into the module JAR.
                # If all of SBT's classes are present in Maven, the extra
                # Maven classes/services are just bundled deps.
                if _is_fat_jar_module(name) and not result.only_in_sbt:
                    result.only_in_maven = set()
                    if not result.services_only_in_sbt:
                        result.services_only_in_maven = set()

            # Calculate size difference as percentage of the smaller JAR
            min_size = min(maven_jar.size, sbt_jar.size)
            if min_size > 0:
                result.size_diff_pct = abs(maven_jar.size - sbt_jar.size) / min_size * 100

        results[name] = result

    return results


def build_report_dict(results: Dict[str, ComparisonResult]) -> Dict[str, Any]:
    """Build a structured report dictionary from comparison results."""
    total = len(results)
    matches = sum(1 for r in results.values() if r.is_match)
    only_maven = sum(1 for r in results.values() if r.maven_jar and not r.sbt_jar)
    only_sbt = sum(1 for r in results.values() if r.sbt_jar and not r.maven_jar)
    content_diffs = sum(1 for r in results.values() if r.has_content_diff)

    return {
        "mode": "jars",
        "summary": {
            "total": total,
            "matching": matches,
            "only_in_maven": only_maven,
            "only_in_sbt": only_sbt,
            "content_differs": content_diffs,
        },
        "jars": {name: r.to_dict() for name, r in sorted(results.items())},
    }


def _summarize_classes(classes: Set[str]) -> List[str]:
    """Summarize a set of class paths by grouping into packages.

    Returns lines like:
        org/apache/spark/connect/proto/ (1852 classes)
        org/apache/spark/api/java/function/ (2 classes)
    """
    pkg_counts: Counter = Counter()
    for cls in classes:
        # package = everything up to and including the last /
        idx = cls.rfind("/")
        pkg = cls[: idx + 1] if idx >= 0 else ""
        pkg_counts[pkg] += 1

    # Collapse child packages into parent when the parent accounts for most classes.
    # e.g. proto/Foo$Bar.class and proto/Baz.class both map to proto/
    # Walk from deepest to shallowest and merge small children into parents.
    collapsed: Dict[str, int] = {}
    for pkg in sorted(pkg_counts, key=lambda p: -p.count("/")):
        merged = False
        # Try to merge into an existing parent
        for existing in list(collapsed):
            if pkg.startswith(existing) and pkg != existing:
                collapsed[existing] += pkg_counts[pkg]
                merged = True
                break
        if not merged:
            collapsed[pkg] = pkg_counts[pkg]

    # Sort by count descending
    lines = []
    for pkg, count in sorted(collapsed.items(), key=lambda x: -x[1]):
        label = pkg if pkg else "(default package)"
        lines.append(f"{label} ({count} classes)")
    return lines


def _class_package_counts(classes: Set[str]) -> Dict[str, int]:
    """Return {package: count} dict for JSON output, collapsing child packages."""
    pkg_counts: Counter = Counter()
    for cls in classes:
        idx = cls.rfind("/")
        pkg = cls[: idx + 1] if idx >= 0 else "(default)"
        pkg_counts[pkg] += 1

    collapsed: Dict[str, int] = {}
    for pkg in sorted(pkg_counts, key=lambda p: -p.count("/")):
        merged = False
        for existing in list(collapsed):
            if pkg.startswith(existing) and pkg != existing:
                collapsed[existing] += pkg_counts[pkg]
                merged = True
                break
        if not merged:
            collapsed[pkg] = pkg_counts[pkg]
    return dict(sorted(collapsed.items(), key=lambda x: -x[1]))


def _status_label(r: ComparisonResult) -> str:
    """Return a concise status label for terminal display."""
    if r.status == "match":
        return "match"
    if r.status == "only_maven":
        return "only in Maven"
    if r.status == "only_sbt":
        return "only in SBT"
    parts = []
    if r.only_in_maven:
        parts.append(f"+{len(r.only_in_maven)} Maven")
    if r.only_in_sbt:
        parts.append(f"+{len(r.only_in_sbt)} SBT")
    if r.services_only_in_maven or r.services_only_in_sbt:
        svc_n = len(r.services_only_in_maven) + len(r.services_only_in_sbt)
        parts.append(f"{svc_n} services differ")
    return ", ".join(parts) if parts else "differs"


def print_report(
    results: Dict[str, ComparisonResult],
    verbose: bool = False,
):
    """Print comparison report as a formatted table."""
    total = len(results)
    matches = sum(1 for r in results.values() if r.is_match)
    content_diffs = sum(1 for r in results.values() if r.has_content_diff)
    only_maven_n = sum(1 for r in results.values() if r.maven_jar and not r.sbt_jar)
    only_sbt_n = sum(1 for r in results.values() if r.sbt_jar and not r.maven_jar)

    # Build table rows: (module, maven_size, sbt_size, status)
    rows = []
    for name, r in sorted(results.items()):
        mvn_size = f"{r.maven_jar.size:,}" if r.maven_jar else "-"
        sbt_size = f"{r.sbt_jar.size:,}" if r.sbt_jar else "-"
        status = _status_label(r)
        rows.append((name, mvn_size, sbt_size, status))

    # Calculate column widths
    col_module = max(len("Module"), max((len(r[0]) for r in rows), default=0))
    col_maven = max(len("Maven (bytes)"), max((len(r[1]) for r in rows), default=0))
    col_sbt = max(len("SBT (bytes)"), max((len(r[2]) for r in rows), default=0))
    col_status = max(len("Status"), max((len(r[3]) for r in rows), default=0))
    line_width = col_module + col_maven + col_sbt + col_status + 9  # separators

    print()
    print(
        f"{'Module':<{col_module}}  {'Maven (bytes)':>{col_maven}}  {'SBT (bytes)':>{col_sbt}}  {'Status':<{col_status}}"
    )
    print("\u2500" * line_width)

    for name, mvn_size, sbt_size, status in rows:
        marker = "\u2713" if status == "match" else "\u2717"
        print(
            f"{name:<{col_module}}  {mvn_size:>{col_maven}}  {sbt_size:>{col_sbt}}  {marker} {status}"
        )

    print("\u2500" * line_width)

    # Summary line
    parts = [f"{matches} match"]
    if content_diffs:
        parts.append(f"{content_diffs} differ")
    if only_maven_n:
        parts.append(f"{only_maven_n} only in Maven")
    if only_sbt_n:
        parts.append(f"{only_sbt_n} only in SBT")
    print(f"Summary: {', '.join(parts)} ({total} total)")

    # Verbose: show details for non-matching JARs
    if verbose and (content_diffs or only_maven_n or only_sbt_n):
        print(f"\n{'=' * line_width}")
        print("Details")
        print("=" * line_width)

        for name, r in sorted(results.items()):
            if r.is_match:
                continue

            print(f"\n  {name} [{r.status}]")

            if r.maven_jar and r.sbt_jar:
                mvn_mr = len(r.maven_jar.multi_release_classes)
                sbt_mr = len(r.sbt_jar.multi_release_classes)
                mvn_svc = len(r.maven_jar.services)
                sbt_svc = len(r.sbt_jar.services)
                print(
                    f"    Maven: {r.maven_jar.class_count()} classes,"
                    f" {mvn_svc} services, {r.maven_jar.size:,} bytes"
                    + (f" ({mvn_mr} multi-release classes skipped)" if mvn_mr else "")
                )
                print(
                    f"    SBT:   {r.sbt_jar.class_count()} classes,"
                    f" {sbt_svc} services, {r.sbt_jar.size:,} bytes"
                    + (f" ({sbt_mr} multi-release classes skipped)" if sbt_mr else "")
                )
                print(f"    Size:  {_format_size_diff(r.maven_jar.size, r.sbt_jar.size)}")

            if r.only_in_maven:
                print(f"    Classes only in Maven ({len(r.only_in_maven)}):")
                for line in _summarize_classes(r.only_in_maven):
                    print(f"      {line}")

            if r.only_in_sbt:
                print(f"    Classes only in SBT ({len(r.only_in_sbt)}):")
                for line in _summarize_classes(r.only_in_sbt):
                    print(f"      {line}")

            if r.services_only_in_maven:
                print(f"    Services only in Maven ({len(r.services_only_in_maven)}):")
                for svc in sorted(r.services_only_in_maven):
                    print(f"      {svc}")

            if r.services_only_in_sbt:
                print(f"    Services only in SBT ({len(r.services_only_in_sbt)}):")
                for svc in sorted(r.services_only_in_sbt):
                    print(f"      {svc}")


def compare_assembly_data() -> Dict[str, Any]:
    """Collect assembly comparison data and return structured dict."""
    maven_assemblies = find_shaded_jars("maven")
    sbt_assemblies = find_shaded_jars("sbt")

    all_names = set(maven_assemblies.keys()) | set(sbt_assemblies.keys())
    assemblies_data: Dict[str, Any] = {}
    total_issues = 0

    for name in sorted(all_names):
        maven_jar = maven_assemblies.get(name)
        sbt_jar = sbt_assemblies.get(name)
        entry: Dict[str, Any] = {}

        maven_info = get_jar_contents(maven_jar) if maven_jar else None
        sbt_info = get_jar_contents(sbt_jar) if sbt_jar else None

        if maven_info:
            entry["maven"] = {
                "jar_type": "module",
                "path": str(maven_jar.relative_to(SPARK_HOME)),
                "size": maven_info.size,
                "class_count": maven_info.class_count(),
                "resource_count": len(maven_info.resources),
            }
        if sbt_info:
            entry["sbt"] = {
                "jar_type": "assembly",
                "path": str(sbt_jar.relative_to(SPARK_HOME)),
                "size": sbt_info.size,
                "class_count": sbt_info.class_count(),
                "resource_count": len(sbt_info.resources),
            }
        if maven_info and sbt_info:
            entry["size_diff"] = _format_size_diff(maven_info.size, sbt_info.size)

        # Compare class packages
        maven_packages: Dict[str, int] = defaultdict(int)
        sbt_packages: Dict[str, int] = defaultdict(int)

        for cls in maven_info.classes if maven_info else set():
            pkg = "/".join(cls.split("/")[:-1])
            maven_packages[pkg] += 1
        for cls in sbt_info.classes if sbt_info else set():
            pkg = "/".join(cls.split("/")[:-1])
            sbt_packages[pkg] += 1

        only_maven_pkgs = set(maven_packages.keys()) - set(sbt_packages.keys())
        only_sbt_pkgs = set(sbt_packages.keys()) - set(maven_packages.keys())

        if only_maven_pkgs:
            entry["only_in_maven"] = {pkg: maven_packages[pkg] for pkg in sorted(only_maven_pkgs)}
            total_issues += len(only_maven_pkgs)
        if only_sbt_pkgs:
            entry["only_in_sbt"] = {pkg: sbt_packages[pkg] for pkg in sorted(only_sbt_pkgs)}
            total_issues += len(only_sbt_pkgs)

        # Shading verification
        shaded_prefixes = ["org/sparkproject/", "org/apache/spark/unused/"]
        shading = {}
        for prefix in shaded_prefixes:
            m_count = sum(
                1 for c in (maven_info.classes if maven_info else set()) if c.startswith(prefix)
            )
            s_count = sum(
                1 for c in (sbt_info.classes if sbt_info else set()) if c.startswith(prefix)
            )
            shading[prefix] = {"maven": m_count, "sbt": s_count}
            if maven_info and sbt_info and m_count != s_count:
                total_issues += 1
        entry["shading"] = shading

        assemblies_data[name] = entry

    return {
        "mode": "assemblies",
        "assemblies": assemblies_data,
        "issues": total_issues,
    }


def print_assembly_report(data: Dict[str, Any]) -> None:
    """Print assembly comparison in human-readable format."""
    assemblies = data["assemblies"]
    issues = data["issues"]

    print(f"\nAssembly Comparison ({len(assemblies)} assemblies)")
    print("\u2500" * 72)

    for name, entry in assemblies.items():
        print(f"\n  {name}")

        for build in ("maven", "sbt"):
            if build in entry:
                info = entry[build]
                jar_type = info.get("jar_type", "")
                suffix = f" ({jar_type} JAR)" if jar_type else ""
                label = "Maven" if build == "maven" else "SBT  "
                print(
                    f"    {label}{suffix}: {info['path']}"
                    f" ({info['size']:,} bytes, {info['class_count']} classes,"
                    f" {info['resource_count']} resources)"
                )
            else:
                label = "Maven" if build == "maven" else "SBT  "
                print(f"    {label}: (not built)")

        if "size_diff" in entry:
            print(f"    Size:  {entry['size_diff']}")

        if "only_in_maven" in entry:
            pkgs = entry["only_in_maven"]
            print(f"    Packages only in Maven ({len(pkgs)}):")
            for pkg, count in list(pkgs.items())[:20]:
                print(f"      {pkg} ({count} classes)")
            if len(pkgs) > 20:
                print(f"      ... and {len(pkgs) - 20} more")

        if "only_in_sbt" in entry:
            pkgs = entry["only_in_sbt"]
            print(f"    Packages only in SBT ({len(pkgs)}):")
            for pkg, count in list(pkgs.items())[:20]:
                print(f"      {pkg} ({count} classes)")
            if len(pkgs) > 20:
                print(f"      ... and {len(pkgs) - 20} more")

        shading = entry.get("shading", {})
        if shading:
            print("    Shading:")
            for prefix, counts in shading.items():
                m, s = counts["maven"], counts["sbt"]
                if m == s:
                    print(f"      \u2713 {prefix}: {m} classes")
                else:
                    print(f"      \u2717 {prefix}: Maven={m}, SBT={s}")

    print("\u2500" * 72)
    if issues == 0:
        print("Result: Assembly comparison passed!")
    else:
        print(f"Result: {issues} assembly issues found")


# ============================================================================
# SHADING COMPARISON
# ============================================================================

# Expected shading relocations for different assembly types
SHADING_RULES = {
    "core": {
        # Original package -> Shaded package
        "org/eclipse/jetty/": "org/sparkproject/jetty/",
        "com/google/common/": "org/sparkproject/guava/",
        "com/google/thirdparty/": "org/sparkproject/guava/",
        "com/google/protobuf/": "org/sparkproject/spark_core/protobuf/",
    },
    "connect-client": {
        "com/google/common/": "org/sparkproject/connect/guava/",
        "com/google/thirdparty/": "org/sparkproject/connect/guava/",
        "com/google/protobuf/": "org/sparkproject/com/google/protobuf/",
        "io/grpc/": "org/sparkproject/io/grpc/",
        "io/netty/": "org/sparkproject/io/netty/",
        "org/apache/arrow/": "org/sparkproject/org/apache/arrow/",
    },
}


def find_shaded_jars(build_type: str) -> Dict[str, Path]:
    """Find the JARs that contain shaded classes for each build system.

    Maven embeds shaded classes in the module JAR itself (no separate assembly),
    so this returns module JARs like ``spark-core_2.13-*.jar``.

    SBT produces separate assembly JARs (``*-assembly-*.jar``) under
    ``target/scala-X.XX/``, so this returns those.

    Used by both ``--assemblies-only`` and ``--shading`` modes.
    """
    assemblies = {}

    # (name, target_path, maven_jar_glob)
    assembly_locations = [
        ("core", "core/target", "spark-core_*.jar"),
        ("connect-client-jvm", "sql/connect/client/jvm/target", "spark-connect-client-jvm_*.jar"),
    ]

    for name, base_path, maven_glob in assembly_locations:
        target_dir = SPARK_HOME / base_path

        if build_type == "maven":
            # Maven embeds shaded deps in the module JAR directly
            for jar in sorted(target_dir.glob(maven_glob)):
                if (
                    "-tests" not in jar.name
                    and "-sources" not in jar.name
                    and "-javadoc" not in jar.name
                    and not jar.name.startswith("original-")
                ):
                    assemblies[name] = jar
                    break
        else:
            # SBT puts assemblies in target/scala-X.XX/
            for scala_dir in target_dir.glob("scala-*"):
                for jar in scala_dir.glob("*-assembly*.jar"):
                    if "-tests" not in jar.name:
                        assemblies[name] = jar
                        break

    return assemblies


def _count_classes_under(jar_path: Path, prefix: str) -> int:
    """Count .class files under a package prefix inside a JAR."""
    count = 0
    with zipfile.ZipFile(jar_path, "r") as zf:
        for name in zf.namelist():
            if name.endswith(".class") and name.startswith(prefix):
                count += 1
    return count


def compare_shading_data() -> Dict[str, Any]:
    """Collect rule-driven shading comparison data.

    For each module in SHADING_RULES, checks every relocation rule against
    both the Maven and SBT JARs:
      - source package should be absent (relocated)
      - target package should be present
      - class counts should match between builds
    """
    maven_jars = find_shaded_jars("maven")
    sbt_jars = find_shaded_jars("sbt")

    # Map SHADING_RULES keys to find_shaded_jars keys
    # SHADING_RULES uses short names ("core", "connect-client")
    # find_shaded_jars uses full names ("core", "connect-client-jvm")
    rule_to_jar_key = {
        "core": "core",
        "connect-client": "connect-client-jvm",
    }

    modules_data: Dict[str, Any] = {}
    total_rules = 0
    rules_pass = 0
    rules_fail = 0
    rules_warn = 0

    for module, rules in SHADING_RULES.items():
        jar_key = rule_to_jar_key.get(module, module)
        maven_jar = maven_jars.get(jar_key)
        sbt_jar = sbt_jars.get(jar_key)

        module_entry: Dict[str, Any] = {}
        if maven_jar:
            module_entry["maven"] = {
                "jar_type": "module",
                "path": str(maven_jar.relative_to(SPARK_HOME)),
            }
        if sbt_jar:
            module_entry["sbt"] = {
                "jar_type": "assembly",
                "path": str(sbt_jar.relative_to(SPARK_HOME)),
            }

        rules_data: List[Dict[str, Any]] = []
        for source, target in rules.items():
            total_rules += 1
            rule_entry: Dict[str, Any] = {"source": source, "target": target}

            # Check each build
            for build, jar_path in [("maven", maven_jar), ("sbt", sbt_jar)]:
                if jar_path is None:
                    rule_entry[build] = {"status": "not_built"}
                    continue

                source_count = _count_classes_under(jar_path, source)
                target_count = _count_classes_under(jar_path, target)

                if source_count > 0:
                    rule_entry[build] = {
                        "status": "unshaded",
                        "source_classes": source_count,
                        "target_classes": target_count,
                    }
                elif target_count > 0:
                    rule_entry[build] = {
                        "status": "relocated",
                        "target_classes": target_count,
                    }
                else:
                    rule_entry[build] = {"status": "absent"}

            # Determine overall rule status
            m_status = rule_entry.get("maven", {}).get("status")
            s_status = rule_entry.get("sbt", {}).get("status")

            if m_status == "unshaded" or s_status == "unshaded":
                rule_entry["result"] = "FAIL"
                rules_fail += 1
            elif m_status == "relocated" and s_status == "relocated":
                m_count = rule_entry["maven"]["target_classes"]
                s_count = rule_entry["sbt"]["target_classes"]
                if m_count == s_count:
                    rule_entry["result"] = "PASS"
                    rules_pass += 1
                else:
                    rule_entry["result"] = "WARN"
                    rules_warn += 1
            elif m_status == "not_built" or s_status == "not_built":
                # Can only verify the side that exists
                existing = m_status if m_status != "not_built" else s_status
                if existing == "relocated":
                    rule_entry["result"] = "PASS"
                    rules_pass += 1
                elif existing == "unshaded":
                    rule_entry["result"] = "FAIL"
                    rules_fail += 1
                else:
                    rule_entry["result"] = "WARN"
                    rules_warn += 1
            else:
                rule_entry["result"] = "WARN"
                rules_warn += 1

            rules_data.append(rule_entry)

        module_entry["rules"] = rules_data
        modules_data[module] = module_entry

    return {
        "mode": "shading",
        "summary": {
            "modules": len(modules_data),
            "total_rules": total_rules,
            "pass": rules_pass,
            "fail": rules_fail,
            "warn": rules_warn,
        },
        "modules": modules_data,
    }


def print_shading_report(data: Dict[str, Any]) -> None:
    """Print rule-driven shading report."""
    modules = data["modules"]
    summary = data["summary"]

    print("\nShading Verification")
    print("Maven embeds shaded classes in module JARs; SBT uses separate assembly JARs.")
    print("Sizes are not comparable. This report verifies relocation rules only.")
    print("\u2500" * 72)

    for module, entry in modules.items():
        rules = entry["rules"]
        print(f"\n  {module} ({len(rules)} rules)")

        if "maven" in entry:
            print(f"    Maven: {entry['maven']['path']} ({entry['maven']['jar_type']} JAR)")
        else:
            print("    Maven: (not built)")
        if "sbt" in entry:
            print(f"    SBT:   {entry['sbt']['path']} ({entry['sbt']['jar_type']} JAR)")
        else:
            print("    SBT:   (not built)")

        for rule in rules:
            source = rule["source"]
            target = rule["target"]
            result = rule["result"]
            marker = "\u2713" if result == "PASS" else ("\u2717" if result == "FAIL" else "~")

            print(f"\n    {marker} {source} \u2192 {target}")

            for build in ("maven", "sbt"):
                info = rule.get(build, {})
                status = info.get("status", "not_built")
                label = "Maven" if build == "maven" else "SBT  "

                if status == "not_built":
                    print(f"      {label}: (not built)")
                elif status == "relocated":
                    count = info["target_classes"]
                    print(f"      {label}: relocated ({count} classes)")
                elif status == "unshaded":
                    src_count = info["source_classes"]
                    tgt_count = info.get("target_classes", 0)
                    print(
                        f"      {label}: UNSHADED ({src_count} source classes remain"
                        + (f", {tgt_count} relocated)" if tgt_count else ")")
                    )
                elif status == "absent":
                    print(f"      {label}: absent (no source or target classes)")

            # Show count mismatch if both relocated
            m_info = rule.get("maven", {})
            s_info = rule.get("sbt", {})
            if (
                m_info.get("status") == "relocated"
                and s_info.get("status") == "relocated"
                and m_info["target_classes"] != s_info["target_classes"]
            ):
                diff = s_info["target_classes"] - m_info["target_classes"]
                sign = "+" if diff > 0 else ""
                print(f"      (SBT has {sign}{diff} classes vs Maven)")

    print("\u2500" * 72)
    total = summary["total_rules"]
    p, f, w = summary["pass"], summary["fail"], summary["warn"]
    parts = [f"{total} rules checked"]
    if p:
        parts.append(f"{p} pass")
    if f:
        parts.append(f"{f} fail")
    if w:
        parts.append(f"{w} warn")
    print(f"Summary: {', '.join(parts)}")
    if f > 0:
        print("FAIL: unshaded classes found that should have been relocated")
    elif w > 0:
        print("WARN: all relocations applied, but class counts differ between builds")
    else:
        print("PASS: all relocation rules verified")


def get_maven_dependencies() -> Dict[str, Set[str]]:
    """Get dependencies for each module from Maven."""
    deps = {}
    cmd = [
        str(SPARK_HOME / "build" / "mvn"),
        "dependency:list",
        "-DoutputAbsoluteArtifactFilename=false",
        "-DincludeScope=compile",
    ]
    ret, stdout, stderr = run_command(cmd)
    if ret != 0:
        print(f"[warn] Failed to get Maven dependencies: {stderr}")
        return deps

    current_module = None
    for line in stdout.split("\n"):
        # Look for module headers
        if line.startswith("[INFO] --- maven-dependency-plugin"):
            # Extract module from path
            match = re.search(r"@ (\S+) ---", line)
            if match:
                current_module = match.group(1)
                deps[current_module] = set()
        elif current_module and ":" in line and line.strip().startswith("[INFO]"):
            # Parse dependency line
            parts = line.strip().split()
            if len(parts) >= 2:
                dep = parts[1]  # groupId:artifactId:type:version:scope
                if ":" in dep:
                    deps[current_module].add(dep)

    return deps


def get_sbt_dependencies() -> Dict[str, Set[str]]:
    """Get dependencies for each module from SBT."""
    deps = {}
    # Use SBT's dependencyList task
    cmd = [str(SPARK_HOME / "build" / "sbt"), "dependencyList"]
    ret, stdout, stderr = run_command(cmd)
    if ret != 0:
        print(f"[warn] Failed to get SBT dependencies: {stderr}")
        return deps

    current_module = None
    for line in stdout.split("\n"):
        # Look for project headers in SBT output
        if line.startswith("[info] ") and "/" in line and "dependencyList" not in line:
            module_match = re.search(r"\[info\] (\S+) /", line)
            if module_match:
                current_module = module_match.group(1)
                deps[current_module] = set()
        elif current_module and line.strip() and not line.startswith("["):
            # SBT dependency format: groupId:artifactId:version
            dep = line.strip()
            if ":" in dep and not dep.startswith("#"):
                deps[current_module].add(dep)

    return deps


def compare_dependencies_data() -> Dict[str, Any]:
    """Collect dependency comparison data and return structured dict."""
    print("\nFetching Maven dependencies...")
    maven_deps = get_maven_dependencies()
    print(f"Found {len(maven_deps)} Maven modules with dependencies")

    print("\nFetching SBT dependencies...")
    sbt_deps = get_sbt_dependencies()
    print(f"Found {len(sbt_deps)} SBT modules with dependencies")

    if not maven_deps or not sbt_deps:
        return {
            "mode": "deps",
            "error": "Could not compare dependencies - missing data from one build",
            "summary": {"total": 0, "matching": 0, "differing": 0},
            "modules": {},
        }

    common_modules = set(maven_deps.keys()) & set(sbt_deps.keys())
    only_maven_modules = set(maven_deps.keys()) - set(sbt_deps.keys())
    only_sbt_modules = set(sbt_deps.keys()) - set(maven_deps.keys())

    modules_data: Dict[str, Any] = {}
    differing = 0

    for module in sorted(common_modules):
        maven_set = maven_deps[module]
        sbt_set = sbt_deps[module]
        only_maven = maven_set - sbt_set
        only_sbt = sbt_set - maven_set

        entry: Dict[str, Any] = {
            "status": "match" if not (only_maven or only_sbt) else "differs",
            "maven_count": len(maven_set),
            "sbt_count": len(sbt_set),
        }
        if only_maven:
            entry["only_in_maven"] = sorted(only_maven)
        if only_sbt:
            entry["only_in_sbt"] = sorted(only_sbt)
        if only_maven or only_sbt:
            differing += 1

        modules_data[module] = entry

    return {
        "mode": "deps",
        "summary": {
            "total": len(common_modules),
            "matching": len(common_modules) - differing,
            "differing": differing,
            "only_in_maven_modules": sorted(only_maven_modules),
            "only_in_sbt_modules": sorted(only_sbt_modules),
        },
        "modules": modules_data,
    }


def print_dependencies_report(data: Dict[str, Any]) -> None:
    """Print dependency comparison in human-readable format."""
    if "error" in data:
        print(f"\n[warn] {data['error']}")
        return

    summary = data["summary"]
    modules = data["modules"]

    print(f"\nDependency Comparison ({summary['total']} common modules)")
    print("\u2500" * 72)

    for module, entry in modules.items():
        if entry["status"] == "match":
            continue
        print(f"\n  {module}:")
        if "only_in_maven" in entry:
            deps = entry["only_in_maven"]
            print(f"    Only in Maven ({len(deps)}):")
            for dep in deps[:5]:
                print(f"      - {dep}")
            if len(deps) > 5:
                print(f"      ... and {len(deps) - 5} more")
        if "only_in_sbt" in entry:
            deps = entry["only_in_sbt"]
            print(f"    Only in SBT ({len(deps)}):")
            for dep in deps[:5]:
                print(f"      - {dep}")
            if len(deps) > 5:
                print(f"      ... and {len(deps) - 5} more")

    if summary["only_in_maven_modules"]:
        print(f"\n  Modules only in Maven: {', '.join(summary['only_in_maven_modules'])}")
    if summary["only_in_sbt_modules"]:
        print(f"\n  Modules only in SBT: {', '.join(summary['only_in_sbt_modules'])}")

    print("\u2500" * 72)
    if summary["differing"] == 0:
        print(f"Result: All {summary['matching']} common modules have matching dependencies!")
    else:
        print(f"Result: {summary['differing']} modules have dependency differences")


def _self_test() -> bool:
    """Run self-tests for internal helpers. Returns True if all pass."""
    passed = 0
    failed = 0

    def check(input_name: str, expected: str) -> None:
        nonlocal passed, failed
        actual = normalize_jar_name(input_name)
        if actual == expected:
            passed += 1
        else:
            failed += 1
            print(f"  FAIL: normalize_jar_name({input_name!r})")
            print(f"        expected {expected!r}, got {actual!r}")

    print("Testing normalize_jar_name ...")

    # Standard Spark artifacts with Scala suffix
    check("spark-core_2.13-4.0.0-SNAPSHOT.jar", "spark-core_2.13")
    check("spark-sql_2.13-4.0.0-SNAPSHOT.jar", "spark-sql_2.13")
    check("spark-catalyst_2.13-4.0.0-SNAPSHOT.jar", "spark-catalyst_2.13")
    check("spark-mllib_2.13-4.0.0-SNAPSHOT.jar", "spark-mllib_2.13")
    check("spark-hive_2.13-4.0.0-SNAPSHOT.jar", "spark-hive_2.13")

    # Artifacts with digits in the name (the tricky cases)
    check(
        "spark-sql-kafka-0-10_2.13-4.0.0-SNAPSHOT.jar",
        "spark-sql-kafka-0-10_2.13",
    )
    check(
        "spark-streaming-kafka-0-10_2.13-4.0.0-SNAPSHOT.jar",
        "spark-streaming-kafka-0-10_2.13",
    )
    check(
        "spark-token-provider-kafka-0-10_2.13-4.0.0-SNAPSHOT.jar",
        "spark-token-provider-kafka-0-10_2.13",
    )

    # Compound module names
    check(
        "spark-connect-client-jvm_2.13-4.0.0-SNAPSHOT.jar",
        "spark-connect-client-jvm_2.13",
    )
    check(
        "spark-hive-thriftserver_2.13-4.0.0-SNAPSHOT.jar",
        "spark-hive-thriftserver_2.13",
    )
    check("spark-mllib-local_2.13-4.0.0-SNAPSHOT.jar", "spark-mllib-local_2.13")

    # Release versions (no SNAPSHOT)
    check("spark-core_2.13-4.0.0.jar", "spark-core_2.13")
    check("spark-core_2.13-3.5.1.jar", "spark-core_2.13")

    # Scala 2.12
    check("spark-core_2.12-4.0.0-SNAPSHOT.jar", "spark-core_2.12")
    check(
        "spark-sql-kafka-0-10_2.12-3.5.1.jar",
        "spark-sql-kafka-0-10_2.12",
    )

    # Scala 3
    check("spark-core_3-4.0.0-SNAPSHOT.jar", "spark-core_3")

    # No version at all (just artifact name)
    check("spark-core_2.13.jar", "spark-core_2.13")
    check("spark-core_2.13", "spark-core_2.13")

    # Non-Scala JARs (fallback to semver regex)
    check("commons-lang3-3.12.0.jar", "commons-lang3")
    check("guava-31.1-jre.jar", "guava")

    # No version, no Scala suffix
    check("some-lib.jar", "some-lib")
    check("some-lib", "some-lib")

    # Test JARs with -tests suffix
    check("spark-core_2.13-4.0.0-SNAPSHOT-tests.jar", "spark-core_2.13")

    # Assembly JARs (normally skipped, but normalize should still work)
    check(
        "spark-streaming-kafka-0-10-assembly_2.13-4.0.0-SNAPSHOT.jar",
        "spark-streaming-kafka-0-10-assembly_2.13",
    )

    # _find_module_dirs smoke test
    print("Testing _find_module_dirs ...")
    module_dirs = _find_module_dirs()
    if len(module_dirs) >= 20:
        passed += 1
    else:
        failed += 1
        print(f"  FAIL: _find_module_dirs() returned {len(module_dirs)} dirs, expected >= 20")
    # Spot-check a few known modules
    rel_paths = {str(d.relative_to(SPARK_HOME)) for d in module_dirs}
    for expected_mod in ("core", "sql/core", "connector/kafka-0-10-sql"):
        if expected_mod in rel_paths:
            passed += 1
        else:
            failed += 1
            print(f"  FAIL: _find_module_dirs() missing expected module '{expected_mod}'")

    print(f"  {passed} passed, {failed} failed")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(
        description="Compare SBT and Maven builds for Spark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--build-maven", action="store_true", help="Build with Maven before comparing"
    )
    parser.add_argument("--build-sbt", action="store_true", help="Build with SBT before comparing")
    parser.add_argument(
        "--build-both",
        action="store_true",
        help="Build with both Maven and SBT before comparing",
    )
    # Comparison mode (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--shading",
        action="store_true",
        help="Verify shading relocation rules against both builds",
    )
    mode_group.add_argument(
        "--assemblies-only", action="store_true", help="Compare assembly JARs only"
    )
    mode_group.add_argument(
        "--deps",
        action="store_true",
        help="Compare dependencies (slower, requires running Maven/SBT)",
    )
    mode_group.add_argument(
        "--self-test",
        action="store_true",
        help="Run internal self-tests and exit",
    )

    # Filtering (default JAR mode only)
    parser.add_argument(
        "--modules",
        type=str,
        help="Comma-separated list of modules to compare (e.g., core,sql,catalyst)",
    )
    parser.add_argument(
        "--matching-only",
        action="store_true",
        help="Only compare JARs that exist in both builds",
    )
    parser.add_argument(
        "--ignore-shaded",
        action="store_true",
        help="Ignore shaded classes/services and bundled deps in fat-JAR modules",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed class-level differences",
    )

    # Output
    parser.add_argument("--output", "-o", type=str, help="Write report to file")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output structured JSON instead of human-readable text",
    )
    parser.add_argument(
        "--maven-profiles",
        type=str,
        default="",
        help="Maven profiles to use (comma-separated, e.g., hive,yarn)",
    )

    args = parser.parse_args()

    if args.self_test:
        sys.exit(0 if _self_test() else 1)

    # Parse modules
    modules = None
    if args.modules:
        modules = [m.strip() for m in args.modules.split(",") if m.strip()]

    # Parse Maven profiles
    maven_profiles = None
    if args.maven_profiles:
        maven_profiles = [p.strip() for p in args.maven_profiles.split(",") if p.strip()]

    # Build if requested
    if args.build_both:
        if not build_maven(maven_profiles):
            sys.exit(1)
        if not build_sbt():
            sys.exit(1)
    elif args.build_maven:
        if not build_maven(maven_profiles):
            sys.exit(1)
    elif args.build_sbt:
        if not build_sbt():
            sys.exit(1)

    def _output_report(report: Dict[str, Any]) -> None:
        """Handle JSON output to stdout and/or file."""
        if args.json:
            print(json.dumps(report, indent=2))
        if args.output:
            Path(args.output).write_text(json.dumps(report, indent=2))
            if not args.json:
                print(f"\nJSON report written to: {args.output}")

    # Dependency comparison mode
    if args.deps:
        report = compare_dependencies_data()
        if args.json or args.output:
            _output_report(report)
        if not args.json:
            print_dependencies_report(report)
        if "error" in report or report["summary"]["differing"] > 0:
            sys.exit(1)
        return

    # Shading comparison mode
    if args.shading:
        report = compare_shading_data()
        if args.json or args.output:
            _output_report(report)
        if not args.json:
            print_shading_report(report)
        if report["summary"]["fail"] > 0:
            sys.exit(1)
        return

    # Assembly comparison mode
    if args.assemblies_only:
        report = compare_assembly_data()
        if args.json or args.output:
            _output_report(report)
        if not args.json:
            print_assembly_report(report)
        if report["issues"] > 0:
            sys.exit(1)
        return

    # Find JARs
    if not args.json:
        print("\nSearching for Maven JARs...")
    maven_jars = find_maven_jars(modules)
    if not args.json:
        print(f"Found {len(maven_jars)} Maven JARs")
        print("\nSearching for SBT JARs...")
    sbt_jars = find_sbt_jars(modules)
    if not args.json:
        print(f"Found {len(sbt_jars)} SBT JARs")

    if not maven_jars and not sbt_jars:
        print("\n[error] No JARs found. Please build first with --build-both")
        sys.exit(1)

    # Compare
    results = compare_jars(
        maven_jars,
        sbt_jars,
        matching_only=args.matching_only,
        ignore_shaded=args.ignore_shaded,
    )

    # Build structured report
    report = build_report_dict(results)

    # Output
    if args.json or args.output:
        _output_report(report)
    if not args.json:
        print_report(results, args.verbose)

    # Exit with error if there are discrepancies
    matches = sum(1 for r in results.values() if r.is_match)
    if matches != len(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
