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
import hashlib
import os
import re
import tarfile
import time
import traceback
import urllib.request
from shutil import rmtree
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from http.client import HTTPResponse


# NOTE that we shouldn't import pyspark here because this is used in
# setup.py, and assume there's no PySpark imported.

DEFAULT_HADOOP = "hadoop3"
DEFAULT_HIVE = "hive2.3"
SUPPORTED_HADOOP_VERSIONS = ["hadoop3", "without-hadoop"]
SUPPORTED_HIVE_VERSIONS = ["hive2.3"]
UNSUPPORTED_COMBINATIONS = []  # type: ignore

# Official ASF-controlled hosts serving release checksums. Integrity metadata
# must come from these origins, never from a (community) mirror, so that a
# tampered tarball served by a mirror fails verification.
CHECKSUM_ORIGINS = [
    "https://downloads.apache.org/spark",
    "https://archive.apache.org/dist/spark",
]


def checked_package_name(spark_version: str, hadoop_version: str, hive_version: str) -> str:
    """
    Check the generated package name, here we need to use the final hadoop version.
    """
    return "%s-bin-%s" % (spark_version, hadoop_version)


def checked_versions(
    spark_version: str, hadoop_version: str, hive_version: str
) -> tuple[str, str, str]:
    """
    Check the valid combinations of supported versions in Spark distributions.

    Parameters
    ----------
    spark_version : str
        Spark version. It should be X.X.X such as '3.0.0' or spark-3.0.0.
    hadoop_version : str
        Hadoop version. It should be X such as '2' or 'hadoop2'.
        'without' and 'without-hadoop' are supported as special keywords for Hadoop free
        distribution.
    hive_version : str
        Hive version. It should be X.X such as '2.3' or 'hive2.3'.

    Parameters
    ----------
    tuple
        fully-qualified versions of Spark, Hadoop and Hive in a tuple.
        For example, spark-3.2.0, hadoop3 and hive2.3.
    """
    if re.match("^[0-9]+\\.[0-9]+\\.[0-9]+(?:\\.dev[0-9]+)?$", spark_version):
        spark_version = "spark-%s" % spark_version
    if not spark_version.startswith("spark-"):
        raise RuntimeError(
            "Spark version should start with 'spark-' prefix; however, got %s" % spark_version
        )

    if hadoop_version == "without":
        hadoop_version = "without-hadoop"
    elif re.match("^[0-9]+$", hadoop_version):
        hadoop_version = "hadoop%s" % hadoop_version

    if hadoop_version not in SUPPORTED_HADOOP_VERSIONS:
        raise RuntimeError(
            "Spark distribution of %s is not supported. Hadoop version should be "
            "one of [%s]" % (hadoop_version, ", ".join(SUPPORTED_HADOOP_VERSIONS))
        )

    if re.match("^[0-9]+\\.[0-9]+$", hive_version):
        hive_version = "hive%s" % hive_version

    if hive_version not in SUPPORTED_HIVE_VERSIONS:
        raise RuntimeError(
            "Spark distribution of %s is not supported. Hive version should be "
            "one of [%s]" % (hive_version, ", ".join(SUPPORTED_HADOOP_VERSIONS))
        )

    return spark_version, convert_old_hadoop_version(spark_version, hadoop_version), hive_version


def convert_old_hadoop_version(spark_version: str, hadoop_version: str) -> str:
    # check if Spark version <= 3.2, if so, convert hadoop3 to hadoop3.2 and hadoop2 to hadoop2.7
    version_dict = {
        "hadoop3": "hadoop3.2",
        "hadoop2": "hadoop2.7",
        "without": "without",
        "without-hadoop": "without-hadoop",
    }
    spark_version_parts = re.search(
        "^spark-([0-9]+)\\.([0-9]+)\\.[0-9]+(?:\\.dev[0-9]+)?$", spark_version
    )
    assert spark_version_parts is not None
    spark_major_version = int(spark_version_parts.group(1))
    spark_minor_version = int(spark_version_parts.group(2))
    if spark_major_version < 3 or (spark_major_version == 3 and spark_minor_version <= 2):
        hadoop_version = version_dict[hadoop_version]
    return hadoop_version


def install_spark(dest: str, spark_version: str, hadoop_version: str, hive_version: str) -> None:
    """
    Installs Spark that corresponds to the given Hadoop version in the current
    library directory.

    Parameters
    ----------
    dest : str
        The location to download and install the Spark.
    spark_version : str
        Spark version. It should be spark-X.X.X form.
    hadoop_version : str
        Hadoop version. It should be hadoopX.X
        such as 'hadoop2.7' or 'without-hadoop'.
    hive_version : str
        Hive version. It should be hiveX.X such as 'hive2.3'.
    """

    package_name = checked_package_name(spark_version, hadoop_version, hive_version)
    package_local_path = os.path.join(dest, "%s.tgz" % package_name)
    explicit_mirror = "PYSPARK_RELEASE_MIRROR" in os.environ
    if explicit_mirror:
        sites = [os.environ["PYSPARK_RELEASE_MIRROR"]]
    else:
        # Plain-http mirrors are trivially tampered with in transit.
        sites = [site for site in get_preferred_mirrors() if site.startswith("https://")]
    print("Trying to download Spark %s from [%s]" % (spark_version, ", ".join(sites)))

    pretty_pkg_name = "%s for Hadoop %s" % (
        spark_version,
        "Free build" if hadoop_version == "without" else hadoop_version,
    )

    # Verify every downloaded tarball against the SHA-512 digest published on
    # the official Apache hosts. Setting PYSPARK_UNVERIFIED_DOWNLOAD=1 opts out
    # (for example for air-gapped setups with no route to downloads.apache.org).
    expected_digest = None
    if os.environ.get("PYSPARK_UNVERIFIED_DOWNLOAD", "") != "1":
        try:
            expected_digest = _fetch_official_checksum(spark_version, package_name)
        except Exception:
            if explicit_mirror:
                # An explicitly configured mirror (e.g. an internal mirror or a
                # release-candidate staging area) may host packages that have no
                # published official checksum; defer to the user's explicit
                # trust in that mirror.
                print(
                    "Warning: no official checksum is available for %s; skipping "
                    "verification for the explicitly configured mirror." % package_name
                )
            else:
                raise

    last_error = None
    for site in sites:
        os.makedirs(dest, exist_ok=True)
        url = "%s/spark/%s/%s.tgz" % (site, spark_version, package_name)

        tar = None
        try:
            print("Downloading %s from:\n- %s" % (pretty_pkg_name, url))
            _download_with_retries(url, package_local_path)

            if expected_digest is not None:
                print("Verifying the SHA-512 checksum of %s" % package_local_path)
                _verify_checksum(package_local_path, expected_digest)

            print("Installing to %s" % dest)
            tar = tarfile.open(package_local_path, "r:gz")
            _extract_tar(tar, package_name, dest)
            return
        except Exception as e:
            last_error = e
            print("Failed to download %s from %s:" % (pretty_pkg_name, url))
            traceback.print_exc()
            rmtree(dest, ignore_errors=True)
        finally:
            if tar is not None:
                tar.close()
            if os.path.exists(package_local_path):
                os.remove(package_local_path)
    raise OSError("Unable to download %s." % pretty_pkg_name) from last_error


def _extract_tar(tar: tarfile.TarFile, package_name: str, dest: str) -> None:
    """
    Extract the members of ``tar`` into ``dest``, stripping the top-level
    ``package_name`` directory from each member path.

    Guards against path traversal ("zip slip"): ``os.path.relpath`` does not
    strip ``..`` segments, so a crafted member could otherwise resolve outside
    ``dest``. Any member whose resolved destination escapes ``dest`` is
    rejected instead of extracted.

    Note: tarfile's ``filter="data"`` (PEP 706) rejects such members natively and
    would replace this manual check, but it is only generally available from
    Python 3.12.0 (backported to 3.11.4+), so we keep the explicit check while
    Spark still supports Python 3.11.
    """
    dest_root = os.path.realpath(dest)
    for member in tar.getmembers():
        if member.name == package_name:
            # Skip the root directory.
            continue
        member.name = os.path.relpath(member.name, package_name + os.path.sep)
        resolved = os.path.realpath(os.path.join(dest, member.name))
        if resolved != dest_root and not resolved.startswith(dest_root + os.sep):
            raise ValueError(
                "Archive member '%s' would extract outside of the destination "
                "directory; refusing to extract." % member.name
            )
        tar.extract(member, dest)


def _parse_sha512_checksum(content: str, package_file_name: str) -> str:
    """
    Parse the contents of an Apache release ``.sha512`` file into a lowercase
    hex digest. Both formats published for Spark releases are accepted:
    ``sha512sum`` style (``<hex digest>  <file name>``) and GPG
    ``--print-md`` style (``<file name>: ABCD 1234 ...``).
    """
    digest = (
        content.replace(package_file_name, "")
        .replace(":", "")
        .replace("\n", "")
        .replace("\t", "")
        .replace(" ", "")
        .strip()
        .lower()
    )
    if not re.match("^[0-9a-f]{128}$", digest):
        raise ValueError(
            "Could not parse a SHA-512 digest for %s from the checksum file." % package_file_name
        )
    return digest


def _fetch_official_checksum(spark_version: str, package_name: str) -> str:
    """
    Fetch the SHA-512 checksum for the given package from the official Apache
    distribution hosts (never from a mirror), so that a tarball served by an
    untrusted mirror can be verified against it.
    """
    file_name = "%s.tgz" % package_name
    last_error = None
    for origin in CHECKSUM_ORIGINS:
        url = "%s/%s/%s.sha512" % (origin, spark_version, file_name)
        try:
            response = urllib.request.urlopen(url, timeout=60)
            return _parse_sha512_checksum(response.read().decode("utf-8"), file_name)
        except Exception as e:
            last_error = e
    raise OSError(
        "Unable to fetch the checksum for %s: %s. Set PYSPARK_UNVERIFIED_DOWNLOAD=1 "
        "to skip verification." % (file_name, last_error)
    )


def _verify_checksum(path: str, expected_digest: str) -> None:
    """
    Verify that the SHA-512 digest of the file at ``path`` matches
    ``expected_digest``, raising ``ValueError`` otherwise.
    """
    sha512 = hashlib.sha512()
    with open(path, mode="rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha512.update(chunk)
    actual_digest = sha512.hexdigest()
    if actual_digest != expected_digest:
        raise ValueError(
            "SHA-512 mismatch for %s: expected %s but got %s. The downloaded file is "
            "corrupt or has been tampered with." % (path, expected_digest, actual_digest)
        )


def get_preferred_mirrors() -> list[str]:
    mirror_urls = []
    for _ in range(3):
        try:
            response = urllib.request.urlopen(
                "https://www.apache.org/dyn/closer.lua?preferred=true", timeout=10
            )
            mirror_urls.append(response.read().decode("utf-8"))
        except Exception:
            # If we can't get a mirror URL, skip it. No retry.
            pass

    default_sites = [
        "https://dlcdn.apache.org/",
        "https://archive.apache.org/dist",
        "https://dist.apache.org/repos/dist/release",
    ]
    return list(set(mirror_urls)) + [x for x in default_sites if x not in mirror_urls]


def _download_with_retries(url: str, path: str, max_retries: int = 3, timeout: int = 600) -> None:
    """
    Download a file from a URL with retry logic and timeout handling.

    Parameters
    ----------
    url : str
        The URL to download from.
    path : str
        The local file path to save the downloaded file.
    max_retries : int
        Maximum number of retry attempts per URL.
    timeout : int
        Timeout in seconds for the HTTP request.
    """
    for attempt in range(max_retries):
        try:
            response = urllib.request.urlopen(url, timeout=timeout)
            download_to_file(response, path)
            return
        except Exception as e:
            if os.path.exists(path):
                os.remove(path)
            if attempt < max_retries - 1:
                wait = 2**attempt * 5
                print(
                    "Download attempt %d/%d failed: %s. Retrying in %d seconds..."
                    % (attempt + 1, max_retries, str(e), wait)
                )
                time.sleep(wait)
            else:
                raise


def download_to_file(response: "HTTPResponse", path: str, chunk_size: int = 1024 * 1024) -> None:
    total_size = int(response.info().get("Content-Length", "0").strip())
    bytes_so_far = 0

    with open(path, mode="wb") as dest:
        while True:
            chunk = response.read(chunk_size)
            bytes_so_far += len(chunk)
            if not chunk:
                break
            dest.write(chunk)
            print(
                "Downloaded %d of %d bytes (%0.2f%%)"
                % (bytes_so_far, total_size, round(float(bytes_so_far) / total_size * 100, 2))
            )
