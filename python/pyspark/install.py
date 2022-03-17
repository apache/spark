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
import os
import re
import tarfile
import traceback
import urllib.request
from shutil import rmtree

# NOTE that we shouldn't import pyspark here because this is used in
# setup.py, and assume there's no PySpark imported.

DEFAULT_HADOOP = "hadoop3"
DEFAULT_HIVE = "hive2.3"
SUPPORTED_HADOOP_VERSIONS = ["hadoop2", "hadoop3", "without-hadoop"]
SUPPORTED_HIVE_VERSIONS = ["hive2.3"]
UNSUPPORTED_COMBINATIONS = []  # type: ignore


def checked_package_name(spark_version, hadoop_version, hive_version):
    """
    Check the generated package name, here we need to use the final hadoop version.
    """
    return "%s-bin-%s" % (spark_version, hadoop_version)


def checked_versions(spark_version, hadoop_version, hive_version):
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
    if re.match("^[0-9]+\\.[0-9]+\\.[0-9]+$", spark_version):
        spark_version = "spark-%s" % spark_version
    if not spark_version.startswith("spark-"):
        raise RuntimeError(
            "Spark version should start with 'spark-' prefix; however, " "got %s" % spark_version
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


def convert_old_hadoop_version(spark_version, hadoop_version):
    # check if Spark version <= 3.2, if so, convert hadoop3 to hadoop3.2 and hadoop2 to hadoop2.7
    version_dict = {
        "hadoop3": "hadoop3.2",
        "hadoop2": "hadoop2.7",
        "without": "without",
        "without-hadoop": "without-hadoop",
    }
    spark_version_parts = re.search("^spark-([0-9]+)\\.([0-9]+)\\.[0-9]+$", spark_version)
    spark_major_version = int(spark_version_parts.group(1))
    spark_minor_version = int(spark_version_parts.group(2))
    if spark_major_version < 3 or (spark_major_version == 3 and spark_minor_version <= 2):
        hadoop_version = version_dict[hadoop_version]
    return hadoop_version


def install_spark(dest, spark_version, hadoop_version, hive_version):
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
    if "PYSPARK_RELEASE_MIRROR" in os.environ:
        sites = [os.environ["PYSPARK_RELEASE_MIRROR"]]
    else:
        sites = get_preferred_mirrors()
    print("Trying to download Spark %s from [%s]" % (spark_version, ", ".join(sites)))

    pretty_pkg_name = "%s for Hadoop %s" % (
        spark_version,
        "Free build" if hadoop_version == "without" else hadoop_version,
    )

    for site in sites:
        os.makedirs(dest, exist_ok=True)
        url = "%s/spark/%s/%s.tgz" % (site, spark_version, package_name)

        tar = None
        try:
            print("Downloading %s from:\n- %s" % (pretty_pkg_name, url))
            download_to_file(urllib.request.urlopen(url), package_local_path)

            print("Installing to %s" % dest)
            tar = tarfile.open(package_local_path, "r:gz")
            for member in tar.getmembers():
                if member.name == package_name:
                    # Skip the root directory.
                    continue
                member.name = os.path.relpath(member.name, package_name + os.path.sep)
                tar.extract(member, dest)
            return
        except Exception:
            print("Failed to download %s from %s:" % (pretty_pkg_name, url))
            traceback.print_exc()
            rmtree(dest, ignore_errors=True)
        finally:
            if tar is not None:
                tar.close()
            if os.path.exists(package_local_path):
                os.remove(package_local_path)
    raise IOError("Unable to download %s." % pretty_pkg_name)


def get_preferred_mirrors():
    mirror_urls = []
    for _ in range(3):
        try:
            response = urllib.request.urlopen(
                "https://www.apache.org/dyn/closer.lua?preferred=true"
            )
            mirror_urls.append(response.read().decode("utf-8"))
        except Exception:
            # If we can't get a mirror URL, skip it. No retry.
            pass

    default_sites = [
        "https://archive.apache.org/dist",
        "https://dist.apache.org/repos/dist/release",
    ]
    return list(set(mirror_urls)) + default_sites


def download_to_file(response, path, chunk_size=1024 * 1024):
    total_size = int(response.info().get("Content-Length").strip())
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
