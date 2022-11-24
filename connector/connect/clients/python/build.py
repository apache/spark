# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import re
import shutil


SPARK_HOME = os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
RELEASE_PATH = os.path.join(os.path.dirname(__file__), "pyspark-connect")
DEST_DIR = os.path.join(RELEASE_PATH, "pyspark")
SRC_DIR = os.path.join(SPARK_HOME, "python", "pyspark")


def extract_modules(path):
    """Extract all modules that are referenced from pyspark that we
    need to copy over."""
    modules = []
    for root, dirs, files in os.walk(path):
        for f in files:
            full_name = os.path.join(root, f)
            if not full_name.endswith("py"):
                continue
            with open(full_name, "r") as fid:
                for l in fid.readlines():
                    m = re.match("import (pyspark\.sql\.[^c]*?)\s", l)
                    if m is not None:
                        modules.append(m.group(1))
                    m = re.match("from pyspark import (.*)$", l)
                    if m is not None:
                        modules.append("pyspark." + m.group(1))
                    m = re.match("from pyspark\.sql\.([^c]*?)\simport.*$", l)
                    if m is not None:
                        modules.append("pyspark.sql." + m.group(1))
    return modules


def rewrite_imports(path):
    for root, dirs, files in os.walk(path):
        for f in files:
            full_name = os.path.join(root, f)
            if not full_name.endswith("py"):
                    continue
            # Read all data
            with open(full_name, "r") as fid:
                data = fid.read()
                pattern = "import pyspark.sql.connect"
                data = re.sub(pattern, "import pyspark.sql", data)
                pattern = "from pyspark.sql.connect"
                data = re.sub(pattern, "from pyspark.sql", data)
            with open(full_name, "w") as fid:
                fid.write(data)


def main():
    # Clean up first
    shutil.rmtree(DEST_DIR, ignore_errors=True)
    os.makedirs(DEST_DIR, exist_ok=True)
    with open(DEST_DIR + "/__init__.py", "w+") as f:
        f.write("")

    # Copy Spark Connect
    shutil.copytree(SRC_DIR + "/sql/connect", DEST_DIR + "/sql")

    # Extract all modules that are not connect from the sources.
    modules = set(extract_modules(DEST_DIR + "/sql"))
    for m in modules:
      ref_path = m.split(".")[1:]
      s = os.path.join(SRC_DIR, *ref_path)
      d = os.path.join(DEST_DIR, *ref_path)
      if os.path.isdir(s):
        shutil.copytree(s, d)
      else:
        shutil.copy(s + ".py", d + ".py")


    print("Extracting new modules...")
    new_mods = set(extract_modules(DEST_DIR))
    print(new_mods)

    # Lastly rewrite all imports to drop the "connect" in the middle
    rewrite_imports(DEST_DIR)

if __name__ == "__main__":
    main()
