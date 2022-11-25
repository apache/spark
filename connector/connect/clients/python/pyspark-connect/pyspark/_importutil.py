import pyspark
import importlib
from importlib.abc import MetaPathFinder
from importlib.machinery import ModuleSpec, SourceFileLoader
from importlib.util import spec_from_file_location
from typing import Sequence, Optional
import sys
import os.path


def _install_sparkconnect_finder():
    pyspark_root = pyspark.__path__[0]
    sql_root = os.path.join(pyspark_root, "sql")
    connect_root = os.path.join(sql_root, "connect")

    namespace_map = {
        "pyspark.sql": {
            "path": connect_root,
            "erases": "pyspark.sql.connect"
        },
        "pyspark.sql.proto": {
            "path": os.path.join(connect_root, "proto"),
            "erases": "pyspark.sql.connect.proto",
        },
        "pyspark.sql.typing": {
            "path": os.path.join(connect_root, "typing"),
            "erases": "pyspark.sql.connect.typing",
        },
    }

    class SparkConnectPathFinder(MetaPathFinder):
        def find_spec(
            self, fullname: str, path: Optional[Sequence[str]], target
        ) -> Optional[ModuleSpec]:
            if not fullname.startswith("pyspark"):
                return None

            # Rewriting the target namespaces as aliases
            if fullname in namespace_map:
                conf = namespace_map[fullname]
                filepath = os.path.join(conf["path"], "__init__.py")
                if not os.path.exists(filepath):
                    raise ImportError(
                        "Couldn't find package {} at {}".format(fullname, filepath)
                    )
                loader = SourceFileLoader(fullname, filepath)
                spec = ModuleSpec(fullname, loader, origin=filepath, is_package=True)
                return spec


            parts = fullname.rsplit(".", 1)
            if len(parts) < 2:
                # not in a package
                return None


            # First check if the package / module exists in the local overrides.
            parent, local_name = parts
            if parent in namespace_map:
                conf = namespace_map[parent]
                root = conf["path"]

                modulepath = os.path.join(root, local_name)
                package_filepath = os.path.join(modulepath, "__init__.py")
                if os.path.exists(package_filepath):
                    # This is a package and is not in the namespace_map
                    return None

                is_package = False
                filepath = "{}.py".format(modulepath)

                if os.path.exists(filepath):
                    loader = SourceFileLoader(fullname, filepath)
                    spec = ModuleSpec(fullname, loader, origin=filepath, is_package=is_package)
                    spec = spec_from_file_location(fullname, filepath)
                    return spec


            # Check if we can find the full file or directory somewhere.
            parts = fullname.split(".", 1)
            if len(parts) < 2:
                return None

            parent, local_name = parts
            os_path = os.path.join(*local_name.split("."))
            module_path = os.path.join(pyspark_root, os_path, "__init__.py")
            if os.path.exists(module_path):
                loader = SourceFileLoader(fullname, module_path)
                spec = ModuleSpec(fullname, loader, origin=module_path, is_package=True)
                return spec

            filepath = os.path.join(pyspark_root, "{}.py".format(os_path))
            if os.path.exists(filepath):
                loader = SourceFileLoader(fullname, filepath)
                spec = ModuleSpec(fullname, loader, origin=filepath, is_package=False)
                return spec

            return None

    sys.meta_path.insert(0, SparkConnectPathFinder())


def install():
    _install_sparkconnect_finder()
