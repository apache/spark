from enum import Enum, unique
from inspect import getmembers, isclass, isfunction, signature
from typing import Callable, Dict, List, Set, TextIO

import pandas as pd
import pandas.core.groupby as pdg
import pandas.core.window as pdw
import pyspark.pandas as ps
import pyspark.pandas.groupby as psg
import pyspark.pandas.window as psw

MAX_MISSING_PARAMS_SIZE = 5
COMMON_PARAMETER_SET = {"kwargs", "args", "cls"}
FILE_PATH_PREFIX = "./user_guide/pandas_on_spark"
HEADER_DOC_FILE = f"{FILE_PATH_PREFIX}/supported_pandas_api_header.txt"
TARGET_DOC_FILE = f"{FILE_PATH_PREFIX}/supported_pandas_api.rst"
MODULE_GROUP_MATCH = [(pd, ps), (pdw, psw), (pdg, psg)]


@unique
class Implemented(Enum):
    IMPLEMENTED = "Y"
    NOT_IMPLEMENTED = "N"
    PARTIALLY_IMPLEMENTED = "P"


class SupportedStatus:
    def __init__(self, implemented: str, missing: str = ""):
        self.implemented = implemented
        self.missing = missing


class SuppportedStatusRSTGenerator:
    def __init__(self):
        self.all_supported_status = {}

    def execute(self) -> None:
        for pd_module_group, ps_module_group in MODULE_GROUP_MATCH:
            pd_modules = self.get_pd_modules(pd_module_group)
            self.update_all_supported_status(
                pd_modules, pd_module_group, ps_module_group
            )
        self.write_rst()

    def create_supported_by_module(
        self, module_name: str, pd_module_group, ps_module_group
    ) -> Dict[str, SupportedStatus]:
        pd_module = (
            getattr(pd_module_group, module_name) if module_name else pd_module_group
        )
        try:
            ps_module = (
                getattr(ps_module_group, module_name)
                if module_name
                else ps_module_group
            )
        except AttributeError:
            # module not implemented
            return {}

        pd_funcs = dict(
            [m for m in getmembers(pd_module, isfunction) if not m[0].startswith("_")]
        )
        if not pd_funcs:
            return {}

        ps_funcs = dict(
            [m for m in getmembers(ps_module, isfunction) if not m[0].startswith("_")]
        )

        return self.organize_by_implementation_status(
            module_name, pd_funcs, ps_funcs, pd_module_group, ps_module_group
        )

    def organize_by_implementation_status(
        self,
        module_name: str,
        pd_funcs: Dict[str, Callable],
        ps_funcs: Dict[str, Callable],
        pd_module_group,
        ps_module_group,
    ) -> Dict[str, SupportedStatus]:
        pd_dict = {}
        for pd_func_name, pd_func in pd_funcs.items():
            ps_func = ps_funcs.get(pd_func_name)
            if ps_func:
                missing_set = (
                    set(signature(pd_func).parameters)
                    - set(signature(ps_func).parameters)
                    - COMMON_PARAMETER_SET
                )
                if missing_set:
                    # partially implemented
                    pd_dict[pd_func_name] = SupportedStatus(
                        Implemented.PARTIALLY_IMPLEMENTED.value,
                        self.transform_missing(
                            module_name,
                            pd_func_name,
                            missing_set,
                            pd_module_group.__name__,
                            ps_module_group.__name__,
                        ),
                    )
                else:
                    # implemented including it's whole parameter
                    pd_dict[pd_func_name] = SupportedStatus(
                        Implemented.IMPLEMENTED.value
                    )
            else:
                # not implemented yet
                pd_dict[pd_func_name] = SupportedStatus(
                    Implemented.NOT_IMPLEMENTED.value
                )
        return pd_dict

    def transform_missing(
        self,
        module_name: str,
        pd_func_name: str,
        missing_set: Set[str],
        pd_module_path: str,
        ps_module_path: str,
    ) -> str:
        missing_str = " , ".join(
            list(
                map(lambda x: f"``{x}``", sorted(missing_set)[:MAX_MISSING_PARAMS_SIZE])
            )
        )
        if len(missing_set) > MAX_MISSING_PARAMS_SIZE:
            module_dot_func = (
                f"{module_name}.{pd_func_name}" if module_name else pd_func_name
            )
            additional_str = (
                " and more. See the "
                f"`{pd_module_path}.{module_dot_func} "
                "<https://pandas.pydata.org/docs/reference/api/"
                f"{pd_module_path}.{module_dot_func}.html>`__ and "
                f"`{ps_module_path}.{module_dot_func} "
                "<https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/api/"
                f"{ps_module_path}.{module_dot_func}.html>`__ for detail."
            )
            missing_str += additional_str
        return missing_str

    def get_pd_modules(self, pd_module_group) -> List[str]:
        return sorted(
            [
                m[0]
                for m in getmembers(pd_module_group, isclass)
                if not m[0].startswith("_")
            ]
        )

    def update_all_supported_status(
        self, pd_modules: List[str], pd_module_group, ps_module_group
    ) -> None:
        pd_modules += [""]  # for General Function APIs
        for module_name in pd_modules:
            supported_status = self.create_supported_by_module(
                module_name, pd_module_group, ps_module_group
            )
            if supported_status:
                self.all_supported_status[
                    (module_name, ps_module_group.__name__)
                ] = supported_status

    def draw_table(
        self,
        module_name: str,
        module_path: str,
        supported_status: Dict[str, SupportedStatus],
        w_fd: TextIO,
    ) -> None:
        lines = []
        lines.append(f"Supported ")
        if module_name:
            lines.append(module_name)
        else:
            lines.append(f"General Function")
        lines.append(f" APIs\n")
        lines.append("-" * 100)
        lines.append("\n")
        lines.append(f".. currentmodule:: {module_path}")
        if module_name:
            lines.append(f".{module_name}\n")
        else:
            lines.append(f"\n")
        lines.append("\n")
        lines.append(f".. list-table::\n")
        lines.append("    :header-rows: 1\n")
        lines.append("\n")
        lines.append("    * - API\n")
        lines.append("      - Implemented\n")
        lines.append("      - Missing parameters\n")
        for func_str, status in supported_status.items():
            func_str = self.escape_func_str(func_str)
            if status.implemented == Implemented.NOT_IMPLEMENTED.value:
                lines.append(f"    * - {func_str}\n")
            else:
                lines.append(f"    * - :func:`{func_str}`\n")
            lines.append(f"      - {status.implemented}\n")
            lines.append("      - \n") if not status.missing else lines.append(
                f"      - {status.missing}\n"
            )
        w_fd.writelines(lines)

    def escape_func_str(self, func_str: str) -> str:
        if func_str.endswith("_"):
            return func_str[:-1] + "\_"
        else:
            return func_str

    def write_rst(self) -> None:
        with open(TARGET_DOC_FILE, "w") as w_fd:
            self.write_header_file(w_fd)
            for module_info, supported_status in self.all_supported_status.items():
                module, module_path = module_info
                if supported_status:
                    self.draw_table(module, module_path, supported_status, w_fd)
                    w_fd.write("\n")

    def write_header_file(self, w_fd: TextIO) -> None:
        with open(HEADER_DOC_FILE, "r") as r_fd:
            header_data = r_fd.read()
            w_fd.write(header_data)
