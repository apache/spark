from base_executor import LocalExecutor
from base_executor import SequentialExecutor
from base_executor import CeleryExecutor

DEFAULT_EXECUTOR = SequentialExecutor()
# DEFAULT_EXECUTOR = CeleryExecutor()
