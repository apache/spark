import os
import faulthandler

class FaultHandlerIntegration:
    def __init__(self):
        self._log_path = None
        self._log_file = None
        self._periodic_dump = False

    def start(self):
        self._log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
        tracebackDumpIntervalSeconds = os.environ.get("PYTHON_TRACEBACK_DUMP_INTERVAL_SECONDS", None)
        if self._log_path:
            self._log_path = os.path.join(self._log_path, str(os.getpid()))
            self._log_file = open(self._log_path, "w")

            faulthandler.enable(file=log_file)

            if tracebackDumpIntervalSeconds is not None and int(tracebackDumpIntervalSeconds) > 0:
                self._periodic_dump = True
                faulthandler.dump_traceback_later(int(tracebackDumpIntervalSeconds), repeat=True)

    def stop():
        if self._periodic_dump:
            faulthandler.cancel_dump_traceback_later()
            self._periodic_dump = False
        if self._log_path:
            faulthandler.disable()
            if self._log_file:
                self._log_file.close()
                self._log_file = None
            os.remove(self._log_path)
            self._log_path = None
