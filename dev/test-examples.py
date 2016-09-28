import os
import os.path
import subprocess
from threading import Lock, Thread
from sparktestsupport import SPARK_HOME
import logging
import sys
from optparse import OptionParser
import tempfile
import time
if sys.version < '3':
    import Queue
else:
    import queue as Queue

EXAMPLES_DIR = os.path.join(SPARK_HOME, "examples/src/main/")
examples_failed = []

LOG_FILE = os.path.join(SPARK_HOME, "logs/spark-examples.log")
LOGGER = logging.getLogger()

FAILURE_REPORTING_LOCK = Lock()


def put_python_examples(task_queue):
    python_examples = os.path.join(EXAMPLES_DIR, "python/")
    for dirpath, dirnames, filenames in os.walk(python_examples):
        for f in filenames:
            print os.path.join(dirpath, f)


def run_example(example):
    if example.endswith('.scala') or example.endswith('.java'):
        _bin = "bin/run-example"
    else:
        _bin = "bin/spark-submit"
    spark_submit = os.path.join(SPARK_HOME, _bin)
    cmd = [spark_submit, "--master", "local[4]", example]
    LOGGER.info("Will run {}".format(example))
    try:
        per_test_output = tempfile.TemporaryFile()
        retcode = subprocess.Popen(cmd,
                                   stdout=per_test_output,
                                   stderr=per_test_output).wait()
    except:
        LOGGER.exception("Got exception while running %s", example)
        os._exit(1)
    if retcode != 0:
        try:
            with FAILURE_REPORTING_LOCK:
                with open(LOG_FILE, 'ab') as log_file:
                    per_test_output.seek(0)
                    log_file.write(example + ' ' + '*' * 72 + '\n')
                    log_file.writelines(per_test_output)
                per_test_output.seek(0)
                per_test_output.close()
        except:
            LOGGER.exception(
                "Got an exception while trying to print failed test output")
        finally:
            LOGGER.error("\033[91m" + " FAILED " + example +
                         '; see logs. \033[0m')
    else:
        LOGGER.info("Finished %s" % example)


def parse_opts():
    parser = OptionParser()
    parser.add_option(
        "-p",
        "--parallelism",
        type="int",
        default=4,
        help="The number of suites to test in parallel (default %default)")
    parser.add_option("--verbose",
                      action="store_true",
                      help="Enable additional debug logging")
    parser.add_option(
        "--languages",
        type="string",
        default="r,python,java,scala".split(','),
        help="A comma-separated list of Python modules to test (default: %default)")

    (opts, args) = parser.parse_args()
    if args:
        parser.error("Unsupported arguments: %s" % ' '.join(args))
    if opts.parallelism < 1:
        parser.error("Parallelism cannot be less than 1")
    return opts


def process_queue(task_queue):
    while True:
        try:
            example = task_queue.get_nowait()
        except Queue.Empty:
            break
        try:
            run_example(example)
        finally:
            task_queue.task_done()


def main():
    opts = parse_opts()
    if (opts.verbose):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(stream=sys.stdout,
                        level=log_level,
                        format="%(message)s")
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    LOGGER.info("Running Spark Examples tests. Output is in %s", LOG_FILE)
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    task_queue = Queue.Queue()

    def put_tests(task_queue, language):
        lang_path = os.path.join(EXAMPLES_DIR, lang)
        for dirpath, dirnames, filenames in os.walk(lang_path):
            for f in filenames:
                if os.path.splitext(f)[-1] in ['.py', '.R', '.java', '.scala']:
                    task_queue.put(os.path.join(dirpath, f))

    for lang in opts.languages.split(','):
        put_tests(task_queue, lang)

    start_time = time.time()
    for _ in range(opts.parallelism):
        worker = Thread(target=process_queue, args=(task_queue, ))
        worker.daemon = True
        worker.start()
    try:
        process_queue(task_queue)
        task_queue.join()
    except (KeyboardInterrupt, SystemExit):
        print "System exit due to interrupt"
        sys.exit(-1)
    total_duration = time.time() - start_time
    LOGGER.info("Tests passed in %i seconds", total_duration)


def _main():
    for dirpath, dirnames, filenames in os.walk():
        for f in filenames:
            print os.path.join(dirpath, f)


if __name__ == "__main__":
    main()
