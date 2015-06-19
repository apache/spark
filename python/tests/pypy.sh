. tests/common

# Try to test with PyPy
if [ $(which pypy) ]; then
    export PYSPARK_PYTHON="pypy"
    echo "Testing with PyPy version:"
    $PYSPARK_PYTHON --version

    run_core_tests
    run_sql_tests
    run_streaming_tests
else
    echo "Skipping tests with PyPy"
    FAILED=1
fi
