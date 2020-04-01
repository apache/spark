# Spark Developer Scripts

This directory contains scripts useful to developers when packaging,
testing, or committing to Spark.

Many of these scripts require Apache credentials to work correctly.

## Managing Python-based Development Requirements

* For local development, use `requirements.txt`.
* For continuous integration and release engineering, use `requirements-pinned.txt`.
* `requirements-pinned.txt` is generated automatically from `requirements.txt`, so don't update it by hand.

    To update `requirements-pinned.txt`, use pip-tools:

    ```sh
    pip install pip-tools
    cd dev/
    pip-compile requirements.txt --output-file requirements-pinned.txt
    ```
