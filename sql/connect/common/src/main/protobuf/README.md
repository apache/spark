# Spark Connect Protobuf Definitions

This directory contains the `.proto` files that define the Spark Connect protocol.

After modifying any `.proto` file here, regenerate the Python stubs under
`python/pyspark/sql/connect/proto/` using one of the two methods below.

---

## Method 1: Docker image (recommended)

This method does not require any local tool installation and produces a
reproducible environment.

### Build the image

```bash
docker build -t connect-cg dev/spark-test-image/connect-gen-protos/
```

### Run the image

From the root of the Spark repository:

```bash
docker run --cpus 1 -it --rm -v "$(pwd)":/spark connect-cg
```

The container mounts the repository at `/spark`, runs `dev/connect-gen-protos.sh`
inside the container, and writes the generated files to
`python/pyspark/sql/connect/proto/` in your local checkout.

---

## Method 2: Local Python environment

### Prerequisites

Install the required tools:

- [`buf`](https://buf.build/docs/cli/installation/) — protobuf code generator
- Python 3.12+

Install the required Python packages. Check `dev/requirements.txt` for the latest
pinned versions of `mypy`, `mypy-protobuf`, and `black`, then run:

```bash
pip install 'mypy==<version>' 'mypy-protobuf==<version>' 'black==<version>'
```

For example, based on the current `dev/requirements.txt`:

```bash
pip install 'mypy==1.19.1' 'mypy-protobuf==3.3.0' 'black==26.3.1'
```

### Generate

From the root of the Spark repository:

```bash
./dev/connect-gen-protos.sh
```

The generated Python files will be written to `python/pyspark/sql/connect/proto/`.

You can also generate to a custom output directory by passing a path:

```bash
./dev/connect-gen-protos.sh /tmp/my-proto-output
```
