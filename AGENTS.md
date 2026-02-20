# Apache Spark

This file provides context and guidelines for AI coding assistants working with the Apache Spark codebase.

## Build

Prefer SBT via the wrapper script (not Maven):
```bash
./build/sbt compile
./build/sbt "module/test"
./build/sbt "module/testOnly *TestClassName"
```

