# Spark Configuration Module

This module provides a proto-based configuration system for defining Spark configurations. Configurations are defined using Protocol Buffers text format (`.textproto` files) and loaded at runtime by `ConfigRegistry`.

## Directory Structure

```
common/config/src/main/
├── java/org/apache/spark/config/
│   └── ConfigRegistry.java          # Loads and queries configs
├── protobuf/org/apache/spark/config/
│   └── config_schema.proto          # Defines the config schema
└── resources/org/apache/spark/config/
    ├── cluster_configs/             # CLUSTER scope configs
    │   └── sql.textproto
    └── session_configs/             # SESSION scope configs
        └── sql.textproto
```

## How to Define a New Config

### Step 1: Choose the Right File

- **CLUSTER scope**: If the config applies to the entire Spark cluster, add to a file in `cluster_configs/` directory.
- **SESSION scope**: If the config applies to a Spark session, add to a file in `session_configs/` directory.

Currently, SQL configs go in `sql.textproto`. New categories can have their own files.

### Step 2: Add the Config Entry

Add a new `configs` block to the appropriate `.textproto` file:

```protobuf
configs {
  key: "spark.sql.myFeature.enabled"
  value_type: BOOL
  default_value: "true"
  scope: SESSION
  visibility: PUBLIC
  doc: "When true, enables my new feature."
  version: "4.0.0"
}
```

**Notes**:
- See `config_schema.proto` for field details and requirements
- Configs within each file must be ordered alphabetically by key
- These constraints are validated at load time

### Step 3: Register the Config File (if new)

If you created a new `.textproto` file, add it to `ConfigRegistry.java`:

```java
private static final String[] DEFAULT_CONFIG_FILES = {
  "org/apache/spark/config/cluster_configs/sql.textproto",
  "org/apache/spark/config/session_configs/sql.textproto",
  "org/apache/spark/config/session_configs/your_new_file.textproto"  // Add here
};
```

## Multi-line Documentation

For long documentation strings, use protobuf string concatenation:

```protobuf
configs {
  key: "spark.sql.myFeature.threshold"
  value_type: INT
  default_value: "100"
  scope: SESSION
  visibility: PUBLIC
  doc: "This is a long documentation string that spans multiple lines. "
       "Simply place multiple quoted strings adjacent to each other "
       "and protobuf will concatenate them automatically."
  version: "4.0.0"
}
```

## Using Configs in Spark

### Reading Config Values

For configs accessed in only one or a few places, use `getConfByKeyStrict` with the config key directly. This will fail at runtime if the key is not defined in a `.textproto` file:

```scala
def myFunc(conf: SQLConf): Unit = {
  if (conf.getConfByKeyStrict[Boolean]("spark.sql.myFeature.enabled")) {
    doSomething()
  }
}
```

For configs accessed in many places, create a `ConfigEntry` in `object SQLConf` (or its friends) to avoid hardcoding keys:

```scala
object SQLConf {
  val MY_FEATURE_ENABLED = buildConfFromConfigFile[Boolean]("spark.sql.myFeature.enabled")
}

def myFunc(conf: SQLConf): Unit = {
  if (conf.getConf(SQLConf.MY_FEATURE_ENABLED)) {
    doSomething()
  }
}
```

### Adding Config Value Validation

For configs that need to validate its value, create a `ConfigEntry` and use `checkValue`:

```scala
val MY_THRESHOLD = buildConfFromConfigFile[Int]("spark.sql.myFeature.threshold")
  .checkValue(_ > 0, "Threshold must be positive")
```

