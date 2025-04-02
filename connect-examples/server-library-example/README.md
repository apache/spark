# Spark Server Library Example - Custom Datasource Handler

This example demonstrates a modular maven-based project architecture with separate client, server 
and common components. It leverages the extensibility of Spark Connect to create a server library 
that may be attached to the server to extend the functionality of the Spark Connect server as a whole. Below is a detailed overview of the setup and functionality.

## Project Structure

```
├── common/                # Shared protobuf/utilities/classes
├── client/                # Sample client implementation 
│   ├── src/               # Source code for client functionality
│   ├── pom.xml            # Maven configuration for the client
├── server/                # Server-side plugin extension
│   ├── src/               # Source code for server functionality
│   ├── pom.xml            # Maven configuration for the server
├── resources/             # Static resources
├── pom.xml                # Parent Maven configuration
```

## Functionality Overview

To demonstrate the extensibility of Spark Connect, a custom datasource handler, `CustomTable` is 
implemented in the server module. The class handles reading, writing and processing data stored in
a custom format, here we simply use the `.custom` extension (which itself is a wrapper over `.csv`
files).

First and foremost, the client and the server must be able to communicate with each other through
custom messages that 'understand' our custom data format. This is achieved by defining custom
protobuf messages in the `common` module. The client and server modules both depend on the `common`
module to access these messages.
- `common/src/main/protobuf/base.proto`: Defines the base `CustomTable` which is simply represented
by a path and a name.
```protobuf
message CustomTable {
  string path = 1;
  string name = 2;
}
```
- `common/src/main/protobuf/commands.proto`: Defines the custom commands that the client can send
to the server. These commands are typically operations that the server can perform, such as cloning
an existing custom table.
```protobuf
message CustomCommand {
  oneof command_type {
    CreateTable create_table = 1;
    CloneTable clone_table = 2;
  }
}
```
- `common/src/main/protobuf/relations.proto`: Defines custom `relations`, which are a mechanism through which an optional input dataset is transformed into an
  output dataset such as a Scan.
```protobuf
message Scan {
  CustomTable table = 1;
}
```

On the client side, the `CustomTable` class mimics the style of Spark's `Dataset` API, allowing the
user to perform and chain operations on a `CustomTable` object.

On the server side, a similar `CustomTable` class is implemented to handle the core functionality of
reading, writing and processing data in the custom format. The plugins (`CustomCommandPlugin` and
`CustomRelationPlugin`) are responsible for processing the custom protobuf messages sent from the client
(those defined in the `common` module) and delegating the appropriate actions to the `CustomTable`.



## Build and Run Instructions

1. **Navigate to the sample project from `SPARK_HOME`**:
   ```bash
   cd connect-examples/server-library-example
   ```

2. **Build and package the modules**:
   ```bash
   mvn clean package
   ```

3. **Download the `4.0.0-preview2` release to use as the Spark Connect Server**:
   - Choose a distribution from https://archive.apache.org/dist/spark/spark-4.0.0-preview2/.
   - Example: `curl -L https://archive.apache.org/dist/spark/spark-4.0.0-preview2/spark-4.0.0-preview2-bin-hadoop3.tgz | tar xz`

4. **Copy relevant JARs to the root of the unpacked Spark distribution**:
   ```bash
    cp \
    <SPARK_HOME>/connect-examples/server-library-example/common/target/spark-daria_2.13-1.2.3.jar \
    <SPARK_HOME>/connect-examples/server-library-example/common/target/spark-server-library-example-common-1.0.0.jar \
    <SPARK_HOME>/connect-examples/server-library-example/server/target/spark-server-library-example-server-extension-1.0.0.jar \
    .
   ```
5. **Start the Spark Connect Server with the relevant JARs**:
   ```bash
    bin/spark-connect-shell \
   --jars spark-server-library-example-server-extension-1.0.0.jar,spark-server-library-example-common-1.0.0.jar,spark-daria_2.13-1.2.3.jar \
   --conf spark.connect.extensions.relation.classes=org.apache.connect.examples.serverlibrary.CustomRelationPlugin \
   --conf spark.connect.extensions.command.classes=org.apache.connect.examples.serverlibrary.CustomCommandPlugin
   ```
6. **In a different terminal, navigate back to the root of the sample project and start the client**:
   ```bash
   java -cp client/target/spark-server-library-client-package-scala-1.0.0.jar org.apache.connect.examples.serverlibrary.CustomTableExample
   ```
7. **Notice the printed output in the client terminal as well as the creation of the cloned table**:
```protobuf
Explaining plan for custom table: sample_table with path: <SPARK_HOME>/spark/connect-examples/server-library-example/client/../resources/dummy_data.custom
== Parsed Logical Plan ==
Relation [id#2,name#3] csv

== Analyzed Logical Plan ==
id: int, name: string
Relation [id#2,name#3] csv

== Optimized Logical Plan ==
Relation [id#2,name#3] csv

== Physical Plan ==
FileScan csv [id#2,name#3] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/venkata.gudesa/spark/connect-examples/server-library-example/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:int,name:string>

Explaining plan for custom table: cloned_table with path: <SPARK_HOME>/connect-examples/server-library-example/client/../resources/cloned_data.data
== Parsed Logical Plan ==
Relation [id#2,name#3] csv

== Analyzed Logical Plan ==
id: int, name: string
Relation [id#2,name#3] csv

== Optimized Logical Plan ==
Relation [id#2,name#3] csv

== Physical Plan ==
FileScan csv [id#2,name#3] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/Users/venkata.gudesa/spark/connect-examples/server-library-example/resou..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<id:int,name:string>
```