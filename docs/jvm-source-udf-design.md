# Inline JVM UDF Source in PySpark

## Status

Draft proposal.

## Summary

Add a PySpark API that accepts Java or Scala source code, compiles it on the Spark driver or
Spark Connect server, distributes the compiled classes to executors, and registers the class as
a scalar SQL UDF.

The new API handles source compilation only. After compilation, it reuses the existing artifact
manager and `UDFRegistration.registerJava` path.

## Motivation

`spark.udf.registerJavaFunction` currently accepts a fully qualified class name. Users must compile
their Java or Scala code into a JAR, place that JAR on Spark's class path, and then register the
class. This is inconvenient for interactive PySpark use, especially with Spark Connect.

The proposed API lets users define a small JVM UDF in the same Python application while preserving
JVM execution. It avoids the Python worker overhead of a Python UDF, although the resulting JVM UDF
remains opaque to Catalyst optimization.

## Goals

- Accept complete Java or Scala source for a scalar UDF from PySpark.
- Support Spark Classic and Spark Connect with the same public API.
- Compile source once per session rather than once per task or executor.
- Reuse the existing JVM UDF validation, type inference, registration, and execution paths.
- Make compiled classes available through Spark's session artifact class loader.
- Return useful compiler diagnostics containing source line and column information.

## Non-goals

- Defining a new expression language for method bodies or lambda snippets.
- Supporting Java or Scala aggregate UDFs in the first version.
- Sandboxing untrusted JVM code.
- Persisting source UDFs in an external catalog.
- Replacing built-in SQL functions or Catalyst expressions.

## Proposed API

Add the following method to PySpark's `UDFRegistration`:

```python
def registerJvmFunctionFromSource(
    self,
    name: str,
    className: str,
    source: str,
    returnType: Optional[DataTypeOrString] = None,
    *,
    language: str = "java",
) -> None:
    ...
```

`language` accepts `"java"` or `"scala"`. A separate method is preferable to overloading
`registerJavaFunction` because the existing method has stable class-loading semantics and its
second positional argument is already a class name.

The source must declare `className`, implement exactly one of
`org.apache.spark.sql.api.java.UDF0` through `UDF22`, and provide a public no-argument constructor.
Scala source uses the same Java UDF interfaces, allowing the existing registration path to handle
both languages.

### Java example

```python
java_source = """
package example;

import org.apache.spark.sql.api.java.UDF1;

public final class PlusOne implements UDF1<Long, Long> {
  @Override
  public Long call(Long value) {
    return value == null ? null : value + 1;
  }
}
"""

spark.udf.registerJvmFunctionFromSource(
    name="plus_one",
    className="example.PlusOne",
    source=java_source,
    returnType="long",
)
```

### Scala example

```python
scala_source = """
package example

import org.apache.spark.sql.api.java.UDF1

final class StringLength extends UDF1[String, java.lang.Integer] {
  override def call(value: String): java.lang.Integer = {
    if (value == null) null else value.length
  }
}
"""

spark.udf.registerJvmFunctionFromSource(
    name="string_length",
    className="example.StringLength",
    source=scala_source,
    language="scala",
)
```

When `returnType` is not specified, the existing Java reflection logic infers it from the selected
`UDFn` interface.

## Design

The source is compiled on the JVM that owns the Spark session. Client-side compilation is avoided
because a Spark Connect Python client may not have a JDK, a Scala compiler, Spark JARs, or the same
dependencies as the server.

```text
PySpark API
  -> Py4J call or Spark Connect command
  -> session JVM source compiler
  -> content-addressed JAR in the session artifact manager
  -> existing UDFRegistration.registerJava
  -> existing JVM UDF execution on executors
```

### Compilation

Introduce an internal source compiler service in SQL core. It accepts the language, source,
declared class name, and the session artifact class loader. It returns all generated class files.

For Java, use `javax.tools.JavaCompiler` with an in-memory file manager. Disable annotation
processing with `-proc:none`. The existing Catalyst `JdkCodeCompiler` contains useful in-memory
compiler and class-loader-aware file-manager code, but its public shape is specific to generated
Catalyst classes. Common compiler plumbing may be extracted rather than making the Catalyst
code-generation API handle arbitrary user classes.

For Scala, use the Scala compiler version with which Spark was built. Scala support must fail with
a structured error when the matching compiler is unavailable. All emitted classes, including
module, anonymous, and nested classes, must be packaged together.

The compiler resolves dependencies through the session artifact class loader. Users must add
dependency JARs before registering the source UDF.

### Artifact distribution

Package generated classes into a JAR named from a SHA-256 digest of the language, declared class
name, source, Spark version, and Scala version when applicable. Add that JAR through the session
artifact manager. This makes the class visible to the driver and distributes it to executors using
the existing job artifact state.

Compilation is idempotent within a session. Re-registering the same class and source reuses the
artifact. Registering different source with an already compiled class name fails because JVM class
redefinition and multiple JARs containing the same class have ambiguous behavior. Users can choose
a new class name or create a new session.

After adding the artifact, call the existing `registerJava(name, className, returnType)` method.
That method remains responsible for loading the class, validating its `UDFn` interface, creating an
instance, inferring the return type, and registering the temporary function.

### Spark Classic

PySpark calls an internal method on the classic JVM `UDFRegistration` through Py4J. The JVM method
compiles, adds the generated artifact, and delegates to `registerJava`.

### Spark Connect

Add a source JVM UDF message to the `register_function` command containing:

- Language.
- Fully qualified class name.
- Source text.
- Optional output data type.

The Connect planner invokes the same compiler service and registration logic used by Spark
Classic. Keeping compilation on the server makes behavior independent of the Python client
environment. An older server that does not support the new command returns the standard unsupported
feature error.

## Errors

Add structured errors for:

- Unsupported source language.
- Source compilation disabled by server policy.
- Requested compiler unavailable.
- Compilation failure, including compiler diagnostics.
- Declared class not generated by the source.
- Class not implementing exactly one supported `UDFn` interface.
- Class name already associated with different source in the session.

Source text should not be included in error parameters or ordinary logs. Compiler diagnostics may
include a bounded source excerpt, controlled by the existing code-generation logging limit or a
similar dedicated setting.

## Security and resource controls

The resulting UDF is arbitrary JVM code. The API must use the same authorization policy as adding
and executing JAR artifacts. Deployments that do not allow users to execute JVM artifacts must be
able to disable source compilation.

The implementation should also:

- Limit source size and compiler diagnostic size.
- Disable Java annotation processing.
- Bound concurrent compilations.
- Clean generated files when the session closes.
- Avoid placing source text in event logs or query plans.

Compilation does not provide a security sandbox. A server must not enable this API for users who
are not already trusted to execute JVM or Python UDF code.

## Compatibility

The existing `registerJavaFunction` and `registerJavaUDAF` APIs are unchanged. Source UDFs are
temporary session functions and use the same runtime semantics as an equivalent precompiled Java
UDF.

The Connect protocol change is additive. Python clients should only send the new command when the
new API is called.

## Test plan

- PySpark Classic tests for Java and Scala source UDFs.
- Spark Connect end-to-end tests for both languages.
- Explicit and inferred return types.
- Null inputs and primitive or boxed results.
- Multiple generated class files from one source unit.
- Dependency resolution from a previously added session JAR.
- Execution on multiple executor JVMs, not only local driver execution.
- Idempotent registration and conflicting source for the same class name.
- Compiler unavailable, compilation disabled, invalid source, and invalid UDF class errors.
- Source and diagnostic size limits.

## Rollout

Implement Java source first because the JDK compiler has a stable API and produces predictable
class files. Add Scala source after validating compiler packaging and Spark distribution size. The
public API includes the language argument from the start so Scala can be added without another API
shape change.
