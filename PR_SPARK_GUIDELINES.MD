
# Apache Spark Contribution & CI Guardrails (for Cursor)

**Goal:**  
When working on `apache/spark` (or a fork), behave like a cautious Spark contributor who:

- Follows Spark + Databricks Scala style guides.
- Always runs relevant local checks **before** pushing.
- Keeps GitHub Actions CI **almost never red** (failures should be rare and explainable, not surprises).

> ⚠️ Reality check:  
> CI can still fail due to flaky tests, infrastructure issues, or new workflows.  
> These rules are about getting as close as practical to “never fail CI,” not a literal 100%.

---

## 0. Scope: When These Rules Apply

Apply **all** of this whenever you (Cursor) are editing code in:

- `https://github.com/apache/spark`
- Any fork of `apache/spark` (for example, `jiteshsoni/spark`)

Treat everything marked as **MANDATORY** below as a **hard pre-push invariant**.

---

## 1. Environment & Tooling Assumptions

Before running any build or tests, assume / ensure:

- **Java**
  - JDK **17** (or 21 if Spark docs say so).
  - `JAVA_HOME` set and on `PATH`.
- **Scala**
  - Managed by Spark’s build; use the wrappers in `./build`.
- **Maven**
  - Use `./build/mvn` (not system `mvn`).
- **SBT**
  - Use `./build/sbt` (not system `sbt`).
- **Python**
  - Supported Python 3.x available as `python` / `python3`.
- **R** (only if touching SparkR)
  - Installed with needed packages.

Recommended JVM options:

```bash
export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"
````

**Fresh jars rule:**
If SQL or core jars exist from previous builds, treat them as **stale**. Rebuild instead of relying on old artifacts.

---

## 2. Contribution Lifecycle (Condensed PR Workflow)

This is the **human** lifecycle that the automation rules support.

### 2.1 JIRA + Idea Validation

1. Search Spark JIRA and mailing lists to avoid duplicates.
2. If non-trivial:

   * Create a **JIRA** (`SPARK-xxxx`) with a clear problem statement and repro (for bugs).
   * Use correct issue type and component.
3. For large changes, consider discussion on `dev@spark.apache.org` first.

### 2.2 Fork, Clone, Branch

```bash
# Clone upstream (optional if you start from fork)
git clone https://github.com/apache/spark.git
cd spark

# Add upstream remote
git remote add upstream https://github.com/apache/spark.git

# Create feature branch
git checkout -b feature/SPARK-xxxx-component
```

Or, more commonly:

* Fork `apache/spark` to your GitHub account.
* Clone your **fork**.
* Add `upstream` pointing to `apache/spark`.

### 2.3 Coding & Local Testing Loop

Typical tight loop:

```bash
# Edit code

# Fast checks while iterating
./dev/scalastyle
./build/mvn -pl sql/core compile
python/run-tests --testnames 'pyspark.sql.tests.streaming.test_something'

# Iterate until green
```

### 2.4 Pre-Push Hardening

Before **any** push that will trigger CI:

1. Rebase on `upstream/master`.
2. Run all **mandatory** pre-push checks (Section 4 + 5).
3. Fix issues, re-run checks.
4. Only then commit + push.

### 2.5 PR Creation & Maintenance

* **Title format:**

  ```text
  [SPARK-xxxx][COMPONENT] Short descriptive title
  ```

* **Description:**

  * Problem → Approach → Tests run.
  * Link to JIRA.

* **After creation:**

  * Enable Actions on your fork if needed.
  * Monitor CI, fix failures promptly.
  * Respond to reviewers within 24–48 hours.
  * Once JIRA is approved, update title & commit messages accordingly.

---

## 3. Non-Negotiable Rules Before Pushing

These are the **core guardrails** that exist specifically to stop CI failures caused by basic issues.

**You must NOT push if any of the following fails.**

1. **Formatting + lint for all touched languages:**

   * `./dev/scalastyle`
   * `./dev/scalafmt`
   * `./dev/lint-scala`
   * `./dev/lint-python` (if `python/` changed)
   * `./dev/reformat-python` (if `python/` changed; **Black is mandatory**)
   * `./dev/lint-java` (if Java changed)
   * `./dev/lint-r` (if R changed)

2. **Baseline build with fresh jars:**

   ```bash
   ./build/mvn -DskipTests clean package
   ```

3. **Dev test harness:**

   ```bash
   ./dev/run-tests
   ```

4. **Area-specific tests** relevant to your changes (Section 5).

**Golden principle:**
CI should confirm what you already know is green, not discover basic problems.

---

## 4. Pre-Push Checklist (Canonical Sequence)

Use this sequence as the **single source of truth** before pushing.

### 4.1 Sync with Upstream

```bash
git fetch upstream
git rebase upstream/master
```

Resolve conflicts and ensure it still builds before proceeding.

### 4.2 Determine What You Touched

Categorize changes to decide which extra tests to run:

* `core/`, `sql/`, `mllib/`, `connector/`, `streaming/`, `resource-managers/` → Scala/Java core.
* `python/` → PySpark.
* `R/` → SparkR.
* `docs/`, `python/docs/` → documentation.
* `.github/`, `dev/`, `build/`, `project/` → infra / CI / build.
* `resource-managers/kubernetes/`, `docker/` → K8s / Docker integration.

### 4.3 Mandatory Lint & Format

Run from repo root:

```bash
# Scala – always if Scala changed
./dev/scalastyle
./dev/scalafmt
./dev/lint-scala

# Java – if Java files touched
./dev/lint-java

# Python – if any python/ changed
./dev/lint-python
./dev/reformat-python   # applies Black, CI requires this

# R – if any R/ changed
./dev/lint-r
```

If any fails → **stop, fix, repeat**.

### 4.4 Baseline Build (Fresh Artifacts)

To avoid stale jars (including SQL jars):

```bash
./build/mvn -DskipTests clean package
```

If this fails → fix and re-run. Do not push.

### 4.5 Global Sanity: Dev Test Harness

```bash
./dev/run-tests
```

This is heavy, but it’s the safest cross-module smoke test.
For small / early iteration changes you might skip it, but **before final push to open a PR or major update, run it.**

---

## 5. Area-Specific Testing Rules

Beyond the global checks, run **focused tests** that match what you touched.

### 5.1 Scala / Java: Core, SQL, Streaming, Connect

**Build is already done (4.4). Now run tests:**

Examples (pick relevant modules):

```bash
# Maven – module-level tests
./build/mvn test -pl :spark-sql_2.13
./build/mvn test -pl :spark-core_2.13

# Maven – targeted Scala suite
./build/mvn -Dtest=none \
  -DwildcardSuites=org.apache.spark.sql.execution.YourSuite \
  test

# SBT – module tests
./build/sbt "core/test"
./build/sbt "sql/test"

# SBT – targeted suite
./build/sbt "sql/testOnly *YourSuite"
```

**If public API changes (core, SQL, MLlib, etc.):**

```bash
dev/mima
```

If MiMa complains, either:

* Restore binary compatibility, or
* Add justified exclusions in `project/MimaExcludes.scala` referring to the JIRA.

### 5.2 PySpark (`python/`)

If anything under `python/` changed:

1. Ensure the build is up-to-date (4.4). For some PySpark tests you may need Hive:

   ```bash
   ./build/mvn -DskipTests clean package -Phive
   ```

2. Run PySpark tests covering your area:

   ```bash
   # New or focused tests
   python/run-tests --testnames 'pyspark.sql.tests.streaming.test_streaming_datasource_admission_control'

   # Module-level
   python/run-tests --modules pyspark-structured-streaming
   python/run-tests --modules pyspark-sql

   # Specific test/module
   python/run-tests --testnames "pyspark.sql.tests.test_arrow ArrowTests"
   ```

If your change is central to PySpark behavior, prefer a broader module run.

### 5.3 SparkR (`R/`)

If anything under `R/` changed:

```bash
./R/run-tests.sh
```

Make sure R + required packages are set up. Do not push if this fails.

### 5.4 Documentation (`docs/`, `python/docs/`)

If you changed docs (especially Python docstrings that feed into Sphinx):

* Build docs in the relevant directory, for example:

  ```bash
  cd python/docs
  make html
  cd -
  ```

Typical failures to watch for:

* Bad `.. versionadded::` / `.. versionchanged::` placement (should usually be in **Notes**, not **See Also**).
* Wrong indentation on directive continuation lines (must align with 4 spaces).
* Malformed numpy-style docstrings.

### 5.5 Kubernetes / Docker

If touching Kubernetes integration or Docker images:

* Follow integration test instructions in the relevant directories.
* At minimum, manually run the same commands that CI uses, where feasible.

---

## 6. Style Rules That Must Be Enforced

### 6.1 Scala (Databricks Scala Style Guide)

At Databricks, our engineers work on some of the most actively developed Scala codebases in the world, including our own internal repo called "universe" as well as the various open source projects we contribute to, e.g. [Apache Spark](https://spark.apache.org) and [Delta Lake](https://delta.io/). This guide draws from our experience coaching and working with our engineering teams as well as the broader open source community.

Code is __written once__ by its author, but __read and modified multiple times__ by lots of other engineers. As most bugs actually come from future modification of the code, we need to optimize our codebase for long-term, global readability and maintainability. The best way to achieve this is to write simple code.

Scala is an incredibly powerful language that is capable of many paradigms. We have found that the following guidelines work well for us on projects with high velocity. Depending on the needs of your team, your mileage might vary.

<a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-nc-sa/4.0/">Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License</a>.


## <a name='TOC'>Table of Contents</a>

1. [Document History](#history)

2. [Syntactic Style](#syntactic)
    * [Naming Convention](#naming)
    * [Variable Naming Convention](#variable-naming)
    * [Line Length](#linelength)
    * [Rule of 30](#rule_of_30)
    * [Spacing and Indentation](#indent)
    * [Blank Lines (Vertical Whitespace)](#blanklines)
    * [Parentheses](#parentheses)
    * [Curly Braces](#curly)
    * [Long Literals](#long_literal)
    * [Documentation Style](#doc)
    * [Ordering within a Class](#ordering_class)
    * [Imports](#imports)
    * [Pattern Matching](#pattern-matching)
    * [Infix Methods](#infix)
    * [Anonymous Methods](#anonymous)

1. [Scala Language Features](#lang)
    * [Case Classes and Immutability](#case_class_immutability)
    * [apply Method](#apply_method)
    * [override Modifier](#override_modifier)
    * [Destructuring Binds](#destruct_bind)
    * [Call by Name](#call_by_name)
    * [Multiple Parameter Lists](#multi-param-list)
    * [Symbolic Methods (Operator Overloading)](#symbolic_methods)
    * [Type Inference](#type_inference)
    * [Return Statements](#return)
    * [Recursion and Tail Recursion](#recursion)
    * [Implicits](#implicits)
    * [Exception Handling (Try vs try)](#exception)
    * [Options](#option)
    * [Monadic Chaining](#chaining)
    * [Symbol Literals](#symbol)

1. [Concurrency](#concurrency)
    * [Scala concurrent.Map](#concurrency-scala-collection)
    * [Explicit Synchronization vs Concurrent Collections](#concurrency-sync-vs-map)
    * [Explicit Synchronization vs Atomic Variables vs @volatile](#concurrency-sync-vs-atomic)
    * [Private Fields](#concurrency-private-this)
    * [Isolation](#concurrency-isolation)

1. [Performance](#perf)
    * [Microbenchmarks](#perf-microbenchmarks)
    * [Traversal and zipWithIndex](#perf-whileloops)
    * [Option and null](#perf-option)
    * [Scala Collection Library](#perf-collection)
    * [private[this]](#perf-private)

1. [Java Interoperability](#java)
    * [Java Features Missing from Scala](#java-missing-features)
    * [Traits and Abstract Classes](#java-traits)
    * [Type Aliases](#java-type-alias)
    * [Default Parameter Values](#java-default-param-values)
    * [Multiple Parameter Lists](#java-multi-param-list)
    * [Varargs](#java-varargs)
    * [Implicits](#java-implicits)
    * [Companion Objects, Static Methods and Fields](#java-companion-object)

1. [Testing](#testing)
    * [Intercepting Exceptions](#testing-intercepting)

1. [Miscellaneous](#misc)
    * [Prefer nanoTime over currentTimeMillis](#misc_currentTimeMillis_vs_nanoTime)
    * [Prefer URI over URL](#misc_uri_url)
    * [Prefer existing well-tested methods over reinventing the wheel](#misc_well_tested_method)



## <a name='history'>Document History</a>
- 2015-03-16: Initial version.
- 2015-05-25: Added [override Modifier](#override_modifier) section.
- 2015-08-23: Downgraded the severity of some rules from "do NOT" to "avoid".
- 2015-11-17: Updated [apply Method](#apply_method) section: apply method in companion object should return the companion class.
- 2015-11-17: This guide has been [translated into Chinese](README-ZH.md). The Chinese translation is contributed by community member [Hawstein](https://github.com/Hawstein). We do not guarantee that it will always be kept up-to-date.
- 2015-12-14:  This guide has been [translated into Korean](README-KO.md). The Korean translation is contributed by [Hyukjin Kwon](https://github.com/HyukjinKwon) and reviewed by [Yun Park](https://github.com/yunpark93), [Kevin (Sangwoo) Kim](https://github.com/swkimme), [Hyunje Jo](https://github.com/RetrieverJo) and [Woochel Choi](https://github.com/socialpercon). We do not guarantee that it will always be kept up-to-date.
- 2016-06-15: Added [Anonymous Methods](#anonymous) section.
- 2016-06-21: Added [Variable Naming Convention](#variable-naming) section.
- 2016-12-24: Added [Case Classes and Immutability](#case_class_immutability) section.
- 2017-02-23: Added [Testing](#testing) section.
- 2017-04-18: Added [Prefer existing well-tested methods over reinventing the wheel](#misc_well_tested_method) section.
- 2019-12-18: Added [Symbol Literals](#symbol) section.
- 2022-08-05: Updated [Monadic Chaining](#chaining) section: do not monadic-chain with an if-else block.

## <a name='syntactic'>Syntactic Style</a>

### <a name='naming'>Naming Convention</a>

We mostly follow Java's and Scala's standard naming conventions.

- Classes, traits, objects should follow Java class convention, i.e. PascalCase style.
  `scala
  class ClusterManager

  trait Expression
  `

- Packages should follow Java package naming conventions, i.e. all-lowercase ASCII letters.
  `scala
  package com.databricks.resourcemanager
  `

- Methods/functions should be named in camelCase style.

- Constants should be all uppercase letters and be put in a companion object.
  `scala
  object Configuration {
    val DEFAULT_PORT = 10000
  }
  `

- An enumeration class or object which extends the `Enumeration` class shall follow the convention for classes and objects, i.e. its name should be in PascalCase style. Enumeration values shall be in the upper case with words separated by the underscore character `_`. For example:
  `scala
    private object ParseState extends Enumeration {
    type ParseState = Value

    val PREFIX,
        TRIM_BEFORE_SIGN,
        SIGN,
        TRIM_BEFORE_VALUE,
        VALUE,
        VALUE_FRACTIONAL_PART,
        TRIM_BEFORE_UNIT,
        UNIT_BEGIN,
        UNIT_SUFFIX,
        UNIT_END = Value
  }
  `

- Annotations should also follow Java convention, i.e. PascalCase. Note that this differs from Scala's official guide.
  `scala
  final class MyAnnotation extends StaticAnnotation
  `


### <a name='variable-naming'>Variable Naming Convention</a>

- Variables should be named in camelCase style, and should have self-evident names.
  `scala
  val serverPort = 1000
  val clientPort = 2000
  `

- It is OK to use one-character variable names in small, localized scope. For example, "i" is commonly used as the loop index for a small loop body (e.g. 10 lines of code). However, do NOT use "l" (as in Larry) as the identifier, because it is difficult to differentiate "l" from "1", "|", and "I".

### <a name='linelength'>Line Length</a>

- Limit lines to 100 characters.
- The only exceptions are import statements and URLs.

### <a name='rule_of_30'>Rule of 30</a>

- A method should not be longer than 30 lines of code (not including blank lines and comments).
- There are some exceptions for this rule, e.g. a method that contains a big `match` expression or a long list of case statements which cannot be broken down further.
- This rule also applies to an anonymous function passed to higher-order functions. If an anonymous function is longer than 30 lines of code, it needs to be extracted out as a private method.
- A class/trait should not be longer than 300 lines of code (not including blank lines and comments).

### <a name='indent'>Spacing and Indentation</a>

- Use 2 spaces for indentation. Do NOT use tabs.
- For control structures, there must be a single space between the keyword and the opening parenthesis.
  `scala
  if (condition) { ... } else { ... }

  while (condition) { ... }
  `
- For method and class definitions, do NOT put a space.
  `scala
  class MyClass(...) { ... }

  def myMethod(...) { ... }
  `
- Put one space before and after an operator.
  `scala
  val x = 1 + 2
  `
- Put one space after "," and ":"
  `scala
  def myMethod(x: Int, y: String): String = { ... }
  `
- Do NOT put space after "(", "[", or before "]", ")".
  `scala
  myMethod(x, y)
  `

### <a name='blanklines'>Blank Lines (Vertical Whitespace)</a>

- A single blank line may appear:
  - Between consecutive members (or initializers) of a class: fields, constructors, methods, nested classes, static initializers, and instance initializers.
    - Exception: A blank line between two consecutive fields (having no other code between them) is optional. Such blank lines are used as needed to create logical groupings of fields.
  - Within method bodies, as needed to create logical groupings of statements.
- Use one blank line, no more and no less. Do NOT use more than one blank line.

### <a name='parentheses'>Parentheses</a>

- It is OK to omit parentheses for nullary methods (i.e. methods that take no arguments and have no side effects). This is a departure from the official [Scala Style Guide](http://docs.scala-lang.org/style/method-invocation.html), but we have been using this convention for a long time in Spark and it has worked well for us. The distinction between `myObj.myMethod` and `myObj.myAttribute` does not seem to justify the verbosity of adding `()` to all method calls.
  `scala
  // This is good
  val s = "my string"
  s.length

  // This is also good and is preferred over s.length()
  s.length
  `
- In method declarations, however, do NOT omit parentheses.
  `scala
  // This is bad
  def myMethod = ...

  // This is good
  def myMethod() = ...
  `
- The reasoning is that the call site should not have to know whether a method is defined with or without parentheses. If we were to change `def myMethod = ...` to `def myMethod() = ...`, all call sites will have to be changed.

### <a name='curly'>Curly Braces</a>

- Curly braces should be used for all control structures, even if the body is a single line. The only exception is for a simple `if-else` that is not a part of a bigger `if-else` chain and that can be put in a single line.
  `scala
  if (condition) {
    action()
  }

  // The following is OK too
  if (condition) action() else anotherAction()
  `
- Opening curly braces must be at the end of a line, not on its own line.
  `scala
  // This is good
  if (condition) {
    ...
  }

  // This is bad
  if (condition)
  {
    ...
  }
  `
- For a class/object/method definition, the opening curly brace must be at the end of the line.
  `scala
  class MyClass extends BaseClass {
    ...
  }
  `
- Closing curly braces must be on its own line.
  `scala
  if (condition) {
    ...
  }
  `

### <a name='long_literal'>Long Literals</a>

- For long literals, use uppercase "L".
  `scala
  val myLong = 1000L
  `
  Lowercase "l" can be easily confused with "1".

### <a name='doc'>Documentation Style</a>

- Use JavaDoc style for documentation.
  `scala
  /**
   * This is a documentation for a class.
   */
  class MyClass {

    /**
     * This is a documentation for a method.
     * @param x the first parameter
     * @param y the second parameter
     * @return something
     */
    def myMethod(x: Int, y: String): String = {
      ...
    }
  }
  `

### <a name='ordering_class'>Ordering within a Class</a>

- If a class contains a companion object, the companion object must be defined immediately after the class.
- Within a class or object, the ordering should be:
  1. Member variables and values
  2. Constructors
  3. Methods

### <a name='imports'>Imports</a>

- Imports must be sorted.
  - The sorting key is: `java`, `javax`, `scala`, everything else, `com.databricks`.
  - Within each group, imports must be sorted lexicographically.
  `scala
  import java.util.HashMap
  import javax.sql.DataSource

  import scala.collection.mutable.ArrayBuffer

  import com.google.common.base.Strings
  import org.apache.spark.SparkConf

  import com.databricks.foo.Bar
  `
- Do NOT use wildcard imports, except for `scala.collection.JavaConverters._` and `org.apache.spark.sql.functions._`.
- Always use absolute paths for imports (e.g. `import com.google.common.base.Strings`), instead of relative ones.
- Never have unused imports.
- Do NOT use curly braces to group imports from the same package.
  `scala
  // This is bad
  import java.util.{HashMap, ArrayList}

  // This is good
  import java.util.HashMap
  import java.util.ArrayList
  `
  This is a departure from the official [Scala Style Guide](http://docs.scala-lang.org/style/imports.html). The reason is that our automated tool can only sort imports line-by-line.

### <a name='pattern-matching'>Pattern Matching</a>

- When a `match` expression is partial, it should always be commented (e.g. if it is known that the default case is impossible).
- The `default` case in a `match` expression should always be the last case, and should use `_` as the identifier.
- Do NOT break a `match` expression into a standalone function, unless the function is also a partial function. The reason is that by making it a standalone function, the compiler can no longer help you check for exhaustiveness.

### <a name='infix'>Infix Methods</a>

- Avoid infix notation for methods, unless the method is a symbolic method (i.e. operator overloading).
  `scala
  // This is bad
  myInstance myMethod 10

  // This is good
  myInstance.myMethod(10)

  // This is good because + is a symbolic method
  1 + 2
  `

### <a name='anonymous'>Anonymous Methods</a>

- When passing anonymous methods to higher-order functions, always use the `_` syntax if the arguments are not explicitly named.
  `scala
  rdd.map(_ + 1)
  `
- Use `_` only when the anonymous method is simple and all parameters are used at most once. For more complex cases, explicitly name the parameters.

## <a name='lang'>Scala Language Features</a>

### <a name='case_class_immutability'>Case Classes and Immutability</a>

- Case classes must be immutable. If you need a mutable data object, use a regular `class`.

### <a name='apply_method'>apply Method</a>

- An `apply` method in a companion object should be used as a factory method. That is, `MyClass(...)` should be equivalent to `new MyClass(...)`. A companion object's apply method should always return an object of its companion class. For example, `object MyClass { def apply() = new MyClass }`. This is because a caller would be very surprised if `MyClass()` returns something of a different type. In very rare cases, if the apply method has to return a different type, it should be very well documented.
- An `apply` method on a class has a different meaning: `myInstance(a, b)` is a shorthand for `myInstance.apply(a, b)`. It should be used when the `apply` operation is the most natural operation for that class. For example, for a DataFrame that represents a table, `df("myColumn")` is a natural shorthand for selecting a column.

### <a name='override_modifier'>override Modifier</a>

- The `override` modifier must be present for any method that overrides a concrete method from a base class. The compiler will check this for you.
- For methods that implement an abstract method from a base class, the `override` modifier is optional. Do NOT put `override` for those methods. The reasoning is that it makes it easier to refactor. For example, if we remove a base trait from a class, we do not need to go and remove all `override` modifiers from that class.

### <a name='destruct_bind'>Destructuring Binds</a>

- Destructuring binds should be used with care. It is a very powerful feature that allows you to extract values from a tuple or a case class.
  `scala
  val (a, b) = myTuple
  val MyCaseClass(c, d) = myCaseClassInstance
  `
- This feature is most useful when you need to extract all fields from an object. If you only need to extract one or two fields, prefer using the dot notation.
- If you use destructuring binds for a case class, and later add a new field to that case class, all your destructuring binds will break. So for case classes that are likely to change, do not use destructuring binds.

### <a name='call_by_name'>Call by Name</a>

- Avoid call-by-name. Use `() => T` instead of `=> T`.
- There are some rare exceptions (e.g. logging, assertions) where call-by-name is the right tool, but in general it should be avoided as it can be confusing. For example, the caller might not know that the expression they pass in is not evaluated immediately.

### <a name='multi-param-list'>Multiple Parameter Lists</a>

- Avoid using multiple parameter lists, unless for enabling a special syntax.
- One exception is for methods that can be used with curly braces for the last argument.
  `scala
  def myMethod(a: Int)(b: String => Int): Int = ...

  // this enables the following syntax
  myMethod(10) { s =>
    ...
  }
  `
  However, this feature should be used sparingly. Most of the time, this can be achieved by making the last argument a function type.

### <a name='symbolic_methods'>Symbolic Methods (Operator Overloading)</a>

- Do NOT define symbolic methods unless they are as intuitive as the arithmetic operators.
- If you do define one, make sure you also define the corresponding text-based method. For example, if you define `+`, also define `add`.

### <a name='type_inference'>Type Inference</a>

- For public methods, the return type must be explicitly declared. The reason is that the method's signature is its contract with the outside world. If we change the implementation, the compiler can help us make sure we do not accidentally change the contract.
  `scala
  // This is good
  def myMethod(x: Int): Int = x + 1

  // This is bad
  def myMethod(x: Int) = x + 1
  `
- For private methods, it is OK to omit the return type.
- For `val` and `var`, it is also OK to omit the type if the right-hand-side expression's type is obvious.
  `scala
  // This is good
  val x = 10

  // This is also good
  val y: Map[String, Int] = ...

  // This is bad
  val z = really.long.expression.that.returns.a.complicated.type
  `
  If the right hand side is a complex expression, explicitly declare the type.

### <a name='return'>Return Statements</a>

- Avoid `return` statements in a function. It is a well-known best practice in Scala. `return` statements are sometimes useful for early exit (e.g. guard clauses), but most of the time can be refactored to an `if-else` expression.
  `scala
  // This is bad
  def myMethod(x: Int): Int = {
    if (x > 0) {
      return x
    }
    0
  }

  // This is good
  def myMethod(x: Int): Int = {
    if (x > 0) x else 0
  }
  `
- The only exception is for exiting a `while` loop early.

### <a name='recursion'>Recursion and Tail Recursion</a>

- Prefer recursion over imperative loops, if the logic is naturally recursive.
- Always use the `@tailrec` annotation for tail-recursive methods. The compiler will help you verify that the method is indeed tail-recursive.
  `scala
  import scala.annotation.tailrec

  @tailrec
  def myMethod(...): Int = { ... }
  `

### <a name='implicits'>Implicits</a>

- Do NOT use implicit conversions. They are very powerful, but also very confusing. It is hard to track where an implicit conversion is defined and which one is in scope.
- Implicit parameters are OK if used in moderation. A good use case is for passing around context information that is obvious to the caller. For example, in Spark, we use implicit parameters to pass around `ClassTag` and `TypeTag` for reifying generic types.
- If a method has an implicit parameter, the implicit parameter list must be the last parameter list.

### <a name='exception'>Exception Handling (Try vs try)</a>

- Prefer `Try` over `try-catch` blocks.
  `scala
  import scala.util.Try

  // This is good
  val result = Try(1 / 0)

  // This is bad
  try {
    1 / 0
  } catch {
    case e: Exception => ...
  }
  `
- `try-finally` blocks are OK. `try-catch` blocks that re-throw the original exception are also OK.

### <a name='option'>Options</a>

- Prefer `Option` over `null`.
- When dealing with Java libraries that can return `null`, wrap the result in an `Option`.
  `scala
  Option(javaMethodThatCanReturnNull)
  `
- Do not use `isDefined` and `get`. Use `map`, `flatMap`, `getOrElse`, or pattern matching instead.
  `scala
  // This is bad
  if (opt.isDefined) {
    doSomething(opt.get)
  }

  // This is good
  opt.foreach(doSomething)

  // This is also good
  opt match {
    case Some(v) => doSomething(v)
    case None => ...
  }
  `

### <a name='chaining'>Monadic Chaining</a>

- Prefer monadic chaining over nested `map`, `flatMap`, `foreach`.
  `scala
  // This is bad
  opt1.flatMap(v1 => opt2.map(v2 => v1 + v2))

  // This is good
  for {
    v1 <- opt1
    v2 <- opt2
  } yield v1 + v2
  `
- Do NOT monadic-chain with an if-else block.
  `scala
  // This is WRONG
  for {
    v1 <- opt1
    v2 <- if (v1.isEmpty) opt2 else opt3
  } yield v2

  // This is CORRECT
  opt1.flatMap { v1 =>
    if (v1.isEmpty) opt2 else opt3
  }
  `

### <a name='symbol'>Symbol Literals</a>

- Do NOT use symbol literals (e.g. `'mySymbol`). They were deprecated in Scala 2.13.

## <a name='concurrency'>Concurrency</a>

### <a name='concurrency-scala-collection'>Scala concurrent.Map</a>

- Avoid using `scala.collection.concurrent.Map` if the map is intended to be used by Java code, due to a potential memory leak in Scala 2.11 ([SI-7943](https://issues.scala-lang.org/browse/SI-7943)). Use `java.util.concurrent.ConcurrentHashMap` instead.

### <a name='concurrency-sync-vs-map'>Explicit Synchronization vs Concurrent Collections</a>

- Prefer using the classes in `java.util.concurrent` over explicit synchronization. For example, use `ConcurrentHashMap` over `HashMap` with `synchronized`.
- The reason is that it is very hard to write correct synchronization code. The concurrent collections are written by experts and are well-tested.

### <a name='concurrency-sync-vs-atomic'>Explicit Synchronization vs Atomic Variables vs @volatile</a>

- For simple counters and pointers, prefer `AtomicInteger` and `AtomicReference` over `synchronized`.
- `volatile` is for memory visibility, not for atomicity. For example, `volatile var counter = 0` does not guarantee that `counter += 1` is atomic. It only guarantees that other threads will see the most up-to-date value of `counter`.

### <a name='concurrency-private-this'>Private Fields</a>

- When a field needs to be protected by a lock, make it `private[this]`.
  `scala
  private[this] var counter = 0

  def increment(): Unit = synchronized {
    counter += 1
  }
  `
- `private[this]` makes the field accessible only within the current object. This is a stronger guarantee than `private`, which allows access from other instances of the same class.

### <a name='concurrency-isolation'>Isolation</a>

- For complex concurrent logic, it is better to isolate it in its own class. The class should have a very clear and well-documented API, and should be thoroughly tested.

## <a name='perf'>Performance</a>

### <a name='perf-microbenchmarks'>Microbenchmarks</a>

- Be very careful with microbenchmarks. The JVM is very good at optimizing code, and it is very easy to write a benchmark that does not measure what you think it measures.
- Use a proper benchmarking tool like [JMH](http://openjdk.java.net/projects/code-tools/jmh/).
- When writing a benchmark, always include a warm-up phase to allow the JIT compiler to kick in.

### <a name='perf-whileloops'>Traversal and zipWithIndex</a>

- For performance-critical code, prefer `while` loops over `for` loops and `foreach`. `for` loops and `foreach` generate anonymous classes and are slower.
- If you need both the element and its index in a loop, avoid `zipWithIndex`. It creates a new collection of tuples. Use a `while` loop instead.
  `scala
  // This is slow
  rdd.zipWithIndex.map { case (v, i) =>
    ...
  }

  // This is faster
  var i = 0
  val iter = rdd.iterator
  while (iter.hasNext) {
    val v = iter.next()
    // do something with v and i
    i += 1
  }
  `

### <a name='perf-option'>Option and null</a>

- In performance-critical code, it is OK to use `null` instead of `Option` to avoid object allocation. However, this should be done with care and should be well-documented.

### <a name='perf-collection'>Scala Collection Library</a>

- The Scala collection library is very powerful, but it can also be slow.
- For performance-critical code, consider using Java collections or primitive arrays.
- Be careful with the complexity of the collection methods. For example, `List.size` is O(N), not O(1).
- Use `view` to avoid creating intermediate collections.

### <a name='perf-private'>private[this]</a>

- Use `private[this]` for fields that are accessed frequently in a hot loop. This can sometimes help the JIT compiler to optimize the code better.

## <a name='java'>Java Interoperability</a>

### <a name='java-missing-features'>Java Features Missing from Scala</a>

- Static fields and methods. See [Companion Objects, Static Methods and Fields](#java-companion-object) for details.
- Annotations on local variables and types.
- `protected` in Scala has a different meaning than in Java. In Scala, `protected` means accessible by subclasses. In Java, it also means accessible by classes in the same package.
- Checked exceptions. Scala does not have checked exceptions. If a Scala method throws a checked exception, it should be documented with the `@throws` annotation.

### <a name='java-traits'>Traits and Abstract Classes</a>

- To create a Java-friendly interface, use an abstract class instead of a trait.
- A trait is compiled into an interface and a companion class. This makes it awkward to use in Java.
- If you need to use a trait, provide a Java-friendly abstract class that extends the trait.

### <a name='java-type-alias'>Type Aliases</a>

- Do NOT use type aliases in public APIs. Type aliases are not visible to Java code.

### <a name='java-default-param-values'>Default Parameter Values</a>

- For a method with default parameter values to be usable in Java, you need to add the `@scala.annotation.varargs` annotation.
- A better way is to use overloading.
  `scala
  // This is NOT Java friendly
  def myMethod(x: Int = 0, y: String = "a") = ...

  // This is Java friendly
  def myMethod(): RDD[T] = myMethod(0, "a")
  def myMethod(x: Int): RDD[T] = myMethod(x, "a")
  def myMethod(x: Int, y: String): RDD[T] = { ... }
  `
- Another way is to use `@JvmOverloads`.
  `scala
  import scala.annotation.meta.getter
  import scala.beans.BeanProperty
  import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyOrder}
  import org.apache.spark.annotation.DeveloperApi

  class MyClass {
    @DeveloperApi
    @BeanProperty
    @JsonProperty("myParam")
    var _myParam: Int = 0

    @JvmOverloads
    def myMethod(x: Int = 0, y: String = "a"): Unit = { ... }
  }
  `
  `@JvmOverloads` will generate overloaded methods for Java.
- For constructors, `@JvmOverloads` is the only way to make default parameter values work for Java.
- However, do not go overboard with `@JvmOverloads`. It generates a lot of bytecode. If you have more than a few parameters with default values, consider using the builder pattern.
- A method from an external library with a default parameter cannot be overridden in Scala with a default parameter.
  `scala
  // From an external library
  class Base {
    def myMethod(x: Int = 0): Unit = { ... }
  }

  // In our code
  class Derived extends Base {
    // This will NOT compile
    override def myMethod(x: Int = 0): Unit = { ... }
  }
  `
  The workaround is to use overloading.
- When overriding a method with default parameters, you must provide the default values again.
- Avoid using default parameters for `val` constructor parameters. This is because the default value is stored in the companion object, which can be inefficient if the default value is a large object.
- Also, avoid mixing default parameters with `varargs`.
  `scala
  // This is bad
  def myMethod(x: Int = 0, y: String*): Unit = { ... }
  `
- If you override a method with default parameters and call it from the base class, the default values from the base class will be used.
  `scala
  class Base {
    def myMethod(x: Int = 0): Unit = { ... }
    def callMyMethod(): Unit = myMethod()
  }

  class Derived extends Base {
    override def myMethod(x: Int = 1): Unit = { ... }
  }

  // This will call myMethod with x = 0, not 1.
  new Derived().callMyMethod()
  `
- Do not use named arguments with default parameters if the method is intended to be called from Java. Java does not support named arguments.

### <a name='java-default-param-values'>Default Parameter Values (Spark-specific)</a>

(The following section is specific to Spark)
- For Spark public APIs, prefer overloading over default parameters. The reason is that Java, Python, and R bindings all work better with overloading.
  `scala
  def sample(ratio: Double, withReplacement: Boolean): RDD[T] = { ... }
  def sample(ratio: Double): RDD[T] = sample(ratio, withReplacement = false)
  `

### <a name='java-multi-param-list'>Multiple Parameter Lists</a>

Do NOT use multi-parameter lists.

### <a name='java-varargs'>Varargs</a>

- Apply `@scala.annotation.varargs` annotation for a vararg method to be usable in Java. The Scala compiler creates two methods, one for Scala (bytecode parameter is a Seq) and one for Java (bytecode parameter array).
  `scala
  @scala.annotation.varargs
  def select(exprs: Expression*): DataFrame = { ... }
  `

- Note that abstract vararg methods does NOT work for Java, due to a Scala compiler bug ([SI-1459](https://issues.scala-lang.org/browse/SI-1459), [SI-9013](https://issues.scala-lang.org/browse/SI-9013)).

- Be careful with overloading varargs methods. Overloading a vararg method with another vararg type can break source compatibility.
  `scala
  class Database {
    @scala.annotation.varargs
    def remove(elems: String*): Unit = ...

    // Adding this will break source compatibility for no-arg remove() call.
    @scala.annotation.varargs
    def remove(elems: People*): Unit = ...
  }

  // This won't compile anymore because it is ambiguous
  new Database().remove()
  `
  Instead, define an explicit first parameter followed by vararg:
  `scala
  class Database {
    @scala.annotation.varargs
    def remove(elems: String*): Unit = ...

    // The following is OK.
    @scala.annotation.varargs
    def remove(elem: People, elems: People*): Unit = ...
  }
  `


### <a name='java-implicits'>Implicits</a>

Do NOT use implicits, for a class or method. This includes `ClassTag`, `TypeTag`.
`scala
class JavaFriendlyAPI {
  // This is NOT Java friendly, since the method contains an implicit parameter (ClassTag).
  def convertTo[T: ClassTag](): T
}
`

### <a name='java-companion-object'>Companion Objects, Static Methods and Fields</a>

There are a few things to watch out for when it comes to companion objects and static methods/fields.

- Companion objects are awkward to use in Java (a companion object `Foo` is a static field `MODULE$` of type `Foo$` in class `Foo$`).
  `scala
  object Foo

  // equivalent to the following Java code
  public class Foo$ {
    Foo$ MODULE$ = // instantiation of the object
  }
  `
  If the companion object is important to use, create a Java static field in a separate class.

- Unfortunately, there is no way to define a JVM static field in Scala. Create a Java file to define that.
- Methods in companion objects are automatically turned into static methods in the companion class, unless there is a method name conflict. The best (and future-proof) way to guarantee the generation of static methods is to add a test file written in Java that calls the static method.
  `scala
  class Foo {
    def method2(): Unit = { ... }
  }

  object Foo {
    def method1(): Unit = { ... }  // a static method Foo.method1 is created in bytecode
    def method2(): Unit = { ... }  // a static method Foo.method2 is NOT created in bytecode
  }

  // FooJavaTest.java (in test/scala/com/databricks/...)
  public class FooJavaTest {
    public static void compileTest() {
      Foo.method1();  // This one should compile fine
      Foo.method2();  // This one should fail because method2 is not generated.
    }
  }
  `

- A case object (or even just plain companion object) MyClass is actually not of type MyClass.
  `scala
  case object MyClass

  // Test.java
  if (MyClass$.MODULE instanceof MyClass) {
    // The above condition is always false
  }
  `
  To implement the proper type hierarchy, define a companion class, and then extend that in case object:
  `scala
  class MyClass
  case object MyClass extends MyClass
  `

## <a name='testing'>Testing</a>

### <a name='testing-intercepting'>Intercepting Exceptions</a>

When testing that performing a certain action (say, calling a function with an invalid argument) throws an exception, be as specific as possible about the type of exception you expect to be thrown. You should NOT simply `intercept[Exception]` or `intercept[Throwable]` (to use the ScalaTest syntax), as this will just assert that _any_ exception is thrown. Often times, this will just catch errors you made when setting up your testing mocks and your test will silently pass without actually checking the behavior you want to verify.

  `scala
  // This is WRONG
  intercept[Exception] {
    thingThatThrowsException()
  }

  // This is CORRECT
  intercept[MySpecificTypeOfException] {
    thingThatThrowsException()
  }
  `

If you cannot be more specific about the type of exception that the code will throw, that is often a sign of code smell. You should either test at a lower level or modify the underlying code to throw a more specific exception.

## <a name='misc'>Miscellaneous</a>

### <a name='misc_currentTimeMillis_vs_nanoTime'>Prefer nanoTime over currentTimeMillis</a>

When computing a *duration* or checking for a *timeout*, avoid using `System.currentTimeMillis()`. Use `System.nanoTime()` instead, even if you are not interested in sub-millisecond precision.

`System.currentTimeMillis()` returns current wallclock time and will follow changes to the system clock. Thus, negative wallclock adjustments can cause timeouts to "hang" for a long time (until wallclock time has caught up to its previous value again).  This can happen when ntpd does a "step" after the network has been disconnected for some time. The most canonical example is during system bootup when DHCP takes longer than usual. This can lead to failures that are really hard to understand/reproduce. `System.nanoTime()` is guaranteed to be monotonically increasing irrespective of wallclock changes.

Caveats:
- Never serialize an absolute `nanoTime()` value or pass it to another system. The absolute value is meaningless and system-specific and resets when the system reboots.
- The absolute `nanoTime()` value is not guaranteed to be positive (but `t2 - t1` is guaranteed to yield the right result)
- `nanoTime()` rolls over every 292 years. So if your Spark job is going to take a really long time, you may need something else :)


### <a name='misc_uri_url'>Prefer URI over URL</a>

When storing the URL of a service, you should use the `URI` representation.

The [equality check](http://docs.oracle.com/javase/7/docs/api/java/net/URL.html#equals(java.lang.Object)) of `URL` actually performs a (blocking) network call to resolve the IP address. The `URI` class performs field equality and is a superset of `URL` as to what it can represent.

### <a name='misc_well_tested_method'>Prefer existing well-tested methods over reinventing the wheel</a>

When there is an existing well-tesed method and it doesn't cause any performance issue, prefer to use it. Reimplementing such method may introduce bugs and requires spending time testing it (maybe we don't even remember to test it!).

  `scala
  val beginNs = System.nanoTime()
  // Do something
  Thread.sleep(1000)
  val elapsedNs = System.nanoTime() - beginNs

  // This is WRONG. It uses magic numbers and is pretty easy to make mistakes
  val elapsedMs = elapsedNs / 1000 / 1000

  // Use the Java TimeUnit API. This is CORRECT
  import java.util.concurrent.TimeUnit
  val elapsedMs2 = TimeUnit.NANOSECONDS.toMillis(elapsedNs)

  // Use the Scala Duration API. This is CORRECT
  import scala.concurrent.duration._
  val elapsedMs3 = elapsedNs.nanos.toMillis
  `

Exceptions:
- Using an existing well-tesed method requires adding a new dependency. If such method is pretty simple, reimplementing it is better than adding a dependency. But remember to test it.
- The existing method is not optimized for our usage and is too slow. But benchmark it first, avoid premature optimization.

### 6.2 Python (PySpark)

* Black formatting is **mandatory** (CI enforces it).
* Style is roughly PEP 8 + Spark conventions (lines ~100 chars).
* No unexpected side effects at module import time.
* Keep Python APIs aligned with corresponding Scala/Java APIs where applicable.

Commands:

```bash
./dev/lint-python
./dev/reformat-python
```

Common Python pitfalls:

* Black “would reformat X files” → fixed by `./dev/reformat-python`.
* Sphinx / numpy docstring errors → fix docstrings and rebuild docs.
* Wrong indentation on `.. versionadded::` / `.. versionchanged::` continuation lines (must be 4 spaces).

### 6.3 Java

* Follow standard Java conventions.
* Similar line-length and readability expectations as Scala.
* Use `./dev/lint-java` to enforce.

### 6.4 R

* Use Google R style plus Spark norms.
* Enforce via:

  ```bash
  ./dev/lint-r
  ./R/run-tests.sh
  ```

---

## 7. Optional: Git Pre-Push Hook (Automation Wrapper)

You can wrap the rules above into a **local pre-push hook** so pushing is impossible unless checks pass.

Create `.git/hooks/pre-push`:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "[pre-push] Running Spark pre-push guardrails..."

# Basic environment sanity
if ! command -v java >/dev/null 2>&1; then
  echo "[pre-push] ERROR: 'java' not found on PATH."
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[pre-push] ERROR: 'python3' not found on PATH."
  exit 1
fi

# 1) Lint & format (Scala + Python if present)
echo "[pre-push] Scala style checks..."
./dev/scalastyle
./dev/scalafmt
./dev/lint-scala

if git diff --name-only | grep -q '^python/'; then
  echo "[pre-push] Python style checks..."
  ./dev/lint-python
  ./dev/reformat-python
fi

# 2) Fresh build without tests
echo "[pre-push] Building Spark (no tests)..."
./build/mvn -DskipTests clean package

# 3) Dev run tests (global sanity)
echo "[pre-push] Running dev test harness..."
./dev/run-tests

echo "[pre-push] ✅ All checks passed. Push allowed."
```

Make it executable:

```bash
chmod +x .git/hooks/pre-push
```

> Cursor can conceptually “simulate” this workflow by ensuring it never suggests `git push` until the equivalent commands have run cleanly.

---

## 8. CI & GitHub Actions Strategy

### 8.1 Why Run CI in the Fork

To avoid hammering `apache/spark` CI and to debug efficiently:

1. In your fork, enable:

   * “Build and test”
   * “Report test results”
2. After local checks pass, push to your branch in your fork.
3. Let CI run.
4. Only open or update a PR to `apache/spark` when your fork’s CI is green.

### 8.2 Handling CI Failures

If CI fails despite passing locally:

1. Inspect the failing job logs carefully.
2. Map failures back to local commands:

   * Failing unit test → run that suite locally.
   * Sphinx error → run docs build locally.
3. Fix root cause, re-run local checks, then push again.

Treat each red CI as a **bug or gap in your process**, not as random noise.

---

## 9. Lessons Learned & Why These Rules Exist

This section is explicitly answering **“why did we choose these rules and not others?”**
It’s based heavily on real issues from the admission-control PR (`SPARK-54305` / `PR #53001`).

### 9.1 What We Saw in Practice

From that PR and others:

* Skipping `./dev/scalastyle` caused:

  * Long-line violations (>100 chars).
  * Trailing whitespace errors.
  * Multiple failed CI jobs on different JDKs.
* Skipping `./dev/lint-python` / `./dev/reformat-python` caused:

  * Black formatting failures.
  * Sphinx docstring parsing errors.
  * Multiple 40-minute CI cycles for tiny fixes.
* Not running broader tests led to:

  * Surprises where unrelated modules broke.
  * Fragile integration with doc builds and structured streaming tests.

**Observation:**
Almost all of these failures would have been caught in **2–5 minutes** locally with the commands we’ve made mandatory.

### 9.2 What We Chose to Keep (and Why)

We **kept**:

* **Global guardrails**:

  * `./dev/scalastyle`, `./dev/scalafmt`, `./dev/lint-scala`
  * `./dev/lint-python`, `./dev/reformat-python`
  * `./dev/lint-java`, `./dev/lint-r` where relevant
  * `./build/mvn -DskipTests clean package`
  * `./dev/run-tests`
* **Area-specific tests**:

  * Maven/SBT module tests for Scala/Java.
  * `python/run-tests` for PySpark (by testnames or modules).
  * `R/run-tests.sh` for SparkR.
  * Docs builds for Sphinx-backed content.
* **Databricks Scala Style Guide** as the canonical style reference.
* **Black** as mandatory for Python code.
* A **single pre-push hook** example as the automation wrapper.

Reasoning:

* These capture **most CI failures** we’ve observed (style, formatting, compilation, basic tests).
* They are scriptable and safe for Cursor to treat as invariants.
* They generalize beyond a single PR and apply to almost any Spark contribution.

### 9.3 What We Deliberately Removed / Compressed

From your long PR guide, we **removed or compressed**:

* **PR-specific details**:

  * Exact PR number, status tables, “congratulations” sections.
  * The full technical deep-dive into Python DataSource Admission Control design.
  * Very detailed block-by-block example outputs of the blockchain demo.
* **Highly specific integration steps**:

  * Hard-coded local paths like `/Users/jitesh.soni/spark`.
  * One-off step-by-steps that are only relevant for that single feature.

Reasoning:

* These are **not reusable guardrails** for future work and would just add noise.
* Cursor doesn’t need emotional context or celebration; it needs **rules**.
* We kept just enough of the admission-control story in **Lessons Learned** to justify the strict pre-push checklist without cluttering the main flow.

### 9.4 Why There’s Still Some Narrative

We kept a **lightweight** lifecycle and “lessons learned” section because:

* Cursor may be used to generate PR descriptions, comments, or explanations.
* You benefit from having the “why” documented once, so future you remembers why these rules are strict (e.g., the 40-minute CI failures).
* Some human-facing context (JIRA, PR naming, review etiquette) is necessary for a complete contributor workflow.

---

## 10. Quick Reference: Behavior Summary for Cursor

When operating in a Spark repo:

1. **Assume** proper JDK, Python, and build tooling are available.
2. **Never** suggest `git push` before:

   * `./dev/scalastyle`
   * `./dev/scalafmt`
   * `./dev/lint-scala`
   * `./dev/lint-python` + `./dev/reformat-python` (if `python/` touched)
   * `./dev/lint-java` / `./dev/lint-r` as needed
   * `./build/mvn -DskipTests clean package`
   * `./dev/run-tests`
   * And relevant area-specific tests.
3. **If any command fails**,:

   * Read the error.
   * Propose code changes to fix it.
   * Re-run until all are green.
4. Only then:

   * Stage + commit changes.
   * Push to the fork.
   * Let CI run and help interpret failures if any.

If there’s a trade-off between **fast** and **safe**, default to **safe**.

---

```
::contentReference[oaicite:0]{index=0}
```
