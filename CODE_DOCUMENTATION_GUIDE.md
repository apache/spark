# Code Documentation Guide

This guide describes documentation standards for Apache Spark source code.

## Overview

Good documentation helps developers understand and maintain code. Spark follows industry-standard documentation practices for each language it supports.

## Scala Documentation (Scaladoc)

Scala code uses Scaladoc for API documentation.

### Basic Format

```scala
/**
 * Brief one-line description.
 *
 * Detailed description that can span multiple lines.
 * Explain what this class/method does, important behavior,
 * and any constraints or assumptions.
 *
 * @param paramName description of parameter
 * @param anotherParam description of another parameter
 * @return description of return value
 * @throws ExceptionType when this exception is thrown
 * @since 3.5.0
 * @note Important note about usage or behavior
 */
def methodName(paramName: String, anotherParam: Int): ReturnType = {
  // Implementation
}
```

### Class Documentation

```scala
/**
 * Brief description of the class purpose.
 *
 * Detailed explanation of the class functionality, usage patterns,
 * and important considerations.
 *
 * Example usage:
 * {{{
 * val example = new MyClass(param1, param2)
 * example.doSomething()
 * }}}
 *
 * @constructor Creates a new instance with the given parameters
 * @param config Configuration object
 * @param isLocal Whether running in local mode
 * @since 3.0.0
 */
class MyClass(config: SparkConf, isLocal: Boolean) extends Logging {
  // Class implementation
}
```

### Code Examples

Use triple braces for code examples:

```scala
/**
 * Transforms the RDD by applying a function to each element.
 *
 * Example:
 * {{{
 * val rdd = sc.parallelize(1 to 10)
 * val doubled = rdd.map(_ * 2)
 * doubled.collect() // Array(2, 4, 6, ..., 20)
 * }}}
 *
 * @param f function to apply to each element
 * @return transformed RDD
 */
def map[U: ClassTag](f: T => U): RDD[U]
```

### Annotations

Use Spark annotations for API stability:

```scala
/**
 * :: Experimental ::
 * This feature is experimental and may change in future releases.
 */
@Experimental
class ExperimentalFeature

/**
 * :: DeveloperApi ::
 * This is a developer API and may change between minor versions.
 */
@DeveloperApi
class DeveloperFeature

/**
 * :: Unstable ::
 * This API is unstable and may change in patch releases.
 */
@Unstable
class UnstableFeature
```

### Internal APIs

Mark internal classes and methods:

```scala
/**
 * Internal utility class for XYZ.
 *
 * @note This is an internal API and may change without notice.
 */
private[spark] class InternalUtil

/**
 * Internal method used by scheduler.
 */
private[scheduler] def internalMethod(): Unit
```

## Java Documentation (Javadoc)

Java code uses Javadoc for API documentation.

### Basic Format

```java
/**
 * Brief one-line description.
 * <p>
 * Detailed description that can span multiple paragraphs.
 * Explain what this class/method does and important behavior.
 * </p>
 *
 * @param paramName description of parameter
 * @param anotherParam description of another parameter
 * @return description of return value
 * @throws ExceptionType when this exception is thrown
 * @since 3.5.0
 */
public ReturnType methodName(String paramName, int anotherParam) 
    throws ExceptionType {
  // Implementation
}
```

### Class Documentation

```java
/**
 * Brief description of the class purpose.
 * <p>
 * Detailed explanation of functionality, usage patterns,
 * and important considerations.
 * </p>
 * <p>
 * Example usage:
 * <pre>{@code
 * MyClass example = new MyClass(param1, param2);
 * example.doSomething();
 * }</pre>
 * </p>
 *
 * @param <T> type parameter description
 * @since 3.0.0
 */
public class MyClass<T> implements Serializable {
  // Class implementation
}
```

### Interface Documentation

```java
/**
 * Interface for shuffle block resolution.
 * <p>
 * Implementations of this interface are responsible for
 * resolving shuffle block locations and reading shuffle data.
 * </p>
 *
 * @since 2.3.0
 */
public interface ShuffleBlockResolver {
  /**
   * Gets the data for a shuffle block.
   *
   * @param blockId the block identifier
   * @return managed buffer containing the block data
   */
  ManagedBuffer getBlockData(BlockId blockId);
}
```

## Python Documentation (Docstrings)

Python code uses docstrings following PEP 257 and Google style.

### Function Documentation

```python
def function_name(param1: str, param2: int) -> bool:
    """
    Brief one-line description.
    
    Detailed description that can span multiple lines.
    Explain what this function does, important behavior,
    and any constraints.
    
    Parameters
    ----------
    param1 : str
        Description of param1
    param2 : int
        Description of param2
    
    Returns
    -------
    bool
        Description of return value
    
    Raises
    ------
    ValueError
        When input is invalid
    
    Examples
    --------
    >>> result = function_name("test", 42)
    >>> print(result)
    True
    
    Notes
    -----
    Important notes about usage or behavior.
    
    .. versionadded:: 3.5.0
    """
    # Implementation
    pass
```

### Class Documentation

```python
class MyClass:
    """
    Brief description of the class.
    
    Detailed explanation of the class functionality,
    usage patterns, and important considerations.
    
    Parameters
    ----------
    config : dict
        Configuration dictionary
    is_local : bool, optional
        Whether running in local mode (default is False)
    
    Attributes
    ----------
    config : dict
        Stored configuration
    state : str
        Current state of the object
    
    Examples
    --------
    >>> obj = MyClass({'key': 'value'}, is_local=True)
    >>> obj.do_something()
    
    Notes
    -----
    This class is thread-safe.
    
    .. versionadded:: 3.0.0
    """
    
    def __init__(self, config: dict, is_local: bool = False):
        self.config = config
        self.is_local = is_local
        self.state = "initialized"
```

### Type Hints

Use type hints consistently:

```python
from typing import List, Optional, Dict, Any, Union
from pyspark.sql import DataFrame

def process_data(
    df: DataFrame,
    columns: List[str],
    options: Optional[Dict[str, Any]] = None
) -> Union[DataFrame, None]:
    """
    Process DataFrame with specified columns.
    
    Parameters
    ----------
    df : DataFrame
        Input DataFrame to process
    columns : list of str
        Column names to include
    options : dict, optional
        Processing options
    
    Returns
    -------
    DataFrame or None
        Processed DataFrame, or None if processing fails
    """
    pass
```

## R Documentation (Roxygen2)

R code uses Roxygen2-style documentation.

### Function Documentation

```r
#' Brief one-line description
#'
#' Detailed description that can span multiple lines.
#' Explain what this function does and important behavior.
#'
#' @param param1 description of param1
#' @param param2 description of param2
#' @return description of return value
#' @examples
#' \dontrun{
#' result <- myFunction(param1 = "test", param2 = 42)
#' print(result)
#' }
#' @note Important note about usage
#' @rdname function-name
#' @since 3.0.0
#' @export
myFunction <- function(param1, param2) {
  # Implementation
}
```

### Class Documentation

```r
#' MyClass: A class for doing XYZ
#'
#' Detailed description of the class functionality
#' and usage patterns.
#'
#' @slot field1 description of field1
#' @slot field2 description of field2
#' @export
#' @since 3.0.0
setClass("MyClass",
  slots = c(
    field1 = "character",
    field2 = "numeric"
  )
)
```

## Documentation Best Practices

### 1. Write Clear, Concise Descriptions

**Good:**
```scala
/**
 * Computes the mean of values in the RDD.
 *
 * @return the arithmetic mean, or NaN if the RDD is empty
 */
def mean(): Double
```

**Bad:**
```scala
/**
 * This method calculates and returns the mean.
 */
def mean(): Double
```

### 2. Document Edge Cases

```scala
/**
 * Divides two numbers.
 *
 * @param a numerator
 * @param b denominator
 * @return result of a / b
 * @throws ArithmeticException if b is zero
 * @note Returns Double.PositiveInfinity if a > 0 and b = 0+
 */
def divide(a: Double, b: Double): Double
```

### 3. Provide Examples

Always include examples for public APIs:

```scala
/**
 * Filters elements using the given predicate.
 *
 * Example:
 * {{{
 * val rdd = sc.parallelize(1 to 10)
 * val evens = rdd.filter(_ % 2 == 0)
 * evens.collect() // Array(2, 4, 6, 8, 10)
 * }}}
 */
def filter(f: T => Boolean): RDD[T]
```

### 4. Document Thread Safety

```scala
/**
 * Thread-safe cache implementation.
 *
 * @note This class uses internal synchronization and is safe
 *       for concurrent access from multiple threads.
 */
class ConcurrentCache[K, V] extends Cache[K, V]
```

### 5. Document Performance Characteristics

```scala
/**
 * Sorts the RDD by key.
 *
 * @note This operation triggers a shuffle and is expensive.
 *       The time complexity is O(n log n) where n is the
 *       number of elements.
 */
def sortByKey(): RDD[(K, V)]
```

### 6. Link to Related APIs

```scala
/**
 * Maps elements to key-value pairs.
 *
 * @see [[groupByKey]] for grouping by keys
 * @see [[reduceByKey]] for aggregating by keys
 */
def keyBy[K](f: T => K): RDD[(K, T)]
```

### 7. Version Information

```scala
/**
 * New feature introduced in 3.5.0.
 *
 * @since 3.5.0
 */
def newMethod(): Unit

/**
 * Deprecated method, use [[newMethod]] instead.
 *
 * @deprecated Use newMethod() instead, since 3.5.0
 */
@deprecated("Use newMethod() instead", "3.5.0")
def oldMethod(): Unit
```

## Internal Documentation

### Code Comments

Use comments for complex logic:

```scala
// Sort by key and value to ensure deterministic output
// This is critical for testing and reproducing results
val sorted = data.sortBy(x => (x._1, x._2))

// TODO: Optimize this for large datasets
// Current implementation loads all data into memory
val result = computeExpensiveOperation()

// FIXME: This breaks when input size exceeds Int.MaxValue
val size = data.size.toInt
```

### Architecture Comments

Document architectural decisions:

```scala
/**
 * Internal scheduler implementation.
 *
 * Architecture:
 * 1. Jobs are submitted to DAGScheduler
 * 2. DAGScheduler creates stages based on shuffle boundaries
 * 3. Each stage is submitted as a TaskSet to TaskScheduler
 * 4. TaskScheduler assigns tasks to executors
 * 5. Task results are returned to the driver
 *
 * Thread Safety:
 * - DAGScheduler runs in a single thread (event loop)
 * - TaskScheduler methods are thread-safe
 * - Results are collected with appropriate synchronization
 */
private[spark] class SchedulerImpl
```

## Generating Documentation

### Scaladoc

```bash
# Generate Scaladoc
./build/mvn scala:doc

# Output in target/site/scaladocs/
```

### Javadoc

```bash
# Generate Javadoc
./build/mvn javadoc:javadoc

# Output in target/site/apidocs/
```

### Python Documentation

```bash
# Generate Sphinx documentation
cd python/docs
make html

# Output in _build/html/
```

### R Documentation

```bash
# Generate R documentation
cd R/pkg
R CMD Rd2pdf .
```

## Documentation Review Checklist

When reviewing documentation:

- [ ] Is the description clear and accurate?
- [ ] Are all parameters documented?
- [ ] Is the return value documented?
- [ ] Are exceptions/errors documented?
- [ ] Are examples provided for public APIs?
- [ ] Is thread safety documented if relevant?
- [ ] Are performance characteristics noted?
- [ ] Is version information included?
- [ ] Are deprecated APIs marked?
- [ ] Are there links to related APIs?
- [ ] Is internal vs. public API clearly marked?

## Tools

### IDE Support

- **IntelliJ IDEA**: Auto-generates documentation templates
- **VS Code**: Extensions for Scaladoc/Javadoc
- **Eclipse**: Built-in Javadoc support

### Linters

- **Scalastyle**: Checks for missing Scaladoc
- **Checkstyle**: Validates Javadoc
- **Pylint**: Checks Python docstrings
- **roxygen2**: Validates R documentation

## Resources

- [Scaladoc Style Guide](https://docs.scala-lang.org/style/scaladoc.html)
- [Oracle Javadoc Guide](https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html)
- [PEP 257 - Docstring Conventions](https://www.python.org/dev/peps/pep-0257/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Roxygen2 Documentation](https://roxygen2.r-lib.org/)

## Contributing

When contributing code to Spark:

1. Follow the documentation style for your language
2. Document all public APIs
3. Include examples for new features
4. Update existing documentation when changing behavior
5. Run documentation generators to verify formatting

For more information, see [CONTRIBUTING.md](CONTRIBUTING.md).
