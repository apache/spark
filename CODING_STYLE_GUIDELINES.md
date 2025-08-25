# Coding Style Guidelines

This document outlines the coding style guidelines to follow when making changes to the Spark codebase.

## General Rules

### Line Length
- **Maximum line length: 100 characters**
- Break long lines appropriately with proper indentation
- For imports, break at logical groupings and indent continuation lines

**Examples:**
```scala
// Good - broken at logical points
import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, GlobalTempView, LocalTempView,
  PersistedView, PlanWithUnresolvedIdentifier, SchemaEvolution, SchemaTypeEvolution,
  UnresolvedAttribute, UnresolvedFunctionName, UnresolvedIdentifier, UnresolvedNamespace}

// Good - long method call broken appropriately
val paramSubstituted = 
  org.apache.spark.sql.catalyst.parser.ThreadLocalParameterContext.get() match {

// Bad - exceeds 100 characters
import org.apache.spark.sql.catalyst.analysis.{CurrentNamespace, GlobalTempView, LocalTempView, PersistedView, PlanWithUnresolvedIdentifier, SchemaEvolution, SchemaTypeEvolution, UnresolvedAttribute, UnresolvedFunctionName, UnresolvedIdentifier, UnresolvedNamespace}
```

### Whitespace
- **No trailing whitespace** on any line
- Use consistent indentation (2 spaces for Scala)
- Add blank lines to separate logical sections

**Check for violations:**
```bash
# Find trailing whitespace
grep -n "[ \t]$" filename.scala

# Find lines exceeding 100 characters  
grep -n ".\{101,\}" filename.scala
```

## Scala-Specific Guidelines

### Import Statements
- Group related imports together
- Break long import lists across multiple lines
- Use proper indentation for continuation lines (2 spaces)

### Method Definitions
- Break long parameter lists across multiple lines
- Align parameters appropriately
- Keep method signatures under 100 characters when possible

### Pattern Matching
- Align case statements
- Break long case expressions appropriately

## Commit Guidelines

### Before Each Commit
1. **Check line length:** `grep -n ".\{101,\}" filename.scala`
2. **Check trailing whitespace:** `grep -n "[ \t]$" filename.scala`
3. **Run linter:** Use `read_lints` tool to check for style violations
4. **Fix all violations** before committing

### Commit Messages
- Include style fixes in commit messages
- Document any new style guidelines established
- Reference specific violations fixed

## Tools and Automation

### Manual Checks
```bash
# Check for style violations in a file
grep -n ".\{101,\}" path/to/file.scala     # Line length
grep -n "[ \t]$" path/to/file.scala        # Trailing whitespace
```

### Linting
- Always run linting tools before committing
- Fix all linter errors and warnings
- Don't commit code with known style violations

## Enforcement

- **All new code** must follow these guidelines
- **All modified code** should be brought into compliance where possible
- **Code reviews** should enforce these standards
- **Automated checks** should be implemented where feasible

## Examples of Common Fixes

### Long Import Statements
```scala
// Before (too long)
import org.apache.spark.sql.catalyst.analysis.{A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P}

// After (properly broken)
import org.apache.spark.sql.catalyst.analysis.{A, B, C, D, E, F, G, H,
  I, J, K, L, M, N, O, P}
```

### Long Method Calls
```scala
// Before (too long)
val result = someVeryLongObjectName.someVeryLongMethodName(param1, param2, param3, param4)

// After (properly broken)
val result = someVeryLongObjectName.someVeryLongMethodName(
  param1, param2, param3, param4)
```

### Pattern Matching
```scala
// Before (too long)
context match { case SomeVeryLongCaseClassName(param1, param2, param3, param4) => doSomething() }

// After (properly broken)
context match {
  case SomeVeryLongCaseClassName(param1, param2, param3, param4) =>
    doSomething()
}
```

---

**Note:** These guidelines should be followed consistently across all future changes to maintain code quality and readability.
