# Guidelines

To throw a standardized user-facing error or exception, developers should specify the error class
and message parameters rather than an arbitrary error message.

## Usage

1. Check if an appropriate error class already exists in `error_classes.py`.
   If true, use the error class and skip to step 3.
2. Add a new class to `error_classes.py`; keep in mind the invariants below.
3. Check if the exception type already extends `PySparkException`.
   If true, skip to step 5.
4. Mix `PySparkException` into the exception.
5. Throw the exception with the error class and message parameters.

### Before

Throw with arbitrary error message:

```python
raise ValueError("Problem A because B")
```

### After

`error_classes.py`

```python
"PROBLEM_BECAUSE": {
  "message": ["Problem <problem> because <cause>"]
}
```

`exceptions.py`

```python
class PySparkTestError(PySparkException):
    def __init__(self, error_class: str, message_parameters: Dict[str, str]):
        super().__init__(error_class=error_class, message_parameters=message_parameters)

    def getMessageParameters(self) -> Optional[Dict[str, str]]:
        return super().getMessageParameters()
```

Throw with error class and message parameters:

```python
raise PySparkTestError("PROBLEM_BECAUSE", {"problem": "A", "cause": "B"})
```

## Access fields

To access error fields, catch exceptions that extend `pyspark.errors.PySparkException` and access to error class with `getErrorClass`

```python
try:
    ...
except PySparkException as pe:
    if pe.getErrorClass() == "PROBLEM_BECAUSE":
        ...
```

## Fields

### Error class

Error classes are a succinct, human-readable representation of the error category.

An uncategorized errors can be assigned to a legacy error class with the prefix `_LEGACY_ERROR_TEMP_` and an unused sequential number, for instance `_LEGACY_ERROR_TEMP_0053`.

#### Invariants

- Unique
- Consistent across releases
- Sorted alphabetically

### Message

Error messages provide a descriptive, human-readable representation of the error.
The message format accepts string parameters via the C-style printf syntax.

The quality of the error message should match the
[guidelines](https://spark.apache.org/error-message-guidelines.html).

#### Invariants

- Unique
