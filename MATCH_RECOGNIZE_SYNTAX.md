# MATCH_RECOGNIZE Clause - Syntax Description

## Overview

The `MATCH_RECOGNIZE` clause is a powerful SQL feature for performing pattern recognition and sequence analysis over ordered sets of rows. It enables detection of patterns in time-series data, event sequences, and other ordered datasets using a regular expression-like syntax.

## General Syntax

```sql
MATCH_RECOGNIZE (
    [ PARTITION BY partition_expression [, ...] ]
    ORDER BY order_expression [ ASC | DESC ] [, ...]
    MEASURES measure_expression AS alias [, ...]
    [ ONE ROW PER MATCH | ALL ROWS PER MATCH ]
    [ AFTER MATCH skip_clause ]
    PATTERN ( pattern_expression )
    [ SUBSET subset_definition [, ...] ]
    DEFINE pattern_variable AS condition [, ...]
)
```

## Clause Components

### 1. PARTITION BY (Optional)

**Purpose**: Divides the input data into independent partitions for parallel pattern matching.

**Syntax**:
```sql
PARTITION BY column_expression [, column_expression ...]
```

**Examples from corpus**:
```sql
PARTITION BY match_0_0
PARTITION BY accountRegion
PARTITION BY field_name
```

**Notes**:
- Pattern matching is performed independently within each partition
- Similar to window function partitioning
- Can be omitted for global pattern matching across all rows
- Supports single or multiple partitioning columns

### 2. ORDER BY (Required)

**Purpose**: Specifies the order of rows within each partition for pattern evaluation.

**Syntax**:
```sql
ORDER BY column_expression [ ASC | DESC ] [, ...]
```

**Examples from corpus**:
```sql
ORDER BY p_event_time ASC
```

**Notes**:
- **REQUIRED** - Pattern matching depends on row ordering
- Typically orders by timestamp for temporal pattern detection
- Supports ASC (ascending) or DESC (descending) ordering
- Can specify multiple ordering columns

### 3. MEASURES

**Purpose**: Defines computed values to be returned for each pattern match.

**Syntax**:
```sql
MEASURES
    expression AS alias [,
    expression AS alias ...]
```

**Common Functions Used in MEASURES**:

| Function | Description | Example |
|----------|-------------|---------|
| `MATCH_NUMBER()` | Returns a unique identifier for each match | `MATCH_NUMBER() AS match_number` |
| `FIRST(column)` | Returns value from first row of the match | `FIRST(p_event_time) AS start_time` |
| `LAST(column)` | Returns value from last row of the match | `LAST(p_event_time) AS end_time` |
| `COUNT(pattern.*)` | Counts rows matching a specific pattern variable | `COUNT(pattern_Login.*) AS num_logins` |

**Examples from corpus**:
```sql
MEASURES
    MATCH_NUMBER() AS match_number,
    FIRST(p_event_time) AS start_time,
    LAST(p_event_time) AS end_time,
    COUNT(pattern_AWS_EC2_Startup_Script_Change.*) AS num_pattern_AWS_EC2_Startup_Script_Change,
    COUNT(pattern_AWS_EC2_StopInstances.*) AS num_pattern_AWS_EC2_StopInstances
```

**Notes**:
- Can reference pattern variables using dot notation (e.g., `pattern_variable.*`)
- Supports aggregate functions and row-level functions
- Column references without qualifiers refer to the entire match

### 4. Output Mode

**Purpose**: Determines how many rows are returned per match.

**Options**:

#### ONE ROW PER MATCH
- Returns a single summary row for each pattern match
- Contains only MEASURES values
- **Default behavior** (if not specified)

#### ALL ROWS PER MATCH
- Returns all rows that participated in the match
- Each row includes the MEASURES values
- Useful for detailed analysis of matched sequences

**Examples from corpus**:
```sql
ALL ROWS PER MATCH
```

**Note**: All examples in the corpus use `ALL ROWS PER MATCH`.

### 5. AFTER MATCH (Optional)

**Purpose**: Specifies where to resume pattern matching after a match is found.

**Syntax**:
```sql
AFTER MATCH skip_strategy
```

**Skip Strategies**:

| Strategy | Description | When to Use |
|----------|-------------|-------------|
| `SKIP PAST LAST ROW` | Resume after the last row of the current match | Non-overlapping matches (most common) |
| `SKIP TO NEXT ROW` | Resume from the row after the first row of the match | Overlapping matches allowed |
| `SKIP TO FIRST pattern_variable` | Resume at the first row of the specified pattern variable | Complex overlapping scenarios |
| `SKIP TO LAST pattern_variable` | Resume at the last row of the specified pattern variable | Complex overlapping scenarios |

**Examples from corpus**:
```sql
AFTER MATCH SKIP PAST LAST ROW
```

**Note**: All examples in the corpus use `SKIP PAST LAST ROW`, which is the most common strategy for detecting distinct, non-overlapping sequences.

### 6. PATTERN (Required)

**Purpose**: Defines the sequence pattern to match using regular expression-like syntax.

**Syntax**:
```sql
PATTERN ( pattern_expression )
```

**Pattern Quantifiers**:

| Quantifier | Description | Example |
|------------|-------------|---------|
| `{n}` | Exactly n occurrences | `A{3}` - exactly 3 A's |
| `{n,}` | At least n occurrences | `A{1,}` - one or more A's |
| `{n,m}` | Between n and m occurrences | `A{2,5}` - 2 to 5 A's |
| `{0,0}` | Zero occurrences (used with PERMUTE for "absence" detection) | `A{0,0}` - no A's |
| `+` | One or more (equivalent to `{1,}`) | `A+` - one or more A's |
| `*` | Zero or more | `A*` - zero or more A's |
| `?` | Zero or one | `A?` - optional A |

**Pattern Operators**:

| Operator | Description | Example |
|----------|-------------|---------|
| Space (concatenation) | Sequential pattern | `A B C` - A followed by B followed by C |
| `\|` (alternation) | Either pattern | `A \| B` - either A or B |
| `()` (grouping) | Groups sub-patterns | `(A B)+` - one or more A-B sequences |

**Special Pattern Functions**:

#### PERMUTE
**Purpose**: Matches pattern variables in any order (not necessarily sequential).

**Syntax**:
```sql
PATTERN ( PERMUTE(pattern_var1{n1,m1}, pattern_var2{n2,m2}, ...) )
```

**Examples from corpus**:
```sql
-- Any order of 4 different patterns
PATTERN (PERMUTE(
    pattern_AWS_CloudTrail_SES_CheckSESSendingEnabled{1,},
    pattern_AWS_CloudTrail_SES_CheckSendQuota{1,},
    pattern_AWS_CloudTrail_SES_ListIdentities{1,},
    pattern_AWS_CloudTrail_SES_CheckIdentityVerifications{1,}
))

-- Detect presence of A but absence of B (within time window)
PATTERN (PERMUTE(pattern_GitHub_Advanced_Security_Change{1,}, pattern_Github_Repo_Archived{0,0}))
```

**Standard Sequential Patterns**:

```sql
-- Simple sequence: A followed by B
PATTERN (pattern_A{1,} pattern_B{1,})

-- Complex sequence: A followed by B followed by C
PATTERN (pattern_A{1,} pattern_B{1,} pattern_C{1,})

-- Three-step sequence with minimum occurrences
PATTERN (pattern_TempStageCreated{1,} pattern_CopyIntoStage{1,} pattern_FileDownloaded{1,})

-- Sequence with specific count requirement
PATTERN (pattern_BruteForce{5,} pattern_LoginSuccess{1,})
```

### 7. SUBSET (Optional)

**Purpose**: Creates a union of multiple pattern variables under a single alias.

**Syntax**:
```sql
SUBSET subset_name = (pattern_var1, pattern_var2, ...)
```

**Use Cases**:
- Grouping related pattern variables for aggregate functions
- Simplifying DEFINE conditions that apply to multiple variables
- Creating logical groups of events

**Note**: Not commonly used in the corpus examples, but supported in the standard.

### 8. DEFINE (Required)

**Purpose**: Specifies the conditions that rows must satisfy to be classified as each pattern variable.

**Syntax**:
```sql
DEFINE
    pattern_variable AS condition [,
    pattern_variable AS condition ...]
```

**Common Condition Patterns**:

#### Simple Equality Conditions
```sql
pattern_AWS_Console_Login AS p_rule_id = 'AWS.Console.Login'
pattern_Okta_Login AS p_rule_id = 'Okta.Login.Success'
```

#### Time-Based Constraints with LAG

**Purpose**: Ensure events occur within a specified time window.

```sql
-- Pattern must occur within N minutes of previous event
pattern_Variable AS p_rule_id = 'Rule.ID'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 60)
```

**Common time windows from corpus**:
- 15 minutes: Quick succession events
- 30 minutes: Related security events
- 60 minutes: Related workflow events
- 90 minutes: Extended workflow patterns
- 120 minutes: Long-running processes
- 720 minutes (12 hours): Extended persistence patterns

#### Negative Time Constraints with LAG

**Purpose**: Ensure a preceding event did NOT occur within a time window.

```sql
-- Match if previous event was different OR happened too long ago
pattern_Variable AS p_rule_id = 'Current.Rule'
    AND (LAG(p_rule_id, 1, '') != 'Previous.Rule'
         OR (LAG(p_rule_id, 1, '') = 'Previous.Rule'
             AND ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) > 15))
```

#### Negative Time Constraints with LEAD

**Purpose**: Ensure a following event did NOT occur within a time window.

```sql
-- Match if next event is different OR happens too far in future
pattern_Variable AS p_rule_id = 'Current.Rule'
    AND (LEAD(p_rule_id, 1, '') != 'Next.Rule'
         OR (LEAD(p_rule_id, 1, '') = 'Next.Rule'
             AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > 60))
```

**Navigation Functions in DEFINE**:

| Function | Description | Example Use Case |
|----------|-------------|------------------|
| `LAG(column, offset, default)` | Access preceding row value | Time gap from previous event |
| `LEAD(column, offset, default)` | Access following row value | Time gap to next event |
| `PREV(column)` | Previous row (shorthand for LAG) | Price comparison |
| `FIRST(column)` | First row in match so far | Compare to starting value |
| `LAST(column)` | Last row in match so far | Compare to ending value |

**Complex Condition Example**:
```sql
DEFINE
    pattern_AWS_EC2_Startup_Script_Change AS
        p_rule_id = 'AWS.EC2.Startup.Script.Change'
        AND (LAG(p_event_time, 1, NULL) is NULL
             OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 90),
    pattern_AWS_EC2_StopInstances AS
        p_rule_id = 'AWS.EC2.StopInstances'
```

## Common Pattern Examples

### 1. Sequential Event Detection

**Use Case**: Detect A followed by B within a time window.

```sql
MATCH_RECOGNIZE (
    PARTITION BY user_id
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(event_time) AS start_time,
        LAST(event_time) AS end_time
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_A{1,} pattern_B{1,})
    DEFINE
        pattern_A AS event_type = 'TypeA',
        pattern_B AS event_type = 'TypeB'
            AND (LAG(event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(event_time), event_time)) <= 60)
)
```

### 2. Three-Step Sequential Pattern

**Use Case**: Detect multi-stage attack or workflow (A → B → C).

```sql
MATCH_RECOGNIZE (
    PARTITION BY entity_id
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(event_time) AS start_time,
        LAST(event_time) AS end_time,
        COUNT(pattern_A.*) AS num_a,
        COUNT(pattern_B.*) AS num_b,
        COUNT(pattern_C.*) AS num_c
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_A{1,} pattern_B{1,} pattern_C{1,})
    DEFINE
        pattern_A AS event_type = 'TypeA',
        pattern_B AS event_type = 'TypeB'
            AND (LAG(event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(event_time), event_time)) <= 15),
        pattern_C AS event_type = 'TypeC'
            AND (LAG(event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(event_time), event_time)) <= 15)
)
```

### 3. Absence Detection (Negative Pattern)

**Use Case**: Detect event A without subsequent event B within time window.

```sql
MATCH_RECOGNIZE (
    PARTITION BY entity_id
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        COUNT(pattern_A.*) AS num_a
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_A{1,})
    DEFINE
        pattern_A AS event_type = 'TypeA'
            AND (LEAD(event_type, 1, '') != 'TypeB'
                 OR (LEAD(event_type, 1, '') = 'TypeB'
                     AND ABS(DATEDIFF(MINS, LEAD(event_time), event_time)) > 60))
)
```

### 4. Unordered Pattern Matching (PERMUTE)

**Use Case**: Detect all of multiple events in any order.

```sql
MATCH_RECOGNIZE (
    PARTITION BY account_region
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        COUNT(pattern_A.*) AS num_a,
        COUNT(pattern_B.*) AS num_b,
        COUNT(pattern_C.*) AS num_c
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (PERMUTE(pattern_A{1,}, pattern_B{1,}, pattern_C{1,}))
    DEFINE
        pattern_A AS event_type = 'TypeA',
        pattern_B AS event_type = 'TypeB',
        pattern_C AS event_type = 'TypeC'
)
```

### 5. Threshold-Based Pattern

**Use Case**: Detect N failures followed by success (e.g., brute force).

```sql
MATCH_RECOGNIZE (
    PARTITION BY ip_address
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(event_time) AS start_time,
        LAST(event_time) AS end_time,
        COUNT(pattern_Failure.*) AS num_failures,
        COUNT(pattern_Success.*) AS num_successes
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_Failure{5,} pattern_Success{1,})
    DEFINE
        pattern_Failure AS event_type = 'LoginFailure',
        pattern_Success AS event_type = 'LoginSuccess'
            AND (LAG(event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(event_time), event_time)) <= 30)
)
```

### 6. Global Pattern (No Partitioning)

**Use Case**: Match patterns across entire dataset.

```sql
MATCH_RECOGNIZE (
    ORDER BY event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(event_time) AS start_time,
        LAST(event_time) AS end_time
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_A{1,})
    DEFINE
        pattern_A AS event_type = 'TargetEvent'
            AND (LEAD(event_type, 1, '') != 'FollowUp'
                 OR (LEAD(event_type, 1, '') = 'FollowUp'
                     AND ABS(DATEDIFF(MINS, LEAD(event_time), event_time)) > 60))
)
```

## Key Design Patterns from Corpus

### 1. Time Window Constraints

Most security and event correlation use cases require events to occur within specific time windows:

```sql
-- Within N minutes of previous event
AND (LAG(p_event_time, 1, NULL) is NULL
     OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 60)
```

### 2. Negative Pattern Detection

Detecting what DIDN'T happen is crucial for security anomaly detection:

```sql
-- Event A without event B within time window
AND (LEAD(p_rule_id, 1, '') != 'Expected.Event'
     OR (LEAD(p_rule_id, 1, '') = 'Expected.Event'
         AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > threshold))
```

### 3. Comprehensive Measures

Security and audit applications typically capture:
- Match identifier: `MATCH_NUMBER()`
- Temporal bounds: `FIRST(p_event_time)`, `LAST(p_event_time)`
- Event counts: `COUNT(pattern_variable.*)` for each pattern variable

### 4. Consistent Naming Convention

Pattern variables follow clear naming: `pattern_<Event_Name>`
- Example: `pattern_AWS_IAM_CreateUser`, `pattern_Okta_Login_Success`

## Implementation Notes

### Typical Use Cases

1. **Security Event Correlation**: Detect multi-stage attacks, privilege escalation, account compromise
2. **Fraud Detection**: Identify suspicious transaction sequences
3. **Workflow Monitoring**: Track multi-step processes and detect anomalies
4. **SLA Monitoring**: Detect missing or delayed steps in expected sequences
5. **Behavioral Analytics**: Identify unusual patterns in user behavior

### Performance Considerations

1. **Partitioning**: Proper partitioning is critical for performance and correctness
   - Partition by entity (user, account, IP address, etc.)
   - Ensures pattern matching within related event streams

2. **Ordering**: Always order by timestamp for temporal patterns
   - Use ASC for forward-looking patterns
   - Critical for LAG/LEAD correctness

3. **Time Windows**: Use DATEDIFF constraints to limit pattern search scope
   - Prevents matching across unrelated time periods
   - Improves query performance

### Best Practices

1. **Always specify PARTITION BY** unless truly global pattern matching is needed
2. **Use ALL ROWS PER MATCH** for detailed forensics and debugging
3. **Include MATCH_NUMBER()** in MEASURES for unique match identification
4. **Capture start/end times** using FIRST() and LAST() functions
5. **Count each pattern variable** to understand match composition
6. **Use SKIP PAST LAST ROW** for non-overlapping matches (most common)
7. **Apply time window constraints** in DEFINE to ensure temporal relevance
8. **Use NULL checks with LAG/LEAD** to handle first/last rows in partition
9. **Use ABS(DATEDIFF(...))** for bidirectional time comparisons

## SQL Dialect Notes

The examples in this corpus appear to be written for **Snowflake SQL** dialect, evidenced by:

- `DATEDIFF(MINS, ...)` function with MINS as first parameter
- `LATERAL FLATTEN` for JSON array expansion
- Snowflake-specific table references and system functions
- `p_occurs_since()` custom function for time filtering

Key functions used:
- `DATEDIFF(unit, start, end)`: Calculate time difference
- `LAG(column, offset, default)`: Access previous row
- `LEAD(column, offset, default)`: Access next row
- `ABS()`: Absolute value
- `COALESCE()`: Return first non-null value
- `GET_PATH()`: Extract JSON path value

## Conclusion

The MATCH_RECOGNIZE clause provides a powerful, declarative way to perform pattern matching on ordered datasets. The syntax combines SQL's familiar structure with regular expression-like pattern matching, making it particularly effective for:

- Temporal sequence analysis
- Security event correlation
- Fraud detection
- Process mining
- Behavioral analytics

The key to effective use is:
1. Proper partitioning by entity
2. Correct temporal ordering
3. Well-defined pattern variables with time constraints
4. Comprehensive measures for analysis
5. Appropriate skip strategy for match handling




