# MATCH_RECOGNIZE Clause - Syntax Observed in Examples

This document describes **only** the MATCH_RECOGNIZE syntax patterns actually present in the provided spreadsheet examples. No additional SQL standard features are included.

## Overall Structure Observed

Every MATCH_RECOGNIZE clause in the examples follows this structure:

```sql
FROM table_name
MATCH_RECOGNIZE (
    [ PARTITION BY column ]
    ORDER BY column ASC
    MEASURES
        measure_expression AS alias,
        ...
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN ( pattern_expression )
    DEFINE
        pattern_variable AS condition,
        ...
)
```

## Clause Usage in Examples

### Clauses Present in ALL Examples (18 out of 18)

1. ✅ **ORDER BY** - Present in all 18 examples
2. ✅ **MEASURES** - Present in all 18 examples
3. ✅ **ALL ROWS PER MATCH** - Present in all 18 examples
4. ✅ **AFTER MATCH SKIP PAST LAST ROW** - Present in all 18 examples
5. ✅ **PATTERN** - Present in all 18 examples (required)
6. ✅ **DEFINE** - Present in all 18 examples (required)

### Clauses Present in MOST Examples

7. ✅ **PARTITION BY** - Present in 17 out of 18 examples
   - Missing in: `secret_exposed_and_not_quarantined.yml`

### Clauses NEVER Used in Examples

- ❌ **ONE ROW PER MATCH** - Never used (all use ALL ROWS PER MATCH)
- ❌ **SUBSET** - Never used in any example

## Detailed Clause Breakdown

### 1. PARTITION BY

**Usage**: 17 out of 18 examples use PARTITION BY

**Observed Syntax**:
```sql
PARTITION BY single_column
```

**Examples from spreadsheet**:
```sql
PARTITION BY match_0_0
PARTITION BY accountRegion
PARTITION BY field_name
```

**Notes**:
- Always partitions by exactly ONE column
- Never uses multiple columns
- One example omits PARTITION BY entirely (global matching)

### 2. ORDER BY

**Usage**: 18 out of 18 examples use ORDER BY

**Observed Syntax**:
```sql
ORDER BY column ASC
```

**Examples from spreadsheet**:
```sql
ORDER BY p_event_time ASC
```

**Notes**:
- Always orders by exactly ONE column (always `p_event_time`)
- Always uses ASC (ascending)
- Never uses DESC
- Never uses multiple columns

### 3. MEASURES

**Usage**: 18 out of 18 examples use MEASURES

**Observed Functions**:

All examples use exactly this pattern:

```sql
MEASURES
    MATCH_NUMBER() AS match_number,
    FIRST(p_event_time) AS start_time,
    LAST(p_event_time) AS end_time,
    COUNT(pattern_variable_name.*) AS num_pattern_variable_name,
    COUNT(pattern_variable_name2.*) AS num_pattern_variable_name2,
    ...
```

**Functions Observed**:
1. `MATCH_NUMBER()` - Used in all 18 examples
2. `FIRST(column)` - Used in all 18 examples (always with `p_event_time`)
3. `LAST(column)` - Used in all 18 examples (always with `p_event_time`)
4. `COUNT(pattern.*)` - Used in all 18 examples (one or more per query)

**Actual Examples**:

```sql
-- Example 1: Two pattern variables
MEASURES
    MATCH_NUMBER() AS match_number,
    FIRST(p_event_time) AS start_time,
    LAST(p_event_time) AS end_time,
    COUNT(pattern_AWS_EC2_Startup_Script_Change.*) AS num_pattern_AWS_EC2_Startup_Script_Change,
    COUNT(pattern_AWS_EC2_StopInstances.*) AS num_pattern_AWS_EC2_StopInstances

-- Example 2: One pattern variable
MEASURES
    MATCH_NUMBER() AS match_number,
    FIRST(p_event_time) AS start_time,
    LAST(p_event_time) AS end_time,
    COUNT(pattern_AWS_Console_Sign_In.*) AS num_pattern_AWS_Console_Sign_In

-- Example 3: Four pattern variables
MEASURES
    MATCH_NUMBER() AS match_number,
    FIRST(p_event_time) AS start_time,
    LAST(p_event_time) AS end_time,
    COUNT(pattern_AWS_CloudTrail_SES_CheckSendQuota.*) AS num_pattern_AWS_CloudTrail_SES_CheckSendQuota,
    COUNT(pattern_AWS_CloudTrail_SES_CheckSESSendingEnabled.*) AS num_pattern_AWS_CloudTrail_SES_CheckSESSendingEnabled,
    COUNT(pattern_AWS_CloudTrail_SES_CheckIdentityVerifications.*) AS num_pattern_AWS_CloudTrail_SES_CheckIdentityVerifications,
    COUNT(pattern_AWS_CloudTrail_SES_ListIdentities.*) AS num_pattern_AWS_CloudTrail_SES_ListIdentities
```

**Pattern**:
- Every example counts occurrences of each pattern variable defined in DEFINE clause
- Naming convention: `num_` + pattern variable name

### 4. ALL ROWS PER MATCH

**Usage**: 18 out of 18 examples

**Observed Syntax**:
```sql
ALL ROWS PER MATCH
```

**Notes**:
- 100% of examples use this
- No examples use `ONE ROW PER MATCH`

### 5. AFTER MATCH

**Usage**: 18 out of 18 examples

**Observed Syntax**:
```sql
AFTER MATCH SKIP PAST LAST ROW
```

**Notes**:
- 100% of examples use `SKIP PAST LAST ROW`
- No other skip strategies observed:
  - Never uses `SKIP TO NEXT ROW`
  - Never uses `SKIP TO FIRST variable`
  - Never uses `SKIP TO LAST variable`

### 6. PATTERN

**Usage**: 18 out of 18 examples (required)

**Observed Pattern Types**:

#### Type 1: Sequential Pattern (11 examples)

**Syntax**:
```sql
PATTERN (pattern_A{n,} pattern_B{n,})
PATTERN (pattern_A{n,} pattern_B{n,} pattern_C{n,})
```

**Examples from spreadsheet**:

```sql
-- Two-step sequence
PATTERN (pattern_AWS_EC2_StopInstances{1,} pattern_AWS_EC2_Startup_Script_Change{1,})
PATTERN (pattern_AWS_IAM_CreateUser{1,} pattern_AWS_IAM_AttachAdminUserPolicy{1,})
PATTERN (pattern_AWS_IAM_CreateRole{1,} pattern_AWS_IAM_AttachAdminRolePolicy{1,})
PATTERN (pattern_AWS_IAM_Backdoor_User_Keys{1,} pattern_AWS_CloudTrail_UserAccessKeyAuth{1,})
PATTERN (pattern_AWS_CloudTrail_LoginProfileCreatedOrModified{1,} pattern_AWS_Console_Login{1,})
PATTERN (pattern_GCP_Cloud_Run_Service_Created{1,} pattern_GCP_Cloud_Run_Set_IAM_Policy{1,})
PATTERN (pattern_Notion_Login{1,} pattern_Notion_AccountChange{1,})
PATTERN (pattern_OneLogin_HighRiskFailedLogin{1,} pattern_OneLogin_Login{1,})
PATTERN (pattern_Okta_Login_Without_Push_Marker{1,} pattern_Push_Security_Phishing_Attack{1,})
PATTERN (pattern_Wiz_Alert_Passthrough{1,} pattern_AWS_VPC_SSHAllowedSignal{1,})
PATTERN (pattern_Crowdstrike_NewUserCreated{1,} pattern_Crowdstrike_UserDeleted{1,})

-- Three-step sequence
PATTERN (pattern_GCP_IAM_Tag_Enumeration{1,} pattern_GCP_Tag_Binding_Creation{1,} pattern_GCP_Privileged_Operation{1,})
PATTERN (pattern_Snowflake_TempStageCreated{1,} pattern_Snowflake_CopyIntoStage{1,} pattern_Snowflake_FileDownloaded{1,})

-- Sequence with minimum count > 1
PATTERN (pattern_Snowflake_Stream_BruteForceByIp{5,} pattern_Snowflake_Stream_LoginSuccess{1,})
```

#### Type 2: Single Pattern (Negative Detection) (4 examples)

**Syntax**:
```sql
PATTERN (pattern_A{1,})
```

**Examples from spreadsheet**:

```sql
PATTERN (pattern_AWS_Console_Sign_In{1,})
PATTERN (pattern_Retrieve_SSO_access_token{1,})
PATTERN (pattern_Okta_Login_Success{1,})
PATTERN (pattern_GitHub_Secret_Scanning_Alert_Created{1,})
```

**Note**: These use negative conditions in DEFINE with LEAD or LAG to detect absence of expected follow-up events.

#### Type 3: PERMUTE (Unordered) Pattern (2 examples)

**Syntax**:
```sql
PATTERN (PERMUTE(pattern_A{n,}, pattern_B{n,}, ...))
```

**Examples from spreadsheet**:

```sql
-- Absence detection: A present, B absent
PATTERN (PERMUTE(pattern_GitHub_Advanced_Security_Change{1,}, pattern_Github_Repo_Archived{0,0}))

-- All four patterns in any order
PATTERN (PERMUTE(
    pattern_AWS_CloudTrail_SES_CheckSESSendingEnabled{1,},
    pattern_AWS_CloudTrail_SES_CheckSendQuota{1,},
    pattern_AWS_CloudTrail_SES_ListIdentities{1,},
    pattern_AWS_CloudTrail_SES_CheckIdentityVerifications{1,}
))
```

**Quantifiers Observed**:

| Quantifier | Meaning | Example Count |
|------------|---------|---------------|
| `{1,}` | One or more | 16 examples |
| `{5,}` | Five or more | 1 example |
| `{0,0}` | Zero (absence) | 1 example |

**Quantifiers NOT Observed**:
- Never uses `{n}` (exactly n)
- Never uses `{n,m}` (between n and m)
- Never uses `+`, `*`, or `?` shorthand
- Never uses `|` (alternation)
- Never uses `()` grouping

### 7. DEFINE

**Usage**: 18 out of 18 examples (required)

**Observed Pattern**: Every DEFINE clause defines one or more pattern variables with conditions.

**Condition Types Observed**:

#### Type 1: Simple Equality (All Examples)

Every pattern variable starts with a simple equality check:

```sql
pattern_variable_name AS p_rule_id = 'Rule.Name'
```

#### Type 2: Simple Equality ONLY (3 examples)

Some pattern variables have ONLY the equality condition:

```sql
pattern_AWS_IAM_CreateUser AS p_rule_id = 'AWS.IAM.CreateUser'
pattern_GCP_IAM_Tag_Enumeration AS p_rule_id = 'GCP.IAM.Tag.Enumeration'
pattern_Snowflake_TempStageCreated AS p_rule_id = 'Snowflake.TempStageCreated'
```

#### Type 3: Equality + LAG Time Constraint (Most Common)

**Syntax Pattern**:
```sql
pattern_variable AS p_rule_id = 'Rule.Name'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= N)
```

**Actual Examples**:

```sql
-- 15 minutes
pattern_Notion_AccountChange AS p_rule_id = 'Notion.AccountChange'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 15)

-- 30 minutes
pattern_Snowflake_Stream_LoginSuccess AS p_rule_id = 'Snowflake.Stream.LoginSuccess'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 30)

-- 60 minutes
pattern_AWS_IAM_AttachAdminUserPolicy AS p_rule_id = 'AWS.IAM.AttachAdminUserPolicy'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 60)

-- 90 minutes
pattern_AWS_EC2_Startup_Script_Change AS p_rule_id = 'AWS.EC2.Startup.Script.Change'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 90)

-- 720 minutes (12 hours)
pattern_Crowdstrike_UserDeleted AS p_rule_id = 'Crowdstrike.UserDeleted'
    AND (LAG(p_event_time, 1, NULL) is NULL
         OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 720)
```

**Time Windows Observed**:
- 15 minutes: 3 examples
- 30 minutes: 1 example
- 60 minutes: 6 examples
- 90 minutes: 2 examples
- 120 minutes: 1 example
- 720 minutes: 1 example

#### Type 4: Negative LAG Constraint (Absence Detection)

**Syntax Pattern**:
```sql
pattern_variable AS p_rule_id = 'Current.Rule'
    AND (LAG(p_rule_id, 1, '') != 'Previous.Rule'
         OR (LAG(p_rule_id, 1, '') = 'Previous.Rule'
             AND ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) > N))
```

**Actual Examples**:

```sql
-- AWS Console sign-in WITHOUT Okta SSO within 15 minutes
pattern_AWS_Console_Sign_In AS p_rule_id = 'AWS.Console.Sign-In'
    AND (LAG(p_rule_id, 1, '') != 'Okta.SSO.to.AWS'
         OR (LAG(p_rule_id, 1, '') = 'Okta.SSO.to.AWS'
             AND ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) > 15))

-- SSO token retrieval WITHOUT CLI prompt within 120 minutes
pattern_Retrieve_SSO_access_token AS p_rule_id = 'Retrieve.SSO.access.token'
    AND (LAG(p_rule_id, 1, '') != 'Sign-in.with.AWS.CLI.prompt'
         OR (LAG(p_rule_id, 1, '') = 'Sign-in.with.AWS.CLI.prompt'
             AND ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) > 120))
```

**Pattern**: Detects event A when event B did NOT occur immediately before, or occurred too long ago.

#### Type 5: Negative LEAD Constraint (Absence Detection)

**Syntax Pattern**:
```sql
pattern_variable AS p_rule_id = 'Current.Rule'
    AND (LEAD(p_rule_id, 1, '') != 'Next.Rule'
         OR (LEAD(p_rule_id, 1, '') = 'Next.Rule'
             AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > N))
```

**Actual Examples**:

```sql
-- Okta login WITHOUT Push Security within 60 minutes
pattern_Okta_Login_Success AS p_rule_id = 'Okta.Login.Success'
    AND (LEAD(p_rule_id, 1, '') != 'Push.Security.Authorized.IdP.Login'
         OR (LEAD(p_rule_id, 1, '') = 'Push.Security.Authorized.IdP.Login'
             AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > 60))

-- GitHub secret exposed WITHOUT quarantine within 60 minutes
pattern_GitHub_Secret_Scanning_Alert_Created AS p_rule_id = 'GitHub.Secret.Scanning.Alert.Created'
    AND (LEAD(p_rule_id, 1, '') != 'AWS.CloudTrail.IAMCompromisedKeyQuarantine'
         OR (LEAD(p_rule_id, 1, '') = 'AWS.CloudTrail.IAMCompromisedKeyQuarantine'
             AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > 60))
```

**Pattern**: Detects event A when event B does NOT occur immediately after, or occurs too far in the future.

**Functions Used in DEFINE**:

| Function | Usage Count | Purpose |
|----------|-------------|---------|
| `LAG(column, offset, default)` | 14 examples | Access previous row value |
| `LEAD(column, offset, default)` | 2 examples | Access next row value |
| `DATEDIFF(MINS, start, end)` | 16 examples | Calculate minute difference |
| `ABS(value)` | 16 examples | Absolute value for time gaps |

**Functions NEVER Used**:
- `PREV()` - Never used (always use LAG instead)
- `FIRST()` - Never used in DEFINE (only in MEASURES)
- `LAST()` - Never used in DEFINE (only in MEASURES)

## Complete Pattern Templates from Examples

### Template 1: Sequential Two-Step Pattern (Most Common)

```sql
FROM filter_data
MATCH_RECOGNIZE (
    PARTITION BY match_column
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_StepA.*) AS num_pattern_StepA,
        COUNT(pattern_StepB.*) AS num_pattern_StepB
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_StepA{1,} pattern_StepB{1,})
    DEFINE
        pattern_StepA AS p_rule_id = 'Rule.A',
        pattern_StepB AS p_rule_id = 'Rule.B'
            AND (LAG(p_event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 60)
)
```

### Template 2: Sequential Three-Step Pattern

```sql
FROM filter_data
MATCH_RECOGNIZE (
    PARTITION BY match_column
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_StepA.*) AS num_pattern_StepA,
        COUNT(pattern_StepB.*) AS num_pattern_StepB,
        COUNT(pattern_StepC.*) AS num_pattern_StepC
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_StepA{1,} pattern_StepB{1,} pattern_StepC{1,})
    DEFINE
        pattern_StepA AS p_rule_id = 'Rule.A',
        pattern_StepB AS p_rule_id = 'Rule.B'
            AND (LAG(p_event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 15),
        pattern_StepC AS p_rule_id = 'Rule.C'
            AND (LAG(p_event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 15)
)
```

### Template 3: Absence Detection (Event Without Follow-up)

```sql
FROM filter_data
MATCH_RECOGNIZE (
    PARTITION BY match_column
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_EventA.*) AS num_pattern_EventA
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_EventA{1,})
    DEFINE
        pattern_EventA AS p_rule_id = 'Rule.A'
            AND (LEAD(p_rule_id, 1, '') != 'Rule.B'
                 OR (LEAD(p_rule_id, 1, '') = 'Rule.B'
                     AND ABS(DATEDIFF(MINS, LEAD(p_event_time), p_event_time)) > 60))
)
```

### Template 4: PERMUTE Pattern (Any Order)

```sql
FROM filter_data
MATCH_RECOGNIZE (
    PARTITION BY match_column
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_EventA.*) AS num_pattern_EventA,
        COUNT(pattern_EventB.*) AS num_pattern_EventB,
        COUNT(pattern_EventC.*) AS num_pattern_EventC
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (PERMUTE(pattern_EventA{1,}, pattern_EventB{1,}, pattern_EventC{1,}))
    DEFINE
        pattern_EventA AS p_rule_id = 'Rule.A',
        pattern_EventB AS p_rule_id = 'Rule.B',
        pattern_EventC AS p_rule_id = 'Rule.C'
)
```

### Template 5: Threshold Pattern (N occurrences then success)

```sql
FROM filter_data
MATCH_RECOGNIZE (
    PARTITION BY match_column
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_Failure.*) AS num_pattern_Failure,
        COUNT(pattern_Success.*) AS num_pattern_Success
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_Failure{5,} pattern_Success{1,})
    DEFINE
        pattern_Failure AS p_rule_id = 'Rule.Failure',
        pattern_Success AS p_rule_id = 'Rule.Success'
            AND (LAG(p_event_time, 1, NULL) is NULL
                 OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 30)
)
```

## Summary Statistics

### By Clause Usage

| Clause | Usage | Notes |
|--------|-------|-------|
| ORDER BY | 18/18 (100%) | Always `p_event_time ASC` |
| MEASURES | 18/18 (100%) | Always includes MATCH_NUMBER(), FIRST(), LAST(), COUNT() |
| ALL ROWS PER MATCH | 18/18 (100%) | No examples use ONE ROW PER MATCH |
| AFTER MATCH | 18/18 (100%) | Always `SKIP PAST LAST ROW` |
| PATTERN | 18/18 (100%) | Required clause |
| DEFINE | 18/18 (100%) | Required clause |
| PARTITION BY | 17/18 (94%) | One example omits it |
| SUBSET | 0/18 (0%) | Never used |

### By Pattern Type

| Pattern Type | Count | Percentage |
|--------------|-------|------------|
| Sequential (2 steps) | 10 | 56% |
| Sequential (3 steps) | 2 | 11% |
| Single pattern (absence detection) | 4 | 22% |
| PERMUTE (unordered) | 2 | 11% |

### By DEFINE Condition Type

| Condition Type | Approx. Count | Percentage |
|----------------|---------------|------------|
| Simple equality only | ~5 | ~28% |
| Equality + LAG time constraint | ~11 | ~61% |
| Equality + negative LAG | ~2 | ~11% |
| Equality + negative LEAD | ~2 | ~11% |

Note: Some examples have multiple pattern variables with different condition types, so percentages don't sum to 100%.

## Naming Conventions Observed

### Pattern Variables
All pattern variables follow this naming convention:
```
pattern_<Event_Name_With_Underscores>
```

Examples:
- `pattern_AWS_IAM_CreateUser`
- `pattern_GCP_Cloud_Run_Service_Created`
- `pattern_Okta_Login_Success`
- `pattern_Snowflake_TempStageCreated`

### Measure Aliases
All measure aliases follow consistent naming:
- Match identifier: `match_number`
- Start time: `start_time`
- End time: `end_time`
- Count pattern: `num_<pattern_variable_name>` (without "pattern_" prefix)

### Column Names
The examples use consistent column naming:
- Event time: `p_event_time`
- Rule ID: `p_rule_id`
- Match key: `match_0_0`, `accountRegion`, `field_name`, `empty_match`

## Complete Example Breakdown

### Example 1: aws_cloudtrail_stopinstance_followed_by_modifyinstanceattributes.yml

```sql
MATCH_RECOGNIZE (
    PARTITION BY match_0_0
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_AWS_EC2_Startup_Script_Change.*) AS num_pattern_AWS_EC2_Startup_Script_Change,
        COUNT(pattern_AWS_EC2_StopInstances.*) AS num_pattern_AWS_EC2_StopInstances
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_AWS_EC2_StopInstances{1,} pattern_AWS_EC2_Startup_Script_Change{1,})
    DEFINE
        pattern_AWS_EC2_Startup_Script_Change AS p_rule_id = 'AWS.EC2.Startup.Script.Change'
            AND (LAG(p_event_time, 1, NULL) is NULL OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 90),
        pattern_AWS_EC2_StopInstances AS p_rule_id = 'AWS.EC2.StopInstances'
)
```

**Pattern Type**: Sequential two-step
**Time Window**: 90 minutes
**Purpose**: Detect EC2 instance stop followed by startup script modification

### Example 2: github_advanced_security_change_not_followed_by_repo_archived.yml

```sql
MATCH_RECOGNIZE (
    PARTITION BY field_name
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_GitHub_Advanced_Security_Change.*) AS num_pattern_GitHub_Advanced_Security_Change,
        COUNT(pattern_Github_Repo_Archived.*) AS num_pattern_Github_Repo_Archived
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (PERMUTE(pattern_GitHub_Advanced_Security_Change{1,}, pattern_Github_Repo_Archived{0,0}))
    DEFINE
        pattern_GitHub_Advanced_Security_Change AS p_rule_id = 'GitHub.Advanced.Security.Change',
        pattern_Github_Repo_Archived AS p_rule_id = 'Github.Repo.Archived'
)
```

**Pattern Type**: PERMUTE with absence detection (`{0,0}`)
**Time Window**: None
**Purpose**: Detect security change WITHOUT repo being archived
**Special**: Uses `HAVING num_pattern_Github_Repo_Archived = 0` after MATCH_RECOGNIZE

### Example 3: snowflake_potential_brute_force_success.yml

```sql
MATCH_RECOGNIZE (
    PARTITION BY match_0_0
    ORDER BY p_event_time ASC
    MEASURES
        MATCH_NUMBER() AS match_number,
        FIRST(p_event_time) AS start_time,
        LAST(p_event_time) AS end_time,
        COUNT(pattern_Snowflake_Stream_BruteForceByIp.*) AS num_pattern_Snowflake_Stream_BruteForceByIp,
        COUNT(pattern_Snowflake_Stream_LoginSuccess.*) AS num_pattern_Snowflake_Stream_LoginSuccess
    ALL ROWS PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (pattern_Snowflake_Stream_BruteForceByIp{5,} pattern_Snowflake_Stream_LoginSuccess{1,})
    DEFINE
        pattern_Snowflake_Stream_BruteForceByIp AS p_rule_id = 'Snowflake.Stream.BruteForceByIp',
        pattern_Snowflake_Stream_LoginSuccess AS p_rule_id = 'Snowflake.Stream.LoginSuccess'
            AND (LAG(p_event_time, 1, NULL) is NULL OR ABS(DATEDIFF(MINS, LAG(p_event_time), p_event_time)) <= 30)
)
```

**Pattern Type**: Threshold-based (minimum 5 failures)
**Time Window**: 30 minutes
**Purpose**: Detect at least 5 brute force attempts followed by successful login

## Functions and Operators Summary

### Functions Used in MEASURES
- `MATCH_NUMBER()` - 18/18 examples
- `FIRST(column)` - 18/18 examples
- `LAST(column)` - 18/18 examples
- `COUNT(pattern.*)` - 18/18 examples

### Functions Used in DEFINE
- `LAG(column, offset, default)` - 14/18 examples
- `LEAD(column, offset, default)` - 2/18 examples
- `DATEDIFF(MINS, start, end)` - 16/18 examples
- `ABS(value)` - 16/18 examples

### Operators Used
- `=` (equality) - All examples
- `!=` (inequality) - 4 examples (negative detection)
- `AND` - All examples with complex conditions
- `OR` - All examples with LAG/LEAD time checks
- `<=` (less than or equal) - Positive time constraints
- `>` (greater than) - Negative time constraints

### Operators NOT Observed
- `<` (less than)
- `>=` (greater than or equal)
- `BETWEEN`
- `IN`
- `LIKE`
- Arithmetic operators (+, -, *, /)

## Snowflake-Specific Syntax

The examples appear to use Snowflake SQL dialect:

1. `DATEDIFF(MINS, start_time, end_time)` - Snowflake syntax for date difference
2. `LATERAL FLATTEN` - Snowflake JSON array processing (in filter CTE, before MATCH_RECOGNIZE)
3. `p_occurs_since('N minutes')` - Custom function for time filtering (in filter CTE)
4. `GET_PATH()` - Snowflake JSON path extraction (in filter CTE)

## Conclusion

Based on the 18 examples in the spreadsheet, the MATCH_RECOGNIZE syntax used follows a very consistent pattern:

**Always Present**:
- `PARTITION BY` (17/18 examples, always single column)
- `ORDER BY p_event_time ASC` (always)
- `MEASURES` with `MATCH_NUMBER()`, `FIRST()`, `LAST()`, `COUNT()` (always)
- `ALL ROWS PER MATCH` (always, never ONE ROW PER MATCH)
- `AFTER MATCH SKIP PAST LAST ROW` (always, no other strategies)
- `PATTERN` with `{1,}` or `{5,}` quantifiers (always)
- `DEFINE` with equality conditions, often with LAG/LEAD time constraints (always)

**Never Present**:
- `SUBSET` clause
- `ONE ROW PER MATCH`
- Other `AFTER MATCH` strategies
- Quantifiers: `{n}`, `{n,m}` (except `{0,0}`), `+`, `*`, `?`
- Pattern operators: `|`, grouping with `()`
- `PREV()` function

**Rarely Present**:
- `PERMUTE()` (2/18 examples)
- `LEAD()` (2/18 examples, mostly use LAG)
- Quantifier `{5,}` (1 example, threshold detection)
- Quantifier `{0,0}` (1 example, absence detection)




