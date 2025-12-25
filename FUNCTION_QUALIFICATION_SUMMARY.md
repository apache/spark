# Function Qualification - Executive Summary (Approach 2)

**Status**: Design document for implementing qualified names for Spark functions
**Approach**: Two Separate Registries (Recommended)

## Quick Overview

### Problem
1. Temporary functions currently overwrite builtins in the cloned session registry
2. No way to access shadowed builtin functions
3. Memory waste: each session clones 400+ builtin functions

### Solution (Approach 2: Separate Registries)
1. **Stop cloning** - don't copy builtin registry per session
2. **Two registries**:
   - `FunctionRegistry.builtin` (global, immutable, shared)
   - `session.tempFunctionRegistry` (per-session, starts empty)
3. **Qualification** determines which registry to check:
   - `builtin.abs()` → check builtin registry only
   - `session.my_func()` → check temp registry only
   - `abs()` → check temp first, then builtin (shadowing)

### Benefits
- **98% memory reduction**: No cloning = massive savings
- **Solves shadowing**: Can always access builtin via qualification
- **Clean architecture**: Separate storage for separate concerns
- **PATH ready**: Natural foundation for future PATH feature

### Example
```sql
-- Session starts with empty temp registry
CREATE TEMPORARY FUNCTION abs AS 'MyCustomAbs';

SELECT abs(-5);                -- Temp (shadows builtin)
SELECT builtin.abs(-5);        -- Builtin (bypass shadow)
SELECT session.abs(-5);        -- Temp (explicit)
```

### Implementation Effort
- **Core changes**: SessionCatalog, BaseSessionStateBuilder, FunctionResolution
- **Estimated time**: 2-3 weeks
- **Files affected**: ~10 files
- **Risk**: Medium (architectural change but well-scoped)

### Memory Savings Calculation
```
Before: 400 builtins × 100 sessions = 40,000 function entries
After:  400 builtins (global) + (5 temps × 100 sessions) = 900 entries
Savings: 98% reduction
```

## Full Analysis

See `FUNCTION_QUALIFICATION_ANALYSIS.md` for complete details including:
- Current architecture analysis
- Detailed design with code examples
- Implementation phases
- Test cases
- Comparison with Approach 1 (minimal change with cloning)

---

Generated: December 2025
