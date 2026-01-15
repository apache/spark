# Cursor Test Suite Migration Status

## Goal
Standardize on Scala test suites over SQL script tests for cursor functionality.

## Current Status

### ✅ Phase 1: Create Working Scala Test Suite (COMPLETE)
- Created `SqlScriptingCursorSuite.scala` with 16 comprehensive tests
- All 16 tests **PASSING** ✅
- Covers core cursor functionality:
  - Feature enabled/disabled toggle
  - Basic cursor lifecycle (DECLARE, OPEN, FETCH, CLOSE)
  - Cursor state transitions (cannot open twice, can reopen after close)
  - Cursor with no rows / empty result set
  - CURSOR_NO_MORE_ROWS as completion condition (SQLSTATE 02000)
  - NOT FOUND handler for cursor exhaustion
  - EXIT HANDLER behavior (exits block before VALUES)
  - Parameterized cursors with USING clause
  - Multiple cursors in sequence
  - Multiple cursors in same scope
  - FETCH INTO STRUCT variables
  - CONTINUE HANDLER with REPEAT loops
  - Cross-frame cursor access (handler accessing outer cursor)
  - Cursor iteration with REPEAT and NOT FOUND handler
  - Declaration order validation

### 📋 Phase 2: Expand to Full Coverage (TODO)
Current SQL test suite has **86 tests total**. Scala suite has **16 tests** (19% coverage).

**Remaining 70 tests to add:**

#### Category: Scoping & Naming (Tests 1a-1c, 8a-10)
- [ ] Test 1a: Cursor/variable same name (separate namespaces)
- [ ] Test 1c: Inner scope cursors shadow outer cursors
- [ ] Test 8a-8d: Label qualification (`label.cursor`)
- [ ] Test 9: Qualified cursor in nested scopes
- [ ] Test 10: Invalid qualifier (too many dots)
- [ ] Test 69: Cursor shadowing in handlers

#### Category: Cursor State & Lifecycle (Tests 2a-3, 32-33)
- [x] Test 2a: Cannot open cursor twice ✅
- [x] Test 2b: Can reopen after close ✅
- [ ] Test 2c-2h: State transition validations
- [ ] Test 3: REPEAT loop with cursor + CONTINUE HANDLER
- [ ] Test 32: Cursor close without open
- [ ] Test 33: Cursor fetch without open

####Category: Parameterized Cursors (Tests 4-7, 41-52)
- [x] Test 4: Basic parameterized cursor ✅
- [ ] Test 5: Named parameters
- [ ] Test 6a-6d: Parameter count mismatches
- [ ] Test 7a: Multiple opens with different parameters
- [ ] Test 41-52: Parameter marker edge cases

#### Category: FETCH INTO Validation (Tests 11-14, 22-26, 27-31)
- [ ] Test 11: Variable count mismatch (too few)
- [ ] Test 12: Variable count mismatch (too many)
- [ ] Test 13: Duplicate target variables
- [ ] Test 14: Non-existent variable
- [x] Test 22-26: STRUCT variables ✅
- [ ] Test 27-31: Session variables

#### Category: Optional Keywords & Syntax (Tests 15-21)
- [ ] Test 15-17: ASENSITIVE vs INSENSITIVE
- [ ] Test 18: FOR READ ONLY clause
- [ ] Test 19-21: FETCH variations (NEXT FROM, FROM, bare cursor)

#### Category: Exception Handling (Tests 34-39, 57-66)
- [x] Test 34: Unhandled CURSOR_NO_MORE_ROWS continues execution ✅
- [x] Test 35: NOT FOUND handler catches CURSOR_NO_MORE_ROWS ✅
- [x] Test 36: EXIT HANDLER for NOT FOUND ✅
- [ ] Test 37a-37b: SQLSTATE handlers (specific vs generic)
- [ ] Test 38-39: Multiple handlers for same condition
- [ ] Test 57-66: Complex handler scenarios with cursors

#### Category: Complex Queries (Test 40)
- [x] Test 40: Multiple cursors in sequence ✅

#### Category: IDENTIFIER() Clause (Tests 36-37b)
- [ ] Test 36-37b: IDENTIFIER() syntax for cursor names

#### Category: Case Sensitivity (Tests 53-56)
- [ ] Test 53-56: Case-insensitive cursor names and labels

#### Category: Declaration Order (Tests 67-68)
- [x] Test 67: Cursor before variable (invalid) ✅
- [ ] Test 68: Cursor after handler (invalid)

### 🎯 Phase 3: Remove SQL Test Suite (BLOCKED - waiting for Phase 2)
Once Phase 2 is complete and Scala suite is a superset:
- [ ] Verify all 86 tests passing in Scala suite
- [ ] Delete `cursors.sql`
- [ ] Delete `cursors.sql.out`
- [ ] Update any documentation/CI references

## Implementation Strategy

### Approach 1: Manual Incremental Addition
- Add 10-15 tests at a time
- Run tests after each batch
- Fix any failures before continuing
- **Estimated time:** 4-6 iterations

### Approach 2: Category-by-Category
- Implement one category at a time (e.g., all scoping tests)
- Ensures logical grouping and easier debugging
- **Estimated time:** 5-7 categories

### Approach 3: Golden File Reference
- For each test in `cursors.sql`, read the expected output from `cursors.sql.out`
- Generate Scala test with inline expectations
- **Estimated time:** Systematic but tedious

## Key Learnings from Phase 1

1. **Error Conditions:**
   - `UNSUPPORTED_FEATURE.SQL_CURSOR` = config disabled
   - `CURSOR_ALREADY_OPEN` = attempt to open already-open cursor
   - `CURSOR_NOT_OPEN` = attempt to fetch/close unopened cursor
   - `CURSOR_NO_MORE_ROWS` (SQLSTATE 02000) = completion condition, NOT error

2. **Declaration Order:**
   - Variables → Conditions → Cursors → Handlers → Statements
   - Violations throw `INVALID_VARIABLE_DECLARATION` or `INVALID_HANDLER_DECLARATION`

3. **Script Return Behavior:**
   - Scripts return **only the last** SELECT/VALUES result
   - Intermediate results are not returned

4. **Error Message Format:**
   - Cursor names in errors use `toSQLId()` for backticks

5. **REPEAT Loop Behavior:**
   - After CONTINUE HANDLER, loop continues but may exit if condition is met

## Next Steps

**Option A (Recommended):** Continue manual addition of remaining 70 tests in batches
**Option B:** Document current coverage and defer full migration
**Option C:** Use the SQL test suite as the primary source of truth for now

**User Decision Required:** Which approach to take for Phase 2?
