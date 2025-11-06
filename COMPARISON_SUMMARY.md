# Identifier-Lite Implementation: Regression Check Summary

## Files Generated

1. **identifier-clause-comparison-v2.csv** - Raw CSV data with all test results
2. **identifier-clause-comparison-v2.md** - Formatted markdown table with analysis
3. **COMPARISON_SUMMARY.md** (this file) - Regression check summary

## Regression Analysis

### ✅ Result: NO REGRESSIONS FOUND

Comparing the previous version (v1) with the current version (v2):
- **Total queries compared**: 227
- **Regressions (was SUCCESS, now error)**: 0
- **Improvements (was error, now SUCCESS)**: 0
- **Unchanged**: 227 (100%)

### Test Statistics

- **Total Tests**: 227
- **Tests from Master (baseline)**: 128
- **New Tests Added**: 99
- **Tests Changed from Master**: 13 (all improvements)
- **Tests with Legacy Mode Differences**: 47 (20.7%)

## Master Comparison

### Tests Changed from Master (13 tests - all improvements):

1. **Query #114**: `SELECT row_number() OVER IDENTIFIER('x.win')...`
   - Master: `PARSE_SYNTAX_ERROR`
   - Current: `IDENTIFIER_TOO_MANY_NAME_PARTS` (better error message)

2. **Query #115**: `SELECT T1.c1 FROM... JOIN... USING (IDENTIFIER('c1'))`
   - Master: `PARSE_SYNTAX_ERROR`
   - Current: `SUCCESS` ✅

3. **Query #117**: `SELECT map('a', 1).IDENTIFIER('a')`
   - Master: `PARSE_SYNTAX_ERROR`
   - Current: `SUCCESS` ✅

4. **Query #118**: `SELECT named_struct('a', 1).IDENTIFIER('a')`
   - Master: `PARSE_SYNTAX_ERROR`
   - Current: `SUCCESS` ✅

5. **Queries #119-123**: Window specs and dereference improvements
   - Multiple queries that were failing now work or have better error messages

6. **Queries #126-130**: DDL improvements (CREATE VIEW, CREATE TABLE, INSERT with column lists)
   - These now work correctly with identifier-lite

## Known Issues

### 🐛 Unfixed Bug: `IDENTIFIER('t').c1`

**Query**: `SELECT IDENTIFIER('t').c1 FROM VALUES(1) AS T(c1)`
**Status**: Still fails with `UNRESOLVED_COLUMN.WITH_SUGGESTION`
**Expected**: Should resolve as table-qualified column reference and return `1`

**Root Cause**: 
- `IDENTIFIER_KW` is in the `nonReserved` keyword list
- Parser matches `IDENTIFIER` as a function name (via `qualifiedName` → `nonReserved`)
- Then treats `('t')` as function arguments
- Result: creates wrong AST structure

**Investigation**:
- Attempted grammar reordering: broke other tests
- Attempted adding predicates to `functionName`: didn't prevent matching via `qualifiedName`
- Needs AST-level fix or removal of `IDENTIFIER_KW` from `nonReserved` (may have side effects)

## Conclusion

✅ **Safe to proceed**: No regressions introduced
✅ **Improvements made**: 13 tests that were broken now work or have better errors
✅ **New functionality**: 99 new tests covering identifier-lite features
⚠️ **One known bug**: `IDENTIFIER('t').c1` case - documented but unfixed

The implementation is stable and provides significant improvements over master, with one edge case remaining to be fixed in future work.
