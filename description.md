[SPARK-61021][BUILD] Add a proper test suite for `dev/sparktestsupport`

### What changes were proposed in this pull request?

Add a standard `unittest` suite for `sparktestsupport`, automatically discover its doctests, and run both in the CI precondition job before module selection. Keep the suite available through `dev/run-tests.py` as well.

The tests cover behavior that was previously buried in long doctests or comments, including module dependency invariants and cases like ensuring streaming changes do not trigger external Kinesis tests. Short usage examples remain doctests.

### Why are the changes needed?

`sparktestsupport` drives CI test selection, but relied mostly on doctests. A separate suite makes regression and invariant tests easier to read and maintain, and ensures CI checks this build infrastructure before relying on it.

### Does this PR introduce _any_ user-facing change?

No. These changes affect developer and CI infrastructure only.

### How was this patch tested?

- All 23 tests, including doctests, pass on Python 3.11–3.15 and through `dev/run-tests.py`.
- Ruff checks and formatting checks pass.

### Was this patch authored or co-authored using generative AI tooling?

Generated-by: GitHub Copilot
