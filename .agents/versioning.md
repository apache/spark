# Versioning and Branch Policy

When a change needs a version — `@since` annotations, config `.version("...")` (`SQLConf` / `*Conf`), `MimaExcludes` entries, etc. — use the version of the branch it first ships in, with `-SNAPSHOT` stripped. Determine that branch:

- **PR opened against a non-`master` base branch** (e.g. a maintenance line like `branch-4.2`): use that base branch's version -- when you're checked out on it, that's just the working tree's `pom.xml`. The helper below covers only the common `master`-base case.
- **PR opened against `master`:** most PRs merge to **both** `master` and the latest `branch-<N>.x` (the branch for the next feature release, e.g. `branch-4.x`), so use the `branch-<N>.x` version. The exception is **master-only** changes — use `master`'s version — which are only:
  - breaking / binary-incompatible changes that can't ship in a minor release;
  - dependency upgrades that don't fix a critical issue worth backporting.

  A MiMa exclusion alone does not make a change master-only: most are for changes a minor release may ship, such as a new abstract method in a public trait or abstract class, or a removed internal member, so they go in the `branch-<N>.x` version's section.

Do **not** just read `master`'s version: a normally-backported PR ships first in `branch-<N>.x`, whose version is lower than `master`'s. If unsure whether a change is master-only, ask the user.

`dev/next_version_candidates.py` (no arguments) prints both candidate versions, reading from the local `apache/spark` remote (the `upstream` configured during pre-flight). It reports the mechanical facts only -- choosing between them per the rules above is the judgement call (the numbers below are illustrative and advance over time):

    $ dev/next_version_candidates.py
    master       5.0.0
    branch-4.x   4.3.0
