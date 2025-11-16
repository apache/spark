# Apache Spark Development Workflow

## Quick Reference for Regular Development

### Daily Sync Workflow

Before starting work each day or before pushing changes:

```bash
# 1. Fetch latest changes from upstream
git fetch upstream

# 2. Rebase your branch on latest master
git rebase upstream/master

# 3. If conflicts occur, resolve them:
#    - Edit conflicted files
#    - git add <resolved-files>
#    - git rebase --continue
#    - Repeat until done

# 4. After rebase, re-run style checks
./dev/scalastyle && ./dev/scalafmt && ./dev/lint-scala
./dev/lint-python && ./dev/reformat-python

# 5. Push to your fork (force push after rebase)
git push origin features/pyspark-streaming-admission-control-custom-sources --force-with-lease
```

### Before Every Push

The pre-push hook will automatically run these checks, but you can run them manually:

```bash
# Scala checks
./dev/scalastyle
./dev/scalafmt
./dev/lint-scala

# Python checks (if you modified python/)
./dev/lint-python
./dev/reformat-python

# Java checks (if you modified .java files)
./dev/lint-java
```

### When Opening/Updating a PR

```bash
# 1. Ensure branch is current
git fetch upstream
git rebase upstream/master

# 2. Run all checks
./dev/scalastyle && ./dev/scalafmt && ./dev/lint-scala
./dev/lint-python && ./dev/reformat-python

# 3. Run targeted tests for your changes
# Example for PySpark changes:
python/run-tests --testnames 'pyspark.sql.tests.streaming.test_your_feature'

# Or for Scala changes:
./build/mvn test -pl :spark-sql_2.13 -Dtest=none -DwildcardSuites=org.apache.spark.sql.YourTestSuite

# 4. Push to fork
git push origin your-branch-name --force-with-lease
```

### Handling Fast-Moving Upstream

Spark moves fast! Here's how to keep your PR current:

#### Strategy: Regular Rebasing (Recommended)

**When to rebase:**
- Daily if working on a long-running feature
- Before opening a PR
- Before pushing new commits to an existing PR
- When reviewers request it
- When you see upstream has changes affecting your code

**How to rebase:**

```bash
# Create a backup branch first (safety net)
git branch backup-$(date +%Y%m%d)-$(git branch --show-current)

# Fetch and rebase
git fetch upstream
git rebase upstream/master

# If conflicts:
# 1. Fix conflicts in each file
# 2. Re-run formatting: ./dev/scalafmt (for affected files)
# 3. Stage resolved files: git add <files>
# 4. Continue: git rebase --continue
# 5. Repeat until done

# If something goes wrong:
git rebase --abort
# Then check your backup branch

# Push (requires force push after rebase)
git push origin your-branch-name --force-with-lease
```

**Benefits of rebasing:**
- Clean, linear history
- Easier for reviewers
- Matches Spark's preferred workflow
- Smaller, manageable conflicts vs. one huge merge

### Common Scenarios

#### Scenario 1: Simple Rebase (No Conflicts)

```bash
$ git fetch upstream
$ git rebase upstream/master
Successfully rebased and updated refs/heads/your-branch.

$ git push origin your-branch --force-with-lease
```

#### Scenario 2: Rebase with Conflicts

```bash
$ git rebase upstream/master
CONFLICT (content): Merge conflict in sql/core/src/main/scala/YourFile.scala
error: could not apply abc1234... Your commit message

$ # Fix conflicts in YourFile.scala
$ git add sql/core/src/main/scala/YourFile.scala
$ ./dev/scalafmt  # Re-format after resolving
$ git rebase --continue

$ git push origin your-branch --force-with-lease
```

#### Scenario 3: Upstream Changed APIs You Use

```bash
$ git fetch upstream
$ git rebase upstream/master
# Conflicts in your code due to API changes

$ # Update your code to use new APIs
$ # Run tests to ensure compatibility
$ ./build/mvn test -pl your-module
$ git add <fixed-files>
$ git rebase --continue

$ git push origin your-branch --force-with-lease
```

### Git Configuration Tips

```bash
# Make rebase the default for pulls
git config pull.rebase true

# Use --force-with-lease by default (safer than --force)
git config alias.pushf 'push --force-with-lease'

# Now you can use: git pushf origin your-branch
```

### Pre-Push Hook

A pre-push hook is installed at `.git/hooks/pre-push` that automatically:
- Checks for uncommitted changes
- Warns if not synced with upstream/master
- Runs all relevant style/lint checks based on what you changed
- Blocks the push if any check fails

**To bypass the hook** (not recommended, only for emergencies):
```bash
git push --no-verify origin your-branch
```

### Key Reminders

✅ **DO:**
- Rebase frequently (smaller conflicts)
- Run checks before pushing
- Use `--force-with-lease` (safer than `--force`)
- Create backup branches before complex rebases
- Read and understand rebase conflicts before resolving

❌ **DON'T:**
- Merge upstream/master into your feature branch (use rebase)
- Use `--force` (use `--force-with-lease` instead)
- Push without running checks
- Ignore rebase conflicts and blindly accept changes
- Rebase shared branches (only rebase your own feature branches)

### Getting Help

If you're stuck:
1. Check Spark's contributing guide: https://spark.apache.org/contributing.html
2. Ask on dev@spark.apache.org mailing list
3. Check your backup branch if rebase went wrong
4. Use `git rebase --abort` to cancel a problematic rebase

### Environment Variables

```bash
# Recommended Maven options
export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"
```

### Useful Git Commands

```bash
# See what commits are in your branch but not in upstream/master
git log upstream/master..HEAD --oneline

# See what commits are in upstream/master but not in your branch
git log HEAD..upstream/master --oneline

# View diff against upstream/master
git diff upstream/master...HEAD

# Interactive rebase to clean up your commits
git rebase -i upstream/master
```

---

**Remember:** CI should confirm what you already know is green, not discover problems!

