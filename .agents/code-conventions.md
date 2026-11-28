# Code Conventions

When adding a member to an existing class or object, follow the sectioning the file already uses and put the new member with the code it belongs with. The common failure mode is dropping it wherever it is first used without checking how the file is organized, splitting a section of unrelated code in the process. Beyond grouping related code there is no prescribed order -- the Databricks Scala guide, which Spark follows, asks only that a long class group its members into logical sections with comment headers. Do not reorganize existing members unless the change requires it.

Avoid introducing non-ASCII characters in code or comments. String literals may contain non-ASCII when the content requires it (error messages, test data, etc.). Identifiers are ASCII by convention. The common failure mode is typographic characters (em-dash, smart quotes, ellipsis, non-breaking space) sneaking into comments; scalastyle flags some of these. Spot-check before committing: `grep -rn -P "[^\x00-\x7F]" <files>`.

Keep source lines within 100 characters — the linters enforce this for Scala, Java, and Python, and LLMs commonly overrun it in comments and long expressions. A quick scan of just the changed files catches most cases in seconds, far cheaper than a CI round trip:

    { git diff --name-only --diff-filter=ACM HEAD; git ls-files --others --exclude-standard; } \
      | grep -E '\.(scala|java|py)$' | sort -u \
      | xargs -r awk 'length>100 && $0 !~ /^[[:space:]]*(import|package) / && $0 !~ /https?:\/\// \
          {print FILENAME":"FNR": "length" chars"}'

This is only a hint: it approximates the linters' exemptions (imports, URLs) rather than matching them exactly, so it can over- or under-report. The linters remain the source of truth.

Keep Scala and Java binary compatibility, including Scala `private[spark]` and other `private[...]` members of public classes, which compile to public bytecode. Two checks in CI enforce it: `dev/mima`, with exclusions in `project/MimaExcludes.scala`, and `dev/connect-jvm-client-mima-check`, which compares the Connect Scala client with the `sql` module in both directions and keeps its exclusions in `CheckConnectJvmClientCompatibility.scala`. When a check flags an intended change, add an exclusion with a `// [SPARK-xxxxx]` comment to that check's file.
