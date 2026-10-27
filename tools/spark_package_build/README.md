# Spark Package Builder

Builds the custom Spark binary (`spark-<ver>-bin-hadoop3-affirm.<N>.tgz`) that Kotlin/JVM DTs download through
`spark_package` in `deployable/<dt>/build-config.yaml` (see `jvm-build-tools/docker/main/Dockerfile` in all-the-things),
and optionally publishes it to Artifactory (location set by the pipeline). Modeled on `tools/pyspark_build/`
(branch `pyspark-build-tool`), which does the same for PySpark wheels.

> POC. Status and findings: Notion page "kotlin spark pipeline effort".

## Flow

```
inputs -> docker build (JDK + Maven + Artifactory mirror) -> docker run build-spark-package.sh
  -> VERSIONS.md check (numeric versions) -> target must not exist (checked before the build)
  -> clone Affirm/spark, check out --commit, verify it descends from upstream tag v<ver>
  -> make-distribution.sh --tgz -> (--upload) PUT with SHA-256 -> download back and compare
```

Published names are **permanent**: the Artifactory users we tested can create files but not delete them, and
overwriting probably needs the same permission. Use a new `--affirm-version` for every publish.

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | JDK (`JAVA_VERSION`, default 8), Maven (`MAVEN_VERSION`, default 3.9.9, checksum verified), git, curl; bakes in `settings.xml` and `VERSIONS.md` |
| `settings.xml` | Optional Maven mirror, URL taken from `MAVEN_MIRROR_URL` at run time (Maven Central rate-limits, Scala downloads were refused) |
| `build-spark-package.sh` | Entry point: validate, clone, build, optionally upload and verify |
| `VERSIONS.md` | Productive (integer) releases, each with its commit SHA |

## Inputs

| Argument | Required | Description |
|----------|----------|-------------|
| `--spark-version` | yes | Upstream version, e.g. `3.5.1`. The commit must descend from tag `v<version>`. |
| `--affirm-version` | yes | Suffix: `dev2`, `6`, ... Integers must be in `VERSIONS.md` as `## <spark-version>-affirm.<N>` with a matching `Commit SHA`. |
| `--commit` | yes | Full SHA in Affirm/spark, checked out as-is (not applied as a diff). |
| `--mvn-flags` | no | Default 3.x: `-Denforcer.skip -Pmesos -Pkubernetes -Pscala-2.12 -Phadoop-3 -Phive -Phive-thriftserver` (flags of `affirm.2`). Default 4.x: `-Pkubernetes -Phadoop-3 -Phive-thriftserver`. |
| `--output-dir` | no | Default `/build/dist`. |
| `--upload` | no | Publish. Without it the script only builds. |

Java: 8 for Spark 3.5.x (a Java 11 build produced `InvalidClassException: SerializableBuffer` at runtime), 21 for Spark 4.x.

### Environment

| Variable | Required | Description |
|----------|----------|-------------|
| `GIT_USER_EMAIL`, `GIT_USER_NAME` | yes | Git identity inside the container |
| `GITHUB_TOKEN` | no | Via `GIT_ASKPASS`; `Affirm/spark` is public, so not needed for the clone |
| `ARTIFACTORY_TOKEN` | with `--upload` | Sent through a curl config on stdin, never as an argument |
| `ARTIFACTORY_USER` | no | If set, basic auth (user + token as password); otherwise the token is a bearer token |
| `ARTIFACTORY_BASE`, `TARGET_REPO_PATH` | with `--upload` | Artifactory base URL and repository path to publish under. No defaults in this repo; the pipeline sets them. |
| `MAVEN_MIRROR_URL` | no | Maven mirror for all downloads; recommended in CI |

## Buildkite (the normal way)

The steps, input fields and secrets live in the Buildkite pipeline settings, not in this repo (same as the PySpark
pipeline). The pipeline builds this folder's image, runs it with the three inputs, and injects the secrets and the
Artifactory and Maven mirror URLs as environment variables. Pipeline setup notes are kept internally.

## Run the image by hand (debugging, or before the pipeline exists)

Any x86_64 Linux Docker host with network access to Artifactory works (not an Apple Silicon laptop: Spark's build is slow and error-prone under emulation).

```bash
git clone --branch <branch> https://github.com/Affirm/spark.git spark-package-build
cd spark-package-build/tools/spark_package_build

docker build --build-arg JAVA_VERSION=8 -t spark-package-builder:java8 .

# Dry run (no upload). ~30 min.
mkdir -p /tmp/spark-dist
docker run --rm -v /tmp/spark-dist:/build/dist \
  -e GIT_USER_EMAIL="you@affirm.com" -e GIT_USER_NAME="you" \
  spark-package-builder:java8 \
  --spark-version 3.5.1 --affirm-version dev2 \
  --commit e7ae8e714cd82916d563650b315e70fc9d11f9d2
```

Publish (only when the dry run matches the expected result): read the token into your shell, then pass it by name.

```bash
read -rs -p "Artifactory token: " ARTIFACTORY_TOKEN; export ARTIFACTORY_TOKEN; echo
export ARTIFACTORY_BASE=<artifactory-base-url> TARGET_REPO_PATH=<repository-path>
docker run --rm -v /tmp/spark-dist:/build/dist \
  -e GIT_USER_EMAIL="you@affirm.com" -e GIT_USER_NAME="you" -e ARTIFACTORY_TOKEN -e ARTIFACTORY_BASE -e TARGET_REPO_PATH \
  spark-package-builder:java8 \
  --spark-version 3.5.1 --affirm-version dev2 \
  --commit e7ae8e714cd82916d563650b315e70fc9d11f9d2 --upload
unset ARTIFACTORY_TOKEN
```

## Use the package

Set `spark_package: spark-3.5.1-bin-hadoop3-affirm.dev2.tgz` in `deployable/<dt>/build-config.yaml` and rebuild the DT image.
A Spark 4 package additionally needs a Java 21 base image and a Scala 2.13 build of the Kotlin Spark library
(`libs/jvm/core/spark/`); a Spark 3.5 driver cannot talk to Spark 4 executors.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (bad arguments, `VERSIONS.md` check, clone or checkout, commit not descended from the tag) |
| 2 | Maven build failed |
| 3 | Expected package was not produced |
| 4 | Upload or post-upload verification failed (missing token, HTTP error, checksum mismatch) |
| 5 | Target already exists in Artifactory |

