# Security policy

## Reporting a vulnerability

To report a security vulnerability in Apache Spark, follow the ASF security process at <https://www.apache.org/security/>. Reports can be sent privately to security@apache.org or to private@spark.apache.org.

## Threat model

Apache Spark's security guidance for operators — covering authentication, encryption, access control, Kerberos integration, trust boundaries, environmental assumptions, security properties provided and not provided, and downstream operator responsibilities — is documented at:

<https://spark.apache.org/docs/latest/security.html>

Automated security scanning agents should consult that document for the project's in-scope / out-of-scope declarations before reporting issues.

## Executor driver-instance identity check

Since Spark 4.4, the driver checks a non-secret instance ID before returning
bootstrap credentials and accepting executor registration. This prevents stale
executors from attaching to another driver after RPC address reuse, even when
application IDs match. The ID does not authenticate malicious callers; RPC
authentication and cluster isolation remain necessary.

External executor launchers must follow the
[driver-instance identity migration instructions](docs/core-migration-guide.md#upgrading-from-core-43-to-44).
