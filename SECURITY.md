# Security policy

## Reporting a vulnerability

To report a security vulnerability in Apache Spark, follow the ASF security process at <https://www.apache.org/security/>. Send reports privately to the ASF Security Team at [security@apache.org](mailto:security@apache.org?subject=%5BSECURITY%5D%20Spark).

Please send one plain-text, unencrypted email per vulnerability, and describe the issue in the **message body** rather than as an image, HTML, or PDF attachment. Do not open a public GitHub issue or pull request for a security vulnerability, and do not disclose it publicly until the project has responded.

## Threat model

Apache Spark's security guidance for operators (covering authentication, encryption, access control, Kerberos integration, trust boundaries, environmental assumptions, security properties provided and not provided, and downstream operator responsibilities) is documented at:

<https://spark.apache.org/docs/latest/security.html>

Automated security scanning agents should consult that document for the project's in-scope / out-of-scope declarations before reporting issues.

## Known non-findings

Two categories of reports are rejected outright.

**Unconfigured deployments.** Like much infrastructure software, Spark is not meant to be deployed as it ships. Its security features are opt-in: as the guidance above puts it, "none are secure by default", and evaluating the environment and securing the deployment is the operator's responsibility. A report whose premise is a deployment that does not follow that guidance is **out of scope**.

**Spark executing the code it was given.** Spark is, by design, remote code execution as a service: submitting a job means asking a cluster to run user-supplied code on the driver and the executors. The ability of an **authorized submitter** to run arbitrary code, read local files, or start processes on cluster nodes is therefore the product working as intended. A report that only demonstrates that Spark executes remote code, without breaking a security property Spark claims to provide, is **out of scope**.

A report is in scope when it crosses a boundary the security documentation says Spark enforces, for example, code execution by a party that never authenticated to a properly configured deployment, or escalation past the authentication and authorization controls Spark does provide.
