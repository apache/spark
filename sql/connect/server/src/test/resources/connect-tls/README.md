# Spark Connect TLS/mTLS test fixtures

Static self-signed PKI used by
`org.apache.spark.sql.connect.service.SparkConnectServiceTlsSuite`.
All material is test-only; no production risk.

## Files

| File | Purpose |
|---|---|
| `cert.pem` / `key.pem` | Server cert used by the plain-TLS tests. Self-signed, imported from `common/network-common/src/test/resources/unencrypted-{certchain,key}.pem`. |
| `ca.pem` / `ca-key.pem` | Root CA used to sign the server and trusted client certs. Its cert is the sole entry in `truststore.jks`. |
| `server-cert.pem` / `server-key.pem` | Server cert signed by `ca.pem`, `CN=localhost` with `subjectAltName=DNS:localhost`. Used by the mTLS tests so the same CA anchors both the client-side server-cert check and the server-side client-cert check. |
| `server-key-encrypted.pem` | Same private key as `server-key.pem`, wrapped as PKCS#8 with AES-256-CBC, password `changeit`. Used by the `privateKeyPassword` test. |
| `client-cert.pem` / `client-key.pem` | Client cert signed by `ca.pem`, presented by the client in the happy-path mTLS test. |
| `truststore.jks` | JKS containing only `ca.pem`. Password: `changeit`. |
| `untrusted-ca.pem` | A different self-signed CA, NOT in `truststore.jks`. |
| `untrusted-client-cert.pem` / `untrusted-client-key.pem` | Client cert signed by `untrusted-ca.pem`; used in the "untrusted client cert is rejected" test. |

`cert.pem` / `key.pem` are left as-is (imported from network-common) so
the existing TLS-only tests keep working; they are independent of the
new CA introduced for mTLS.

## Regenerating

If any material expires (10-year validity from generation) or you need
different subjects, regenerate with:

```
mkdir -p /tmp/pki && cd /tmp/pki
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout ca-key.pem -out ca.pem \
  -subj "/CN=Spark Connect Test CA"

openssl req -newkey rsa:2048 -nodes -keyout client-key.pem \
  -out client.csr -subj "/CN=Spark Connect Test Client"
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 3650 -out client-cert.pem

# Server cert (signed by CA, SAN=localhost)
openssl req -newkey rsa:2048 -nodes -keyout server-key.pem \
  -out server.csr -subj "/CN=localhost"
cat > server-ext.cnf <<'EXTEOF'
subjectAltName=DNS:localhost
EXTEOF
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem \
  -CAcreateserial -days 3650 -out server-cert.pem -extfile server-ext.cnf

openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout untrusted-ca-key.pem -out untrusted-ca.pem \
  -subj "/CN=Untrusted CA"
openssl req -newkey rsa:2048 -nodes -keyout untrusted-client-key.pem \
  -out untrusted-client.csr -subj "/CN=Untrusted Client"
openssl x509 -req -in untrusted-client.csr -CA untrusted-ca.pem \
  -CAkey untrusted-ca-key.pem -CAcreateserial -days 3650 \
  -out untrusted-client-cert.pem

keytool -import -noprompt -trustcacerts \
  -alias spark-connect-test-ca -file ca.pem \
  -keystore truststore.jks -storepass changeit

# Encrypted PKCS#8 copy of server-key.pem for the privateKeyPassword test.
openssl pkcs8 -in server-key.pem -topk8 -v2 aes-256-cbc \
  -passout pass:changeit -out server-key-encrypted.pem
```

Copy the resulting `ca.pem`, `ca-key.pem`, `server-cert.pem`,
`server-key.pem`, `server-key-encrypted.pem`, `client-cert.pem`,
`client-key.pem`, `untrusted-ca.pem`, `untrusted-client-cert.pem`,
`untrusted-client-key.pem`, and `truststore.jks` into this directory.
