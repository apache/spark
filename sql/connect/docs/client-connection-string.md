# Connecting to Spark Connect using Clients

From the client perspective, Spark Connect mostly behaves as any other GRPC
client and can be configured as such. However, to make it easy to use from
different programming languages and to have a homogeneous connection surface
this document proposes what the user surface is for connecting to a
Spark Connect endpoint.

## Background
Similar to JDBC or other database connections, Spark Connect leverages a
connection string that contains the relevant parameters that are interpreted
to connect to the Spark Connect endpoint


## Connection String

Generally, the connection string follows the standard URI definitions. The URI
scheme is fixed and set to `sc://`. The full URI has to be a 
[valid URI](http://www.faqs.org/rfcs/rfc2396.html) and must
be parsed properly by most systems. For example, hostnames have to be valid and
cannot contain arbitrary characters. Configuration parameters are passed in the 
style of the HTTP URL Path Parameter Syntax. This is similar to the JDBC connection
strings. The path component must be empty. All parameters are interpreted **case sensitive**.

```text
sc://host:port/;param1=value;param2=value
```

<table>
  <tr>
    <td>Parameter</td>
    <td>Type</td>
    <td>Description</td>
    <td>Examples</td>
  </tr>
  <tr>
    <td>host</td>
    <td>String</td>
    <td>
      The hostname of the endpoint for Spark Connect. Since the endpoint
      has to be a fully GRPC compatible endpoint a particular path cannot
      be specified. The hostname must be fully qualified or can be an IP
      address as well.
    </td>
    <td>
      <pre>myexample.com</pre>
      <pre>127.0.0.1</pre>
    </td>
  </tr>
  <tr>
    <td>port</td>
    <td>Numeric</td>
    <td>The port to be used when connecting to the GRPC endpoint. The
    default value is: <b>15002</b>. Any valid port number can be used.</td>
    <td><pre>15002</pre><pre>443</pre></td>
  </tr>
  <tr>
    <td>token</td>
    <td>String</td>
    <td>When this param is set in the URL, it will enable standard
    bearer token authentication using GRPC. By default this value is not set.
    Setting this value enables SSL.</td>
    <td><pre>token=ABCDEFGH</pre></td>
  </tr>
  <tr>
    <td>use_ssl</td>
    <td>Boolean</td>
    <td>When this flag is set, will by default connect to the endpoint
    using TLS. The assumption is that the necessary certificates to verify
    the server certificates are available in the system. The default
    value is <b>false</b></td>
    <td><pre>use_ssl=true</pre><pre>use_ssl=false</pre></td>
  </tr>
  <tr>
    <td>user_id</td>
    <td>String</td>
    <td>User ID to automatically set in the Spark Connect UserContext message.
    This is necessary for the appropriate Spark Session management. This is an
    *optional* parameter and depending on the deployment this parameter might
    be automatically injected using other means.</td>
    <td>
    <pre>user_id=Martin</pre>
    </td>
  </tr>
  <tr>
    <td>user_agent</td>
    <td>String</td>
    <td>The user agent acting on behalf of the user, typically applications
    that use Spark Connect to implement its functionality and execute Spark
    requests on behalf of the user.<br/>
    <i>Default: </i><pre>_SPARK_CONNECT_PYTHON</pre> in the Python client</td>
    <td><pre>user_agent=my_data_query_app</pre></td>
  </tr>
  <tr>
    <td>session_id</td>
    <td>String</td>
    <td>In addition to the user ID, the cache of Spark Sessions in the Spark Connect
    server uses a session ID as the cache key. This option in the connection string
    allows to provide this session ID to allow sharing Spark Sessions for the same users
    for example across multiple languages. The value must be provided in a valid UUID 
    string format.<br/>
    <i>Default: </i><pre>A UUID generated randomly</pre></td>
    <td><pre>session_id=550e8400-e29b-41d4-a716-446655440000</pre></td>
  </tr>
  <tr>
    <td>grpc_max_message_size</td>
    <td>Numeric</td>
    <td>Maximum message size allowed for gRPC messages in bytes.<br/>
    <i>Default: </i><pre> 128 * 1024 * 1024</pre></td>
    <td><pre>grpc_max_message_size=134217728</pre></td>
  </tr>
  <tr>
    <td>grpc_keepalive_enabled</td>
    <td>Boolean</td>
    <td>Whether the client sends gRPC/HTTP2 keepalive PINGs to detect a silently-dead
    connection (e.g. a NAT gateway or load balancer dropping an idle connection mapping
    without closing the socket), so a blocked call fails with an error instead of hanging
    forever. Can be turned off as an escape hatch, e.g. in environments prone to long stalls
    (GC pauses, etc.) that could otherwise trip a false-positive disconnect.<br/>
    <i>Default: </i><pre>true</pre></td>
    <td><pre>grpc_keepalive_enabled=false</pre></td>
  </tr>
  <tr>
    <td>grpc_keepalive_time_ms</td>
    <td>Numeric</td>
    <td>Idle time (in milliseconds) before the client sends a keepalive PING. A Spark Connect
    server tolerates client PINGs no more often than every 10s; setting this below that floor
    will get the connection torn down as "too_many_pings".<br/>
    <i>Default: </i><pre>60000</pre></td>
    <td><pre>grpc_keepalive_time_ms=30000</pre></td>
  </tr>
  <tr>
    <td>grpc_keepalive_timeout_ms</td>
    <td>Numeric</td>
    <td>Time (in milliseconds) the client waits for a keepalive PING ack before considering
    the connection dead.<br/>
    <i>Default: </i><pre>20000</pre></td>
    <td><pre>grpc_keepalive_timeout_ms=10000</pre></td>
  </tr>
  <tr>
    <td>grpc_keepalive_without_calls</td>
    <td>Boolean</td>
    <td>Whether to keep sending keepalive PINGs when there are no in-flight RPCs on the
    connection.<br/>
    <i>Default: </i><pre>true</pre></td>
    <td><pre>grpc_keepalive_without_calls=false</pre></td>
  </tr>
</table>

## Examples

### Valid Examples
Below we capture valid configuration examples, explaining how the connection string
will be used when configuring the Spark Connect client.

The below example connects to port **`15002`** on **myhost.com**.
```python
server_url = "sc://myhost.com/"
```

The next example configures the connection to use a different port with SSL.

```python
server_url = "sc://myhost.com:443/;use_ssl=true"
```

```python
server_url = "sc://myhost.com:443/;use_ssl=true;token=ABCDEFG"
```

The next example tunes the gRPC keepalive settings, e.g. to detect a dead connection faster
than the 60s/20s default, or to disable it entirely.

```python
server_url = "sc://myhost.com:443/;grpc_keepalive_time_ms=30000;grpc_keepalive_timeout_ms=10000"
```

```python
server_url = "sc://myhost.com:443/;grpc_keepalive_enabled=false"
```

### Invalid Examples

As mentioned above, Spark Connect uses a regular GRPC client and the server path
cannot be configured to remain compatible with the GRPC standard and HTTP. For
example the following examples are invalid.

```python
server_url = "sc://myhost.com:443/mypathprefix/;token=AAAAAAA"
```

