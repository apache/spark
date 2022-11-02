# Connecting to Spark Connect using Clients

From the client perspective, Spark Connect mostly behaves as any other GRPC
client and can be configured as such. However, to make it easy to use from
different programming languages and to have a homogenous connection surface
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
cannot contain arbitrary characters. Configuration parameter are passed in the 
style of the HTTP URL Path Parameter Syntax. This is similar to the JDBC connection
strings. The path component must be empty.

```shell
sc://hostname:port/;param1=value;param2=value
```

<table>
  <tr>
    <td>Parameter</td>
    <td>Type</td>
    <td>Description</td>
    <td>Examples</td>
  </tr>
  <tr>
    <td>hostname</td>
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
    <td>The portname to be used when connecting to the GRPC endpoint. The
    default values is: <b>15002</b>. Any valid port number can be used.</td>
    <td><pre>15002</pre><pre>443</pre></td>
  </tr>
  <tr>
    <td>token</td>
    <td>String</td>
    <td>When this param is set in the URL, it will enable standard
    bearer token authentication using GRPC. By default this value is not set.</td>
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
    This is necssary for the appropriate Spark Session management. This is an
    *optional* parameter and depending on the deployment this parameter might
    be automatically injected using other means.</td>
    <td>
    <pre>user_id=Martin</pre>
    </td>
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

### Invalid Examples

As mentioned above, Spark Connect uses a regular GRPC client and the server path
cannot be configured to remain compatible with the GRPC standard and HTTP. For
example the following examles are invalid.

```python
server_url = "sc://myhost.com:443/mypathprefix/;token=AAAAAAA"
```

