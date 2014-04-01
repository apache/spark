Hadoop Auth, Java HTTP SPNEGO

Hadoop Auth consists of a client and a server
components to enable Kerberos SPNEGO authentication for HTTP.

The client component is the AuthenticatedURL class.

The server component is the AuthenticationFilter servlet filter class.

Authentication mechanisms support is pluggable in both the client and
the server components via interfaces.

In addition to Kerberos SPNEGO, Hadoop Auth also supports Pseudo/Simple
authentication (trusting the value of the query string parameter
'user.name').
