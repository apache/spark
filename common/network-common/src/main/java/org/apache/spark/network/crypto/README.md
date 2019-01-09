Spark Auth Protocol and AES Encryption Support
==============================================

This file describes an auth protocol used by Spark as a more secure alternative to DIGEST-MD5. This
protocol is built on symmetric key encryption, based on the assumption that the two endpoints being
authenticated share a common secret, which is how Spark authentication currently works. The protocol
provides mutual authentication, meaning that after the negotiation both parties know that the remote
side knows the shared secret. The protocol is influenced by the ISO/IEC 9798 protocol, although it's
not an implementation of it.

This protocol could be replaced with TLS PSK, except no PSK ciphers are available in the currently
released JREs.

The protocol aims at solving the following shortcomings in Spark's current usage of DIGEST-MD5:

- MD5 is an aging hash algorithm with known weaknesses, and a more secure alternative is desired.
- DIGEST-MD5 has a pre-defined set of ciphers for which it can generate keys. The only
  viable, supported cipher these days is 3DES, and a more modern alternative is desired.
- Encrypting AES session keys with 3DES doesn't solve the issue, since the weakest link
  in the negotiation would still be MD5 and 3DES.

The protocol assumes that the shared secret is generated and distributed in a secure manner.

The protocol always negotiates encryption keys. If encryption is not desired, the existing
SASL-based authentication, or no authentication at all, can be chosen instead.

When messages are described below, it's expected that the implementation should support
arbitrary sizes for fields that don't have a fixed size.

Client Challenge
----------------

The auth negotiation is started by the client. The client starts by generating an encryption
key based on the application's shared secret, and a nonce.

    KEY = KDF(SECRET, SALT, KEY_LENGTH)

Where:
- KDF(): a key derivation function that takes a secret, a salt, a configurable number of
  iterations, and a configurable key length.
- SALT: a byte sequence used to salt the key derivation function.
- KEY_LENGTH: length of the encryption key to generate.


The client generates a message with the following content:

    CLIENT_CHALLENGE = (
        APP_ID,
        KDF,
        ITERATIONS,
        CIPHER,
        KEY_LENGTH,
        ANONCE,
        ENC(APP_ID || ANONCE || CHALLENGE))

Where:

- APP_ID: the application ID which the server uses to identify the shared secret.
- KDF: the key derivation function described above.
- ITERATIONS: number of iterations to run the KDF when generating keys.
- CIPHER: the cipher used to encrypt data.
- KEY_LENGTH: length of the encryption keys to generate, in bits.
- ANONCE: the nonce used as the salt when generating the auth key.
- ENC(): an encryption function that uses the cipher and the generated key. This function
  will also be used in the definition of other messages below.
- CHALLENGE: a byte sequence used as a challenge to the server.
- ||: concatenation operator.

When strings are used where byte arrays are expected, the UTF-8 representation of the string
is assumed.

To respond to the challenge, the server should consider the byte array as representing an
arbitrary-length integer, and respond with the value of the integer plus one.


Server Response And Challenge
-----------------------------

Once the client challenge is received, the server will generate the same auth key by
using the same algorithm the client has used. It will then verify the client challenge:
if the APP_ID and ANONCE fields match, the server knows that the client has the shared
secret. The server then creates a response to the client challenge, to prove that it also
has the secret key, and provides parameters to be used when creating the session key.

The following describes the response from the server:

    SERVER_CHALLENGE = (
        ENC(APP_ID || ANONCE || RESPONSE),
        ENC(SNONCE),
        ENC(INIV),
        ENC(OUTIV))

Where:

- RESPONSE: the server's response to the client challenge.
- SNONCE: a nonce to be used as salt when generating the session key.
- INIV: initialization vector used to initialize the input channel of the client.
- OUTIV: initialization vector used to initialize the output channel of the client.

At this point the server considers the client to be authenticated, and will try to
decrypt any data further sent by the client using the session key.


Default Algorithms
------------------

Configuration options are available for the KDF and cipher algorithms to use.

The default KDF is "PBKDF2WithHmacSHA1". Users should be able to select any algorithm
from those supported by the `javax.crypto.SecretKeyFactory` class, as long as they support
PBEKeySpec when generating keys. The default number of iterations was chosen to take a
reasonable amount of time on modern CPUs. See the documentation in TransportConf for more
details.

The default cipher algorithm is "AES/CTR/NoPadding". Users should be able to select any
algorithm supported by the commons-crypto library. It should allow the cipher to operate
in stream mode.

The default key length is 128 (bits).


Implementation Details
----------------------

The commons-crypto library currently only supports AES ciphers, and requires an initialization
vector (IV). This first version of the protocol does not explicitly include the IV in the client
challenge message. Instead, the IV should be derived from the nonce, including the needed bytes, and
padding the IV with zeroes in case the nonce is not long enough.

Future versions of the protocol might add support for new ciphers and explicitly include needed
configuration parameters in the messages.


Threat Assessment
-----------------

The protocol is secure against different forms of attack:

* Eavesdropping: the protocol is built on the assumption that it's computationally infeasible
  to calculate the original secret from the encrypted messages. Neither the secret nor any
  encryption keys are transmitted on the wire, encrypted or not.

* Man-in-the-middle: because the protocol performs mutual authentication, both ends need to
  know the shared secret to be able to decrypt session data. Even if an attacker is able to insert a
  malicious "proxy" between endpoints, the attacker won't be able to read any of the data exchanged
  between client and server, nor insert arbitrary commands for the server to execute.

* Replay attacks: the use of nonces when generating keys prevents an attacker from being able to
  just replay messages sniffed from the communication channel.

An attacker may replay the client challenge and successfully "prove" to a server that it "knows" the
shared secret. But the attacker won't be able to decrypt the server's response, and thus won't be
able to generate a session key, which will make it hard to craft a valid, encrypted message that the
server will be able to understand. This will cause the server to close the connection as soon as the
attacker tries to send any command to the server. The attacker can just hold the channel open for
some time, which will be closed when the server times out the channel. These issues could be
separately mitigated by adding a shorter timeout for the first message after authentication, and
potentially by adding host blacklists if a possible attack is detected from a particular host.
