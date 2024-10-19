Forward Secure Auth Protocol v2.0
==============================================

Summary
-------

This file describes a forward secure authentication protocol which may be used by Spark. This
protocol is essentially ephemeral Diffie-Hellman key exchange using Curve25519, referred to as
X25519.

Both client and server share a (possibly low-entropy) pre-shared secret that is used to derive a
key-encrypting key using HKDF. This will mix in any preceding protocol transcript.

The key-encrypting key is used to encrypt an X25519 public key with AES-GCM. This is intended to
authenticate the message exchange between the parties and there is no expectation of secrecy for
the public key. This protocol utilizes GCM's associated authenticated data (AAD) field to include
metadata and the prior protocol transcript, to bind each round with all preceding rounds.

Client Challenge
----------------

The auth negotiation is started by the client. Given an application ID, the client starts by
generating a random 16-byte salt value and deriving a key encryption key:

    preSharedKey = lookupKey(appId)
    nonSecretSalt = Random(16 bytes)
    aadState = Concat(appId, nonSecretSalt)
    keyEncryptingKey = HKDF(preSharedKey, nonSecretSalt, aadState)

This key encryption key is then used to encrypt an ephemeral X25519 public key.

    clientKeyPair = X25519.generate()
    randomIV = Random(16 bytes)
    ciphertext = AES-GCM-Encrypt(
      key = keyEncryptingKey,
      iv = randomIV,
      plaintext = clientKeyPair.publicKey(),
      aad = aadState)
    clientChallenge = (appId, nonSecretSalt, randomIV, ciphertext)

Note that the App ID and non-secret salt are bound to the ciphertext both through HKDF key
derivation and AES-GCM AAD. We are not relying on keeping the client public key secret and could
alternatively compute a MAC rather than encrypting with AES-GCM.

The client sends this challenge to a server.

Server Response And Challenge
-----------------------------

Once the client challenge is received, the server will derive the same key encryption key and
recover the client's public key:

    assert(appId = clientChallenge.appId)
    preSharedKey = lookupKey(appId)
    aadState = Concat(appId, clientChallenge.nonSecretSalt)
    keyEncryptingKey = HKDF(preSharedKey, nonSecretSalt, aadState)
    clientPublicKey = AES-GCM-Decrypt(
      key = keyEncryptingKey,
      iv = clientChallenge.randomIV,
      ciphertext = clientChallenge.ciphertext,
      aad = aadState)

The server can then send its own ephemeral public key to the client, encrypted under a key derived
from the pre-shared key and the protocol transcript so far:

    preSharedKey = lookupKey(appId)
    nonSecretSalt = Random(16 bytes)
    aadState = Concat(appId, nonSecretSalt, clientChallenge)
    keyEncryptingKey = HKDF(preSharedKey, nonSecretSalt, aadState)
    randomIV = Random(16 bytes)
    serverKeyPair = X25519.generate()
    ciphertext = AES-GCM-Encrypt(
      key = keyEncryptingKey,
      iv = randomIV,
      plaintext = serverKeyPair.publicKey(),
      aad = aadState)
    serverResponse = (appId, nonSecretSalt, randomIV, ciphertext)

Now that the server has the client's ephemeral public key, it can generate its own ephemeral
keypair and compute a shared secret.

    sharedSecret = X25519.computeSharedSecret(clientPublicKey, serverKeyPair.privateKey())
    derivedKey = HKDF(sharedSecret, salt=transcript, info="deriveKey")

With the shared secret, the server will also generate two initialization vectors to be used for
inbound and outbound streams. These IVs are not secret and will be bound to the preceding protocol
transcript in order to be deterministic by both parties.

    clientIv = HKDF(sharedSecret, salt=transcript, info="clientIv")
    serverIv = HKDF(sharedSecret, salt=transcript, info="serverIv")

The server can then send its response to the client, who can decrypt the server's ephemeral public
key, and reconstruct the same shared secret and IVs.

Security Comments
-----------------

This protocol is essentially a [NNpsk0](http://www.noiseprotocol.org/noise.html#pattern-modifiers)
pattern in the [Noise framework](http://www.noiseprotocol.org/) built around ECDHE using X25519 as
the underlying curve. If the pre-shared key is compromised, it does not allow for recovery of past
sessions. It would, however, allow impersonation of future sessions.

In the event of a pre-shared key compromise, messages would still be confidential from a passive
observer. Only active adversaries spoofing a session would be able to recover plaintext.

Security Changes & Compatibility
-------------

The original version of this protocol, retroactively called v1.0, did not apply an HKDF to `sharedSecret` to derive
a key (i.e. `derivedKey`) and was directly using the encoded X coordinate as key material. This is atypical and
standard practice is to pass that shared coordinate through an HKDF. The latest version adds this additional
HKDF to derive `derivedKey`.

Consequently, Apache Spark instances using v1.0 of this protocol will not negotiate the same key as
instances using v2.0 and will be **unable to send encrypted RPCs** across incompatible versions. For this reason, v1.0
remains the default to preserve backward-compatibility.