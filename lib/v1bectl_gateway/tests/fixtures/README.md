# Test fixtures: `v1bectl_gateway`

`selfsigned-localhost.crt.pem` and `selfsigned-localhost.key.pem` are a **test-only, throwaway** self-signed certificate (`CN=localhost`, SAN `DNS:localhost`, `IP:127.0.0.1`, RSA 2048, SHA-256) and its unencrypted PKCS#8 private key. They exist so the fake Dirigera hub in `src/dirigera.rs` (`mod event_stream_tests`) can serve TLS on `127.0.0.1`, which lets the event-stream tests check that the client accepts the hub's self-signed certificate. They are read only through `include_bytes!` inside `#[cfg(test)]`, never ship in a binary, and protect nothing, so the key is public on purpose. They were generated with OpenSSL 3.6.4:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 36500 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout selfsigned-localhost.key.pem -out selfsigned-localhost.crt.pem
```
