# Requenta public keys

Requenta countersigns every qualification record it accepts (RFC-024, D-21 and D-34). Anyone holding a stored
record can check that countersignature offline with the public key below. Only public keys are published here.

| File | Algorithm | `key_id` | Status |
|---|---|---|---|
| [`requenta-countersign-6f75ec64a0e1.pem`](requenta-countersign-6f75ec64a0e1.pem) | `ecdsa-p256-sha256` (ECDSA P-256, SHA-256, DER signature) | `sha256:6f75ec64a0e1d07fba8f3c5001c737ad097b9a3d41f56f09bb47a95e89fc82a1` | active since 2026-10-25, pilot key |

The `key_id` is `sha256:` followed by the SHA-256 of the key's SubjectPublicKeyInfo in DER. To recompute it:

```sh
openssl pkey -pubin -in requenta-countersign-6f75ec64a0e1.pem -outform DER | shasum -a 256
```

A countersignature signs the bytes `requenta-countersignature-v0\n` followed by the canonical JSON of its `signed`
object (`requenta-integer-json-v1`).

The pilot key lives in the console's deployment secrets. It moves to a managed key service before the first paid
trade. A retired key stays listed here with its end date, so records it countersigned remain verifiable for the
whole retention period.
