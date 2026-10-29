#!/bin/sh
# Generates throwaway PKI material for the opt-in TLS test fixture.
# Output (all under docker/tls-certs/):
#   ca.crt/ca.key          - CA that signs the server and client certs
#   server.crt/server.key  - server cert, CN=aerospike-tls, SAN DNS:aerospike-tls,IP:127.0.0.1
#   client.crt/client.key  - client cert, CN=chronon-client (PKI user CN)
#   other-ca.crt           - unrelated CA (wrong-CA negative tests)
#   untrusted-client.*     - client cert signed by other-ca (mTLS negative test)
#   other-user.crt/.key    - CA-signed cert with an unprovisioned CN (PKI negative)
#   Java stores:
#   ca.truststore.p12      - PKCS12 truststore holding ca.crt (password: storepass)
#   other-ca.truststore.p12- PKCS12 truststore holding other-ca.crt (wrong CA)
#   client.keystore.p12    - PKCS12 keystore: client.crt + ca.crt + client.key
#   untrusted.keystore.p12 - PKCS12 keystore signed by other-ca
#   other-user.keystore.p12- PKCS12 keystore for the unprovisioned CN
#   storepass.txt          - the store/key password ("storepass")
#   .complete              - written LAST; absent after an interrupted run so the
#                            fixture regenerates instead of trusting a partial set
set -eu

OUT="$(dirname "$0")/tls-certs"
mkdir -p "$OUT"
rm -f "$OUT/.complete"

# --- CA ---
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$OUT/ca.key" -out "$OUT/ca.crt" \
  -subj "/CN=chronon-test-ca" 2>/dev/null

# --- Server cert: CN + SAN must match the configured tls-name 'aerospike-tls' ---
openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/server.key" -out "$OUT/server.csr" \
  -subj "/CN=aerospike-tls" 2>/dev/null
printf "subjectAltName=DNS:aerospike-tls,IP:127.0.0.1" > "$OUT/server.ext"
openssl x509 -req -in "$OUT/server.csr" -days 3650 \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/server.crt" \
  -extfile "$OUT/server.ext" 2>/dev/null

# --- Client cert (CN is the PKI username) ---
openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/client.key" -out "$OUT/client.csr" \
  -subj "/CN=chronon-client" 2>/dev/null
openssl x509 -req -in "$OUT/client.csr" -days 3650 \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/client.crt" 2>/dev/null

# --- Untrusted CA for negative tests ---
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
  -keyout "$OUT/other-ca.key" -out "$OUT/other-ca.crt" \
  -subj "/CN=untrusted-ca" 2>/dev/null

# --- Client cert signed by the untrusted CA (mTLS negative test) ---
openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/untrusted-client.key" -out "$OUT/untrusted-client.csr" \
  -subj "/CN=untrusted-client" 2>/dev/null
openssl x509 -req -in "$OUT/untrusted-client.csr" -days 3650 \
  -CA "$OUT/other-ca.crt" -CAkey "$OUT/other-ca.key" -CAcreateserial \
  -out "$OUT/untrusted-client.crt" 2>/dev/null

# --- CA-signed client cert whose CN is not a provisioned user (PKI negative) ---
openssl req -newkey rsa:2048 -nodes \
  -keyout "$OUT/other-user.key" -out "$OUT/other-user.csr" \
  -subj "/CN=not-a-provisioned-user" 2>/dev/null
openssl x509 -req -in "$OUT/other-user.csr" -days 3650 \
  -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
  -out "$OUT/other-user.crt" 2>/dev/null

# --- Java stores (password in storepass.txt; the fixture reads it from disk) ---
# Remove any existing stores first: `keytool -importcert` refuses to add an
# alias that already exists in the target store, which would leave a store
# signed by the OLD CA next to freshly-regenerated certs.
rm -f "$OUT"/*.p12
printf "storepass" > "$OUT/storepass.txt"

# Truststore: CA only
keytool -importcert -noprompt -storetype PKCS12 -alias chronon-test-ca \
  -keystore "$OUT/ca.truststore.p12" -file "$OUT/ca.crt" -storepass storepass 2>/dev/null
keytool -importcert -noprompt -storetype PKCS12 -alias untrusted-ca \
  -keystore "$OUT/other-ca.truststore.p12" -file "$OUT/other-ca.crt" -storepass storepass 2>/dev/null

# Client keystores: the Java client needs cert chain + key in one PKCS12.
cat "$OUT/client.crt" "$OUT/ca.crt" "$OUT/client.key" > "$OUT/client.chain.pem"
openssl pkcs12 -export -in "$OUT/client.chain.pem" \
  -out "$OUT/client.keystore.p12" -name chronon-client \
  -passout pass:storepass 2>/dev/null

cat "$OUT/untrusted-client.crt" "$OUT/other-ca.crt" "$OUT/untrusted-client.key" > "$OUT/untrusted.chain.pem"
openssl pkcs12 -export -in "$OUT/untrusted.chain.pem" \
  -out "$OUT/untrusted.keystore.p12" -name untrusted-client \
  -passout pass:storepass 2>/dev/null

cat "$OUT/other-user.crt" "$OUT/ca.crt" "$OUT/other-user.key" > "$OUT/other-user.chain.pem"
openssl pkcs12 -export -in "$OUT/other-user.chain.pem" \
  -out "$OUT/other-user.keystore.p12" -name not-a-provisioned-user \
  -passout pass:storepass 2>/dev/null

rm -f "$OUT"/*.csr "$OUT"/*.srl "$OUT"/*.ext "$OUT"/*.chain.pem
chmod 644 "$OUT"/*
touch "$OUT/.complete"
echo "TLS test certs and Java stores written to $OUT"
