#!/usr/bin/env bash
#
# generate-certs.sh
# Generates a self-signed CA and a Kafka broker keystore/truststore for TLS.
#
# Output artifacts (all written to $OUT_DIR):
#   ca.key                  - CA private key (encrypted)
#   ca.crt                  - CA self-signed root certificate
#   ca.srl                  - CA serial file (created when signing the broker cert)
#   kafka.csr               - broker certificate signing request
#   kafka.ext               - SAN / extensions used to sign the broker cert
#   kafka.crt               - broker certificate, signed by the CA
#   kafka.keystore.jks      - broker keystore (private key + signed cert + CA)
#   kafka.truststore.jks    - truststore containing the CA cert
#   kafka_keystore_creds    - file holding the keystore password
#   kafka_key_creds         - file holding the key password
#   kafka_truststore_creds  - file holding the truststore password
#
# Usage:
#   ./generate-certs.sh
#   OUT_DIR=../secrets SANS="DNS:kafka1.general-my.com,DNS:kafka2.general-my.com" ./generate-certs.sh
#
set -euo pipefail

# ----------------------------------------------------------------------------
# Configuration (override via environment variables)
# ----------------------------------------------------------------------------
OUT_DIR="${OUT_DIR:-../secrets}"
VALIDITY_DAYS="${VALIDITY_DAYS:-3650}"

# ----------------------------------------------------------------------------
# Configuration — read from .env (override the path with ENV_FILE=/path ./generate-certs.sh)
# ----------------------------------------------------------------------------
ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a              # auto-export everything defined in .env
  source "$ENV_FILE"
  set +a
else
  echo ">> No $ENV_FILE found — falling back to defaults" >&2
fi

OUT_DIR="${OUT_DIR:-../secrets}"
VALIDITY_DAYS="${VALIDITY_DAYS:-3650}"

# Passwords. Use ONE shared value for keystore + key to avoid the classic
# "keystore was tampered with, or password was incorrect" mismatch.
STORE_PASS="${STORE_PASS:-changeit-keystore}"
KEY_PASS="${KEY_PASS:-$STORE_PASS}"
TRUST_PASS="${TRUST_PASS:-changeit-truststore}"
CA_PASS="${CA_PASS:-changeit-ca}"

# Distinguished name fields
COUNTRY="${COUNTRY:-MY}"
LOCALITY="${LOCALITY:-Kuala Lumpur}"
ORG="${ORG:-General Imaging}"
OU="${OU:-IoT}"
CA_CN="${CA_CN:-kafka-ca}"
BROKER_CN="${BROKER_CN:-kafka}"

# Subject Alternative Names. EVERY hostname/IP a client may use in the TLS
# handshake MUST appear here, or hostname verification will fail.
# This includes each broker's advertised DNS name and, if you front the
# cluster with an NLB, the NLB hostname too.
SANS="${SANS:-DNS:kafka1.general-my.com,DNS:kafka2.general-my.com,DNS:kafka.general-my.com,DNS:localhost,IP:127.0.0.1}"

# ----------------------------------------------------------------------------
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo ">> Output directory: $(pwd)"
echo ">> SANs: $SANS"

# Clean any previous run so the script is idempotent
rm -f ca.key ca.crt ca.srl kafka.csr kafka.ext kafka.crt \
      kafka.keystore.jks kafka.truststore.jks \
      kafka_keystore_creds kafka_key_creds kafka_truststore_creds

# 1. Certificate Authority: private key + self-signed root certificate
echo ">> [1/8] Creating CA key and self-signed certificate"
openssl req -new -x509 \
  -keyout ca.key -out ca.crt \
  -days "$VALIDITY_DAYS" -newkey rsa:4096 -sha256 \
  -subj "/C=${COUNTRY}/L=${LOCALITY}/O=${ORG}/OU=${OU}/CN=${CA_CN}" \
  -passout pass:"$CA_PASS"

# 2. Truststore: import the CA so clients/brokers trust certs it signed
echo ">> [2/8] Building truststore and importing CA"
keytool -keystore kafka.truststore.jks -storetype JKS \
  -alias CARoot -import -file ca.crt \
  -storepass "$TRUST_PASS" -noprompt

# 3. Keystore: generate the broker key pair
echo ">> [3/8] Generating broker key pair in keystore"
keytool -keystore kafka.keystore.jks -storetype JKS \
  -alias "$BROKER_CN" -validity "$VALIDITY_DAYS" \
  -genkeypair -keyalg RSA -keysize 2048 \
  -storepass "$STORE_PASS" -keypass "$KEY_PASS" \
  -dname "C=${COUNTRY}, L=${LOCALITY}, O=${ORG}, OU=${OU}, CN=${BROKER_CN}"

# 4. Certificate signing request from the keystore
echo ">> [4/8] Exporting certificate signing request"
keytool -keystore kafka.keystore.jks \
  -alias "$BROKER_CN" -certreq -file kafka.csr \
  -storepass "$STORE_PASS" -keypass "$KEY_PASS"

# 5. Extensions file carrying the SANs (consumed by openssl at signing time)
echo ">> [5/8] Writing kafka.ext (SAN list)"
cat > kafka.ext <<EOF
subjectAltName=${SANS}
extendedKeyUsage=serverAuth,clientAuth
EOF

# 6. Sign the CSR with the CA -> kafka.crt (also creates ca.srl)
echo ">> [6/8] Signing broker certificate with CA"
openssl x509 -req \
  -CA ca.crt -CAkey ca.key \
  -in kafka.csr -out kafka.crt \
  -days "$VALIDITY_DAYS" -CAcreateserial -sha256 \
  -passin pass:"$CA_PASS" \
  -extfile kafka.ext

# 7. Import the CA, then the signed broker cert, back into the keystore.
#    Order matters: the CA (chain root) must be imported before the leaf cert.
echo ">> [7/8] Importing CA and signed cert into keystore"
keytool -keystore kafka.keystore.jks \
  -alias CARoot -import -file ca.crt \
  -storepass "$STORE_PASS" -noprompt

keytool -keystore kafka.keystore.jks \
  -alias "$BROKER_CN" -import -file kafka.crt \
  -storepass "$STORE_PASS" -keypass "$KEY_PASS" -noprompt

# 8. Credential files referenced by the Confluent image env vars
echo ">> [8/8] Writing credential files"
printf '%s' "$STORE_PASS" > kafka_keystore_creds
printf '%s' "$KEY_PASS"   > kafka_key_creds
printf '%s' "$TRUST_PASS" > kafka_truststore_creds
chmod 600 kafka_keystore_creds kafka_key_creds kafka_truststore_creds ca.key

echo
echo ">> Done. Artifacts in $(pwd):"
ls -1 ca.key ca.crt ca.srl kafka.csr kafka.ext kafka.crt \
      kafka.keystore.jks kafka.truststore.jks \
      kafka_keystore_creds kafka_key_creds kafka_truststore_creds