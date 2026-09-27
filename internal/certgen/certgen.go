// Package certgen issues ECDSA P-256 certificates signed by a throwaway CA. It
// generates the dev certificates under certs/ and the certificates tests use,
// so tests never depend on the committed ones expiring.
//
// TODO: out of scope - short-lived certificates from a real CA.
package certgen

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"time"
)

const (
	CAValidity   = 365 * 24 * time.Hour
	LeafValidity = 90 * 24 * time.Hour
)

// CA signs leaf certificates. Its private key only lives in memory.
type CA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	CertPEM []byte
}

// NewCA creates a self-signed CA valid for CAValidity.
func NewCA(commonName string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := newSerial()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now,
		NotAfter:              now.Add(CAValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true, // signs leaf certificates only
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{cert: cert, key: key, CertPEM: pemEncode("CERTIFICATE", der)}, nil
}

// Leaf describes a certificate for the CA to issue.
type Leaf struct {
	CommonName string
	// Hosts are the DNS names and IP addresses the certificate is valid for.
	// Only server certificates need them.
	Hosts []string
	// Usage is x509.ExtKeyUsageServerAuth or x509.ExtKeyUsageClientAuth. A
	// certificate gets exactly one, so it can't be used for the other side.
	Usage x509.ExtKeyUsage
	// NotBefore defaults to now and NotAfter to NotBefore plus LeafValidity.
	NotBefore, NotAfter time.Time
}

// Issue creates a new key and a certificate for it signed by the CA, both PEM
// encoded.
func (ca *CA) Issue(l Leaf) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := newSerial()
	if err != nil {
		return nil, nil, err
	}
	if l.NotBefore.IsZero() {
		l.NotBefore = time.Now()
	}
	if l.NotAfter.IsZero() {
		l.NotAfter = l.NotBefore.Add(LeafValidity)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: l.CommonName},
		NotBefore:    l.NotBefore,
		NotAfter:     l.NotAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{l.Usage},
	}
	for _, h := range l.Hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pemEncode("CERTIFICATE", der), pemEncode("PRIVATE KEY", keyDER), nil
}

// newSerial returns a random 128-bit serial number, as RFC 5280 recommends.
func newSerial() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}
	return serial, nil
}

func pemEncode(typ string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
}
