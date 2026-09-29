//go:build ignore

// gen writes the dev certificates in this directory. Run from the repo root:
//
//	go run certs/gen.go
//
// The CA's private key is never written, so nobody can issue more certificates
// from the committed CA. Rerunning replaces all of them.
//
// TODO: out of scope - short-lived certificates from a real CA, and never
// committing keys.
package main

import (
	"crypto/x509"
	"log"
	"os"
	"path/filepath"

	"github.com/jimmytsang/api_linux_jimmy/internal/certgen"
)

const dir = "certs"

func main() {
	ca, err := certgen.NewCA("job worker dev CA")
	if err != nil {
		log.Fatal(err)
	}
	write("ca.crt", ca.CertPEM, 0o644)

	leaves := map[string]certgen.Leaf{
		"server": {CommonName: "worker-server", Hosts: []string{"localhost", "127.0.0.1"}, Usage: x509.ExtKeyUsageServerAuth},
		"jimmy":  {CommonName: "jimmy", Usage: x509.ExtKeyUsageClientAuth},
		"jimbob": {CommonName: "jimbob", Usage: x509.ExtKeyUsageClientAuth},
	}
	for name, leaf := range leaves {
		certPEM, keyPEM, err := ca.Issue(leaf)
		if err != nil {
			log.Fatal(err)
		}
		write(name+".crt", certPEM, 0o644)
		write(name+".key", keyPEM, 0o600)
	}
}

func write(name string, data []byte, perm os.FileMode) {
	if err := os.WriteFile(filepath.Join(dir, name), data, perm); err != nil {
		log.Fatal(err)
	}
}
