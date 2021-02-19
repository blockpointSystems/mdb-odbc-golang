package mdb

import (
	"crypto/rsa"
	"sync"
)

// server pub keys registry
var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

// RegisterServerPubKey registers a server RSA public key which can be used to
// send data in a secure manner to the server without receiving the public key
// in a potentially insecure way from the server first.
// Registered keys can afterwards be used adding serverPubKey=<name> to the DSN.
//
// Note: The provided rsa.PublicKey instance is exclusively owned by the driver
// after registering it and may not be modified.
//
//  data, err := ioutil.ReadFile("mykey.pem")
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  block, _ := pem.Decode(data)
//  if block == nil || block.Type != "PUBLIC KEY" {
//  	log.Fatal("failed to decode PEM block containing public key")
//  }
//
//  pub, err := x509.ParsePKIXPublicKey(block.Bytes)
//  if err != nil {
//  	log.Fatal(err)
//  }
//
//  if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
//  	mysql.RegisterServerPubKey("mykey", rsaPubKey)
//  } else {
//  	log.Fatal("not a RSA public key")
//  }
//
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}
