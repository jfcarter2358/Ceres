package auth

import (
	"ceresdb/utils"
	"testing"
)

func TestCheckPermissions(t *testing.T) {
	type Def struct {
		a       string
		b       string
		success bool
	}

	for idx, a := range permissionList {
		for jdx, b := range permissionList {
			err := CheckPermissions(a, b)
			if idx >= jdx && err != nil {
				t.Errorf("permission test success failed: %s | %s", a, b)
			}
			if jdx > idx && err == nil {
				t.Errorf("permission test error failed: %s | %s", a, b)
			}
		}
	}
}

func TestComparePasswords(t *testing.T) {
	password := "password"
	hashed, err := utils.HashAndSalt([]byte(password))
	if err != nil {
		t.Errorf("error hashing good password: %s", err.Error())
	}
	badHash, err := utils.HashAndSalt([]byte("baspassword"))
	if err != nil {
		t.Errorf("error hashing bad password: %s", err.Error())
	}
	if !ComparePasswords(hashed, password) {
		t.Errorf("error checking hashed password: mismatch")
	}
	if ComparePasswords(badHash, password) {
		t.Errorf("error checking hashed password: match")
	}
}
