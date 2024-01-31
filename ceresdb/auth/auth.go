package auth

import (
	"ceresdb/constants"
	"ceresdb/utils"
	"fmt"
	"log"

	"golang.org/x/crypto/bcrypt"
)

type User struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	Roles    []string `json:"roles"`
	Groups   []string `json:"groups"`
}

var Users map[string]User

var permissionList = []string{constants.PERMISSION_ADMIN, constants.PERMISSION_UPDATE, constants.PERMISSION_WRITE, constants.PERMISSION_READ}

func CheckPermissions(a, b string) error {
	// b is guaranteed to be one of the above constants, so we don't need to check for -1
	idx := utils.Index(permissionList, b)
	jdx := utils.Index(permissionList, a)
	for _, val := range permissionList[:jdx+1] {
		if b == val {
			return nil
		}
	}
	return fmt.Errorf("permission %s is at a higher index (%d) than desired permission %s (%d)", b, idx, a, jdx)
}

func ComparePasswords(hashedPassword string, plainPassword string) bool {
	// Since we'll be getting the hashed password from the DB it
	// will be a string so we'll need to convert it to a byte slice
	byteHash := []byte(hashedPassword)
	bytePlain := []byte(plainPassword)
	err := bcrypt.CompareHashAndPassword(byteHash, bytePlain)
	if err != nil {
		log.Println(err)
		return false
	}

	return true
}
