package database

import (
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Database struct {
	Collections []string          `json:"collections" yaml:"collections"`
	Users       map[string]string `json:"users" yaml:"users"`
	Groups      map[string]string `json:"groups" yaml:"group"`
	Roles       map[string]string `json:"roles" yaml:"roles"`
}

var Databases map[string]Database

func LoadDatabases() error {
	path := fmt.Sprintf("%s/databases.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Databases = make(map[string]Database)
		return nil
	}

	jsonFile, err := os.Open(path)
	if err != nil {
		return err
	}
	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}
	json.Unmarshal(byteValue, &Databases)
	return nil
}

func Create(name string) error {
	d := Database{
		Collections: []string{},
		Users: map[string]string{
			config.Config.AdminUsername: constants.PERMISSION_ADMIN,
		},
		Groups: map[string]string{
			constants.GROUP_ADMIN: constants.PERMISSION_ADMIN,
		},
		Roles: map[string]string{
			constants.ROLE_ADMIN: constants.ROLE_ADMIN,
		},
	}
	if Databases == nil {
		Databases = make(map[string]Database)
	}
	Databases[name] = d
	path := fmt.Sprintf("%s/databases.json", config.Config.DataDir)
	data, err := json.Marshal(Databases)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0777)
}

func Delete(name string) error {
	delete(Databases, name)
	path := fmt.Sprintf("%s/databases.json", config.Config.DataDir)
	data, err := json.Marshal(Databases)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0777)
}

func AddCollection(d, name string, u auth.User) error {
	if err := VerifyAuth(d, constants.PERMISSION_WRITE, u); err != nil {
		return err
	}
	db := Databases[d]
	db.Collections = append(db.Collections, name)
	Databases[d] = db
	return saveDatabases()
}

func DeleteCollection(d, name string, u auth.User) error {
	if err := VerifyAuth(d, constants.PERMISSION_UPDATE, u); err != nil {
		return err
	}
	db := Databases[d]
	db.Collections = utils.Remove(db.Collections, name)
	Databases[d] = db
	return saveDatabases()
}

func updateAuth(name string, us, gs, rs map[string]string) error {
	d := Databases[name]
	d.Users = us
	d.Groups = gs
	d.Roles = rs
	Databases[name] = d
	return saveDatabases()
}

func AddGroupAuth(name, g, p string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	gs[g] = p
	return updateAuth(name, us, gs, rs)
}

func DeleteGroupAuth(name, g string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	if _, ok := gs[g]; ok {
		delete(gs, g)
	}
	return updateAuth(name, us, gs, rs)
}

func AddRoleAuth(name, r, p string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	rs[r] = p
	return updateAuth(name, us, gs, rs)
}

func DeleteRoleAuth(name, r string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	if _, ok := rs[r]; ok {
		delete(rs, r)
	}
	return updateAuth(name, us, gs, rs)
}

func AddUserAuth(name, u, p string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	us[u] = p
	return updateAuth(name, us, gs, rs)
}

func DeleteUserAuth(name, u string) error {
	d := Databases[name]
	us := d.Users
	gs := d.Groups
	rs := d.Roles
	if _, ok := us[u]; ok {
		delete(us, u)
	}
	return updateAuth(name, us, gs, rs)
}

func VerifyAuth(name, p string, u auth.User) error {
	d := Databases[name]
	for name, permission := range d.Users {
		if name == u.Username {
			return auth.CheckPermissions(p, permission)
		}
	}
	for name, permission := range d.Groups {
		for _, g := range u.Groups {
			if name == g {
				return auth.CheckPermissions(p, permission)
			}
		}
	}
	for name, permission := range d.Roles {
		for _, r := range u.Roles {
			if name == r {
				return auth.CheckPermissions(p, permission)
			}
		}
	}
	return fmt.Errorf("user %s does not have access to %s", u.Username, name)
}

func saveDatabases() error {
	path := fmt.Sprintf("%s/databases.json", config.Config.DataDir)
	data, err := json.Marshal(Databases)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0777)
}
