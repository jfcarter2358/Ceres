package collection

import (
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/constants"
	"ceresdb/index"
	"ceresdb/logger"
	"ceresdb/schema"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type File struct {
	Length    int   `json:"length" yaml:"length"`
	Available []int `json:"available" yaml:"available"`
}

type Collection struct {
	Files  map[string]File   `json:"files" yaml:"files"`
	Users  map[string]string `json:"users" yaml:"users"`
	Groups map[string]string `json:"groups" yaml:"group"`
	Roles  map[string]string `json:"roles" yaml:"roles"`
}

var Collections map[string]map[string]Collection

func LoadCollections() error {
	path := fmt.Sprintf("%s/collections.json", config.Config.DataDir)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		Collections = make(map[string]map[string]Collection)
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
	json.Unmarshal(byteValue, &Collections)
	return nil
}

func Create(d, name string) error {
	if Collections == nil {
		Collections = make(map[string]map[string]Collection)
	}
	if _, ok := Collections[d]; !ok {
		Collections[d] = map[string]Collection{}
	}
	c := Collection{
		Files: make(map[string]File),
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
	Collections[d][name] = c

	return saveCollections()
}

func Delete(d, name string) error {
	if err := index.Delete(d, name); err != nil {
		return err
	}
	if err := schema.Delete(d, name); err != nil {
		return err
	}
	for f, _ := range Collections[d][name].Files {
		path := fmt.Sprintf("%s/%s/%s/%s", config.Config.DataDir, d, name, f)
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	delete(Collections[d], name)
	return saveCollections()
}

func updateAuth(d, name string, us, gs, rs map[string]string) error {
	c := Collections[d][name]
	c.Users = us
	c.Groups = gs
	c.Roles = rs
	Collections[d][name] = c
	return saveCollections()
}

func AddGroupAuth(d, name, g, p string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	gs[g] = p
	return updateAuth(d, name, us, gs, rs)
}

func DeleteGroupAuth(d, name, g string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	if _, ok := gs[g]; ok {
		delete(gs, g)
	}
	return updateAuth(d, name, us, gs, rs)
}

func AddRoleAuth(d, name, r, p string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	rs[r] = p
	return updateAuth(d, name, us, gs, rs)
}

func DeleteRoleAuth(d, name, r string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	if _, ok := rs[r]; ok {
		delete(rs, r)
	}
	return updateAuth(d, name, us, gs, rs)
}

func AddUserAuth(d, name, u, p string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	us[u] = p
	return updateAuth(d, name, us, gs, rs)
}

func DeleteUserAuth(d, name, u string) error {
	c := Collections[d][name]
	us := c.Users
	gs := c.Groups
	rs := c.Roles
	if _, ok := us[u]; ok {
		delete(us, u)
	}
	return updateAuth(d, name, us, gs, rs)
}

func VerifyAuth(d, c, p string, u auth.User) error {
	col := Collections[d][c]
	logger.Tracef("", "Collection information: %v", col)
	for name, permission := range col.Users {
		logger.Tracef("", "Checking collection user auth against %s | %s", name, u.Username)
		if name == u.Username {

			// if err := auth.CheckPermissions(p, permission); err == nil {
			// 	return fmt.Errorf("Found success in user with permission %s", permission)
			// }
			return auth.CheckPermissions(p, permission)
		}
	}
	for name, permission := range col.Groups {
		for _, g := range u.Groups {
			logger.Tracef("", "Checking collection group auth %s | %s", name, g)
			if name == g {
				// if err := auth.CheckPermissions(p, permission); err == nil {
				// 	return fmt.Errorf("Found success in group with permission %s", permission)
				// }
				return auth.CheckPermissions(p, permission)
			}
		}
	}
	for name, permission := range col.Roles {
		for _, r := range u.Roles {
			logger.Tracef("", "Checking collection role auth %s | %s", name, r)
			if name == r {
				// if err := auth.CheckPermissions(p, permission); err == nil {
				// 	return fmt.Errorf("Found success in role with permission %s", permission)
				// }
				return auth.CheckPermissions(p, permission)
			}
		}
	}
	return fmt.Errorf("user %s does not have access to %s.%s", u.Username, d, c)
}

func saveCollections() error {
	path := fmt.Sprintf("%s/collections.json", config.Config.DataDir)
	data, err := json.Marshal(Collections)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0777)
}
