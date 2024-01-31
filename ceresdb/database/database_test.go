package database

import (
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/constants"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
)

const TEST_DB_NAME = "foo"

func copy(src string, dst string) {
	// Read all content of src to data, may cause OOM for a large file.
	data, err := ioutil.ReadFile(src)
	if err != nil {
		panic(err)
	}
	// Write data to dst
	if err := ioutil.WriteFile(dst, data, 0644); err != nil {
		panic(err)
	}
}

func cleanEnv() {
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", "")
	os.Setenv("CERESDB_DATA_DIR", "")
}

func cleanFixtures(id string) {
	os.RemoveAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id))
}

func clean(delete_database bool, id string) error {
	if delete_database {
		if err := Delete(TEST_DB_NAME); err != nil {
			return err
		}
	}
	cleanFixtures(id)
	cleanEnv()
	return nil
}

func setupEnv(id string) {
	os.Setenv("CERESDB_STORAGE_LINE_LIMIT", "10")
	os.Setenv("CERESDB_DATA_DIR", fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id))
}

func setupFixtures(id string) {
	copy("../../test/fixtures/databases.json", fmt.Sprintf("/tmp/ceresdb/fixtures/%s/databases.json", id))
	copy("../../test/fixtures/collections.json", fmt.Sprintf("/tmp/ceresdb/fixtures/%s/collections.json", id))
}

func setup(empty bool, id string) error {
	os.MkdirAll(fmt.Sprintf("/tmp/ceresdb/fixtures/%s", id), 0777)
	setupEnv(id)
	if !empty {
		setupFixtures(id)
	}
	config.ReadConfig()
	Databases = map[string]Database{}
	return LoadDatabases()
}

func checkDatabase(db map[string]Database) error {
	if len(Databases) != len(db) {
		return fmt.Errorf("database length mismatch: got (%d) %v, want (%d) %v", len(Databases), Databases, len(db), db)
	}
	for key, a := range Databases {
		if key == constants.AUTH_DB_NAME {
			continue
		}
		if b, ok := db[key]; ok {
			for idx, c := range a.Collections {
				if idx > len(b.Collections) {
					return fmt.Errorf("collections were incorrect, got: %v, want: %v", a.Collections, b.Collections)
				}
				if c != b.Collections[idx] {
					return fmt.Errorf("collections were incorrect, got: %v, want: %v", a.Collections, b.Collections)
				}
			}
			for k, u := range a.Users {
				if v, ok := b.Users[k]; ok {
					if u != v {
						return fmt.Errorf("users were incorrect, got: %v, want: %v", a.Users, b.Users)
					}
					continue
				}
				return fmt.Errorf("users were incorrect, got: %v, want: %v", a.Users, b.Users)
			}
			for k, g := range a.Groups {
				if h, ok := b.Groups[k]; ok {
					if g != h {
						return fmt.Errorf("groups were incorrect, got: %v, want: %v", a.Groups, b.Groups)
					}
					continue
				}
				return fmt.Errorf("groups were incorrect, got: %v, want: %v", a.Groups, b.Groups)
			}
			for k, r := range a.Roles {
				if s, ok := b.Roles[k]; ok {
					if r != s {
						return fmt.Errorf("roles were incorrect, got: %v, want: %v", a.Roles, b.Roles)
					}
					continue
				}
				return fmt.Errorf("roles were incorrect, got: %v, want: %v", a.Roles, b.Roles)
			}
			continue
		}
		return fmt.Errorf("invalid database key: foo, got: %v, want: %v", Databases, db)
	}
	return nil
}

func TestLoadDatabases(t *testing.T) {
	var expected = []map[string]Database{
		{},
		{
			"foo": {
				Collections: []string{"cFoo"},
				Users: map[string]string{
					"uFoo": "write",
				},
				Groups: map[string]string{
					"gFoo": "read",
				},
				Roles: map[string]string{
					"rFoo": "update",
				},
			},
			"bar": {
				Collections: []string{"cBar"},
				Users: map[string]string{
					"uBar": "write",
				},
				Groups: map[string]string{
					"gBar": "read",
				},
				Roles: map[string]string{
					"rBar": "update",
				},
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	os.Setenv("CERESDB_DATA_DIR", "/tmp/ceresdb/fixtures/clean")
	LoadDatabases()
	if len(Databases) > 0 {
		t.Errorf("invalid default database, got %v, want %v", Databases, expected[0])
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}

	for _, db := range expected[1:] {
		id := uuid.New().String()
		if err := setup(false, id); err != nil {
			t.Errorf(err.Error())
		}
		if err := checkDatabase(db); err != nil {
			t.Error(err.Error())
		}
		if err := clean(true, id); err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestCreate(t *testing.T) {
	var expected = map[string]Database{
		"foo": {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
			},
			Groups: map[string]string{
				"admin": "admin",
			},
			Roles: map[string]string{
				"admin": "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDelete(t *testing.T) {
	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := Delete(TEST_DB_NAME); err != nil {
		t.Errorf("error deleting database: %s", err.Error())
	}
	if len(Databases) != 0 {
		t.Errorf("database length mismatch: got (%d) %v, want (0) {}", len(Databases), Databases)
	}
	if err := clean(false, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddGroupAuth(t *testing.T) {
	var expected = map[string]Database{
		"foo": {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
			},
			Groups: map[string]string{
				"admin": "admin",
				"foo":   "admin",
			},
			Roles: map[string]string{
				"admin": "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := AddGroupAuth(TEST_DB_NAME, "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteGroupAuth(t *testing.T) {
	var expected = map[string]Database{
		TEST_DB_NAME: {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
			},
			Groups: map[string]string{},
			Roles: map[string]string{
				"admin": "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := DeleteGroupAuth(TEST_DB_NAME, "admin"); err != nil {
		t.Errorf("error deleting group auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddRoleAuth(t *testing.T) {
	var expected = map[string]Database{
		"foo": {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
			},
			Groups: map[string]string{
				"admin": "admin",
			},
			Roles: map[string]string{
				"admin": "admin",
				"foo":   "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := AddRoleAuth(TEST_DB_NAME, "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteRoleAuth(t *testing.T) {
	var expected = map[string]Database{
		TEST_DB_NAME: {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
			},
			Groups: map[string]string{
				"admin": "admin",
			},
			Roles: map[string]string{},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := DeleteRoleAuth(TEST_DB_NAME, "admin"); err != nil {
		t.Errorf("error deleting role auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddUserAuth(t *testing.T) {
	var expected = map[string]Database{
		TEST_DB_NAME: {
			Collections: []string{},
			Users: map[string]string{
				"ceresdb": "admin",
				"foo":     "admin",
			},
			Groups: map[string]string{
				"admin": "admin",
			},
			Roles: map[string]string{
				"admin": "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := AddUserAuth(TEST_DB_NAME, "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteUserAuth(t *testing.T) {
	var expected = map[string]Database{
		TEST_DB_NAME: {
			Collections: []string{},
			Users:       map[string]string{},
			Groups: map[string]string{
				"admin": "admin",
			},
			Roles: map[string]string{
				"admin": "admin",
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME); err != nil {
		t.Errorf("error creating database: %s", err.Error())
	}
	if err := DeleteUserAuth("foo", "ceresdb"); err != nil {
		t.Errorf("error deleting user auth: %s", err.Error())
	}
	if err := checkDatabase(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestVerifyAuth(t *testing.T) {
	type e struct {
		Users      map[string]string
		Roles      map[string]string
		Groups     map[string]string
		Success    bool
		Permission string
	}

	u := auth.User{
		Username: "foo",
		Password: "",
		Groups:   []string{"foo"},
		Roles:    []string{"foo"},
	}

	var expected = []e{
		{
			Users: map[string]string{
				"foo": "read",
			},
			Roles:      map[string]string{},
			Groups:     map[string]string{},
			Permission: "read",
			Success:    true,
		},
		{
			Users: map[string]string{},
			Roles: map[string]string{
				"foo": "read",
			},
			Groups:     map[string]string{},
			Permission: "write",
			Success:    false,
		},
		{
			Users: map[string]string{},
			Roles: map[string]string{
				"foo": "read",
			},
			Groups:     map[string]string{},
			Permission: "read",
			Success:    true,
		},
		{
			Users: map[string]string{},
			Roles: map[string]string{
				"foo": "read",
			},
			Groups:     map[string]string{},
			Permission: "write",
			Success:    false,
		},
		{
			Users: map[string]string{},
			Roles: map[string]string{},
			Groups: map[string]string{
				"foo": "read",
			},
			Permission: "read",
			Success:    true,
		},
		{
			Users: map[string]string{},
			Roles: map[string]string{},
			Groups: map[string]string{
				"foo": "read",
			},
			Permission: "write",
			Success:    false,
		},
	}

	for _, ex := range expected {
		id := uuid.New().String()
		if err := setup(true, id); err != nil {
			t.Errorf(err.Error())
		}
		if err := Create("foo"); err != nil {
			t.Errorf("error creating database: %s", err.Error())
		}

		for u, p := range ex.Users {
			AddUserAuth("foo", u, p)
		}
		for g, p := range ex.Groups {
			AddGroupAuth("foo", g, p)
		}
		for r, p := range ex.Roles {
			AddRoleAuth("foo", r, p)
		}

		if ex.Success {
			if err := VerifyAuth("foo", ex.Permission, u); err != nil {
				t.Errorf("auth check failed for permission %s: got %s, want: nil", ex.Permission, err.Error())
			}
		} else {
			if err := VerifyAuth(TEST_DB_NAME, ex.Permission, u); err == nil {
				t.Errorf("auth check failed for permission %s: got nil, want: err", ex.Permission)
			}
		}
		if err := clean(true, id); err != nil {
			t.Errorf(err.Error())
		}
	}
}
