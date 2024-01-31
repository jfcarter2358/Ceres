package collection

import (
	"ceresdb/auth"
	"ceresdb/config"
	"ceresdb/database"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/uuid"
)

const TEST_DB_NAME = "foo"
const TEST_COLLECTION_NAME = "foo"

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

func clean(delete_collection bool, id string) error {
	if delete_collection {
		if err := Delete(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
			return err
		}
	}
	if err := database.Delete(TEST_DB_NAME); err != nil {
		return err
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
	if err := database.LoadDatabases(); err != nil {
		return err
	}
	if err := database.Create(TEST_DB_NAME); err != nil {
		return err
	}
	LoadCollections()
	return nil
}

func checkFile(f, g File) error {
	if f.Length != g.Length {
		return fmt.Errorf("file length mismatch: got %d, want %d", f.Length, g.Length)
	}
	if len(f.Available) != len(g.Available) {
		return fmt.Errorf("file available length mismatch: got (%d) %v, want (%d) %v", len(f.Available), f.Available, len(g.Available), g)
	}
	for idx, v := range f.Available {
		if v != g.Available[idx] {
			return fmt.Errorf("file available mismatch at %d: got %d, want %d", idx, v, g.Available[idx])
		}
	}
	return nil
}

func checkCollection(col map[string]map[string]Collection) error {
	if len(Collections) != len(col) {
		return fmt.Errorf("database length mismatch: got (%d) %v, want (%d) %v", len(Collections), Collections, len(col), col)
	}
	for i, c := range Collections {
		if d, ok := col[i]; ok {
			if len(Collections) != len(col) {
				return fmt.Errorf("collection length mismatch: got (%d) %v, want (%d) %v", len(c), c, len(d), d)
			}
			for j, a := range c {
				if b, ok := d[j]; ok {
					for k, f := range a.Files {
						if g, ok := b.Files[k]; ok {
							if err := checkFile(f, g); err != nil {
								return err
							}
							continue
						}
						return fmt.Errorf("files were incorrect, got: %v, want: %v", a.Users, b.Users)
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
			}
			continue
		}
		return fmt.Errorf("invalid collection key: foo, got: %v, want: %v", Collections, c)
	}
	return nil
}

func TestLoadCollections(t *testing.T) {
	var expected = []map[string]map[string]Collection{
		{
			"foo": {
				"foo": {
					Files: map[string]File{
						"1": {
							Length:    0,
							Available: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
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
					Files: map[string]File{
						"1": {
							Length:    0,
							Available: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
						},
					},
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
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if len(Collections) > 0 {
		t.Errorf("invalid default collection, got %v, want %v", Collections, expected[0])
	}
	if err := clean(false, id); err != nil {
		t.Errorf(err.Error())
	}

	for _, col := range expected[1:] {
		if err := setup(false, id); err != nil {
			t.Errorf(err.Error())
		}
		if err := LoadCollections(); err != nil {
			t.Errorf("error loading collection: %s", err.Error())
		}
		if err := checkCollection(col); err != nil {
			t.Error(err.Error())
		}
		if err := clean(true, id); err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestCreate(t *testing.T) {
	var expected = map[string]map[string]Collection{
		TEST_DB_NAME: {
			TEST_COLLECTION_NAME: {
				Files: map[string]File{},
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
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create(TEST_DB_NAME, TEST_COLLECTION_NAME); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
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
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := Delete("foo", "foo"); err != nil {
		t.Errorf("error deleting collection: %s", err.Error())
	}
	if len(Collections["foo"]) != 0 {
		t.Errorf("collection length mismatch: got (%d) %v, want (0) {}", len(Collections["foo"]), Collections["foo"])
	}
	if err := clean(false, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddGroupAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
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
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := AddGroupAuth("foo", "foo", "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteGroupAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
				Users: map[string]string{
					"ceresdb": "admin",
				},
				Groups: map[string]string{},
				Roles: map[string]string{
					"admin": "admin",
				},
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := DeleteGroupAuth("foo", "foo", "admin"); err != nil {
		t.Errorf("error deleting group auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddRoleAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
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
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := AddRoleAuth("foo", "foo", "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteRoleAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
				Users: map[string]string{
					"ceresdb": "admin",
				},
				Groups: map[string]string{
					"admin": "admin",
				},
				Roles: map[string]string{},
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := DeleteRoleAuth("foo", "foo", "admin"); err != nil {
		t.Errorf("error deleting role auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestAddUserAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
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
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := AddUserAuth("foo", "foo", "foo", "admin"); err != nil {
		t.Errorf("error adding group auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
		t.Error(err.Error())
	}
	if err := clean(true, id); err != nil {
		t.Errorf(err.Error())
	}
}

func TestDeleteUserAuth(t *testing.T) {
	var expected = map[string]map[string]Collection{
		"foo": {
			"foo": {
				Files: map[string]File{},
				Users: map[string]string{},
				Groups: map[string]string{
					"admin": "admin",
				},
				Roles: map[string]string{
					"admin": "admin",
				},
			},
		},
	}

	id := uuid.New().String()
	if err := setup(true, id); err != nil {
		t.Errorf(err.Error())
	}
	if err := Create("foo", "foo"); err != nil {
		t.Errorf("error creating collection: %s", err.Error())
	}
	if err := DeleteUserAuth("foo", "foo", "ceresdb"); err != nil {
		t.Errorf("error deleting user auth: %s", err.Error())
	}
	if err := checkCollection(expected); err != nil {
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
		if err := Create("foo", "foo"); err != nil {
			t.Errorf("error creating collection: %s", err.Error())
		}

		for u, p := range ex.Users {
			AddUserAuth("foo", "foo", u, p)
		}
		for g, p := range ex.Groups {
			AddGroupAuth("foo", "foo", g, p)
		}
		for r, p := range ex.Roles {
			AddRoleAuth("foo", "foo", r, p)
		}

		if ex.Success {
			if err := VerifyAuth("foo", "foo", ex.Permission, u); err != nil {
				t.Errorf("auth check failed for permission %s: got %s, want: nil", ex.Permission, err.Error())
			}
		} else {
			if err := VerifyAuth("foo", "foo", ex.Permission, u); err == nil {
				t.Errorf("auth check failed for permission %s: got nil, want: err", ex.Permission)
			}
		}

		if err := clean(true, id); err != nil {
			t.Errorf(err.Error())
		}
	}
}
