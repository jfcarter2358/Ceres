from ceresdb_python import __version__
from ceresdb_python import Connection
import json
import pytest

CERES_USERNAME="ceres"
CERES_PASSWORD="ceres"
CERES_HOST="localhost"
CERES_PORT=7437

def test_version():
    assert __version__ == '0.1.0'

def test_database():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    # Delete resources
    conn.query("delete database foo")

    # Get databases when none exist
    expected_data = [
        {"name":"_auth"},
    ]
    data = conn.query("get database")
    assert data == expected_data

    # Post database
    expected_data = [
        {"name":"_auth"},
        {"name":"foo"}
    ]
    conn.query("post database foo")
    data = conn.query("get database")
    assert data == expected_data

    # Delete database
    expected_data = [
        {"name":"_auth"},
    ]
    conn.query("delete database foo")
    data = conn.query("get database")
    assert data == expected_data

def test_collection():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    # Delete resources
    conn.query("delete database foo")
    # Create resources
    conn.query("post database foo")

    # Get collections when none exist
    expected_data = [
        {"name":"_users",'schema':{'role':'STRING','username':'STRING'}}
    ]
    data = conn.query("get collection foo")
    assert data == expected_data

    # Post collection
    expected_data = [
        {"name":"_users",'schema':{'role':'STRING','username':'STRING'}},
        {"name":"bar",'schema':{'hello':'STRING','world':'INT'}}
    ]
    input_data = {"hello":"STRING","world":"INT"}
    conn.query(f"post collection foo.bar {json.dumps(input_data)}")
    data = conn.query("get collection foo | ORDERASC name")
    assert data == expected_data

    # Put collection
    expected_data = [
        {"name":"_users",'schema':{'role':'STRING','username':'STRING'}},
        {"name":"bar",'schema':{'hello':'STRING','world':'STRING'}}
    ]
    input_data = {"hello":"STRING","world":"STRING"}
    conn.query(f"put collection foo.bar {json.dumps(input_data)}")
    data = conn.query("get collection foo | ORDERASC name")
    data = sorted(data, key=lambda d: d['name']) 
    assert data == expected_data

    # Delete collection
    expected_data = [
        {"name":"_users",'schema':{'role':'STRING','username':'STRING'}}
    ]
    conn.query("delete collection foo.bar")
    data = conn.query("get collection foo")
    assert data == expected_data

    # Delete resources
    conn.query("delete database foo")

def test_record():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    # Delete resources
    conn.query("delete database foo")
    # Create resources
    conn.query("post database foo")
    conn.query("post collection foo.bar {\"item\":\"STRING\",\"price\":\"FLOAT\",\"in_stock\":\"BOOL\",\"count\":\"INT\"}")

    # Get records when none exist
    data = conn.query("get record foo.bar *")
    assert data == []

    # Post record
    expected_data = [
        {"item": "bolt", "price": 0.8, "in_stock": True, "count": 20},
        {"item": "nail", "price": 0.3, "in_stock": True, "count": 10},
        {"item": "nut", "price": 0.5, "in_stock": True, "count": 50},
        {"item": "screw", "price": 0.2, "in_stock": False, "count": 0}
    ]
    input_data = [
        {"item": "bolt", "price": 0.8, "in_stock": True, "count": 20},
        {"item": "nail", "price": 0.3, "in_stock": True, "count": 10},
        {"item": "nut", "price": 0.5, "in_stock": True, "count": 50},
        {"item": "screw", "price": 0.2, "in_stock": False, "count": 0}
    ]
    conn.query(f"post record foo.bar {json.dumps(input_data)}")
    data = conn.query("get record foo.bar [\"item\",\"price\",\"in_stock\",\"count\"] | ORDERASC item")
    assert data == expected_data

    # Put record
    expected_data = [
        {"item": "bolt", "price": 0.8, "in_stock": True, "count": 20},
        {"item": "nail", "price": 0.3, "in_stock": True, "count": 10},
        {"item": "nut", "price": 0.5, "in_stock": True, "count": 60},
        {"item": "screw", "price": 0.2, "in_stock": False, "count": 0}
    ]
    input_data = conn.query(f"get record foo.bar | filter item = \"nut\"")[0]
    input_data["count"] = 60
    conn.query(f"put record foo.bar {json.dumps(input_data)}")
    data = conn.query("get record foo.bar [\"item\",\"price\",\"in_stock\",\"count\"] | ORDERASC item")
    assert data == expected_data

    # Patch record
    expected_data = [
        {"item": "bolt", "price": 1.0, "in_stock": True, "count": 20},
        {"item": "nail", "price": 1.0, "in_stock": True, "count": 10},
        {"item": "nut", "price": 1.0, "in_stock": True, "count": 60},
        {"item": "screw", "price": 1.0, "in_stock": False, "count": 0}
    ]
    input_data = {"price": 1.0}
    conn.query(f"get record foo.bar | patch record foo.bar - {json.dumps(input_data)}")
    data = conn.query("get record foo.bar [\"item\",\"price\",\"in_stock\",\"count\"] | ORDERASC item")
    assert data == expected_data

    # Delete record
    expected_data = []
    input_data = [datum[".id"] for datum in conn.query("get record foo.bar .id")]
    conn.query(f"delete record foo.bar {json.dumps(input_data)}")
    data = conn.query("get record foo.bar *")
    assert data == expected_data

    # Delete resources
    conn.query("delete collection foo.bar")
    conn.query("delete database foo")

def test_permit():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    # Delete resources
    conn.query("delete database foo")
    # Create resources
    conn.query("post database foo")
    conn.query("post user {\"username\":\"readonly\",\"role\":\"READ\",\"password\":\"readonly\"}")

    # Get permit when none exist
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
    ]
    data = conn.query("get permit foo [\"username\",\"role\"]")
    assert data == expected_data

    # Post permit
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
        {"username": "readonly", "role": "READ"}
    ]
    input_data = [
        {"username": "readonly", "role": "READ"}
    ]
    conn.query(f"post permit foo {json.dumps(input_data)}")
    data = conn.query("get permit foo [\"username\",\"role\"] | ORDERASC username")
    assert data == expected_data

    # Verify permission check
    with pytest.raises(Exception):
        input_data = {'role':'STRING','username':'STRING'}
        conn_readonly = Connection("readonly", "readonly", CERES_HOST, CERES_PORT)
        conn_readonly.query(f"post collection foo.bar {json.dumps(input_data)}")

    # Put permit
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
        {"username": "readonly", "role": "WRITE"}
    ]
    input_data = conn.query(f"get permit foo | filter username = \"readonly\"")[0]
    input_data["role"] = "WRITE"
    conn.query(f"put permit foo {json.dumps(input_data)}")
    data = conn.query("get permit foo [\"username\",\"role\"] | ORDERASC username")
    assert data == expected_data

    # Delete permit
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
    ]
    input_data = [datum[".id"] for datum in conn.query("get permit foo .id | filter username != \"ceres\"")]
    conn.query(f"delete permit foo {json.dumps(input_data)}")
    data = conn.query("get permit foo [\"username\",\"role\"] | ORDERASC username")
    assert data == expected_data

    # Delete resources
    conn.query("delete collection foo.bar")
    conn.query("delete database foo")

def test_user():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    # Delete resources
    input_data = [datum[".id"] for datum in conn.query("get user .id | filter username != \"ceres\"")]
    conn.query(f"delete user {json.dumps(input_data)}")

    # Get user when none exist
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
    ]
    data = conn.query("get user [\"username\",\"role\"]")
    assert data == expected_data

    # Post user
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
        {"username": "readonly", "role": "READ"}
    ]
    input_data = [
        {"username": "readonly", "password": "readonly", "role": "READ"}
    ]
    conn.query(f"post user {json.dumps(input_data)}")
    data = conn.query("get user [\"username\",\"role\"] | ORDERASC username")
    assert data == expected_data

    # Verify permission check
    with pytest.raises(Exception):
        input_data = {'role':'STRING','username':'STRING'}
        conn_readonly = Connection("readonly", "readonly", CERES_HOST, CERES_PORT)
        conn_readonly.query(f"post collection foo.bar {json.dumps(input_data)}")

    # Put user
    expected_data = [
        {"username": "readonly", "role": "WRITE"},
        {"username": "ceres", "role": "ADMIN"}
    ]
    input_data = conn.query(f"get user | filter username = \"readonly\"")[0]
    input_data["role"] = "WRITE"
    input_data["password"] = "readonly"
    conn.query(f"put user {json.dumps(input_data)}")
    data = conn.query("get user [\"username\",\"role\"] | ORDERDSC username")
    assert data == expected_data

    # Delete user
    expected_data = [
        {"username": "ceres", "role": "ADMIN"},
    ]
    input_data = [datum[".id"] for datum in conn.query("get user .id | filter username != \"ceres\"")]
    conn.query(f"delete user {json.dumps(input_data)}")
    data = conn.query("get user [\"username\",\"role\"] | ORDERASC username")
    assert data == expected_data
