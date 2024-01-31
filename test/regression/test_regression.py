import json
import pytest
import requests
import base64

CERESDB_USERNAME="ceresdb"
CERESDB_PASSWORD="ceresdb"
CERESDB_HOST="localhost"
CERESDB_PORT=7437
CERESDB_URI=f'http://{CERESDB_HOST}:{CERESDB_PORT}/api/query'

def _do_query(query, username="", password=""):
    auth_string = base64.b64encode(f'{CERESDB_USERNAME}:{CERESDB_PASSWORD}'.encode("utf-8")).decode('utf-8')
    if username != "" and password != "":
        auth_string = base64.b64encode(f'{username}:{password}'.encode("utf-8")).decode('utf-8')
    body = {
        "auth": auth_string,
        "query": query
    }
    response = requests.post(CERESDB_URI, json=body)
    return response.json()

def test_create_database():
    expected = None
    assert _do_query('create database foo') == expected
    expected = ['_auth', 'foo']
    assert sorted(_do_query(f'get database')) == sorted(expected)

def test_create_collection():
    schema = {
        "a": "string",
        "b": "int",
        "c": "float",
        "d": "bool"
    }
    expected = None
    assert _do_query(f'create collection bar in foo with schema {json.dumps(schema)}') == expected
    expected = ['bar']
    assert _do_query(f'get collection from foo') == expected

def test_create_user():
    expected = None
    assert _do_query('create user foo with password bar') == expected
    expected = 2
    assert len(_do_query('get user')) == expected

def test_insert_record():
    body = {
        "a": "foobar",
        "b": 0,
        "c": 3.14,
        "d": True
    }
    expected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar') == expected

def test_add_group_user():
    expected = None
    assert _do_query('add group foo to user foo') == expected
    expected = ["foo"]
    assert _do_query('get user')[1]['groups'] == expected

def test_add_group_permission_database():
    expected = None
    assert _do_query('add group foo with permission read to foo') == expected
    expected = ["bar"]
    assert _do_query('get collection from foo', username='foo', password='bar') == expected

def test_add_group_permission_collection():
    expected = None
    assert _do_query('add group foo with permission read to foo.bar') == expected
    expected = [
        {
            "a": "foobar",
            "b": 0,
            "c": 3.14,
            "d": True
        }
    ]
    out = _do_query('get record from foo.bar', username='foo', password='bar')
    del out[0]['_id']
    assert out == expected

def test_delete_group_permission_collection():
    expected = None
    assert _do_query('delete group foo from foo.bar') == expected
    unexpected = None
    assert _do_query('get record from foo.bar', username='foo', password='bar') != unexpected

def test_delete_group_permission_database():
    expected = None
    assert _do_query('delete group foo from foo') == expected
    unexpected = None
    assert _do_query('get collection from foo', username='foo', password='bar') != unexpected

def test_delete_group_user():
    expected = None
    assert _do_query('delete group foo from user foo') == expected
    expected = []
    assert _do_query('get user')[1]['groups'] == expected

def test_add_role_user():
    expected = None
    assert _do_query('add role foo to user foo') == expected
    expected = ["foo"]
    assert _do_query('get user')[1]['roles'] == expected

def test_add_role_permission_database():
    expected = None
    assert _do_query('add role foo with permission read to foo') == expected
    expected = ["bar"]
    assert _do_query('get collection from foo', username='foo', password='bar') == expected

def test_add_role_permission_collection():
    expected = None
    assert _do_query('add role foo with permission read to foo.bar') == expected
    expected = [
        {
            "a": "foobar",
            "b": 0,
            "c": 3.14,
            "d": True
        }
    ]
    out = _do_query('get record from foo.bar', username='foo', password='bar')
    del out[0]['_id']
    assert out == expected

def test_delete_role_permission_collection():
    expected = None
    assert _do_query('delete role foo from foo.bar') == expected
    unexpected = None
    assert _do_query('get record from foo.bar', username='foo', password='bar') != unexpected

def test_delete_role_permission_database():
    expected = None
    assert _do_query('delete role foo from foo') == expected
    unexpected = None
    assert _do_query('get collection from foo', username='foo', password='bar') != unexpected

def test_delete_role_user():
    expected = None
    assert _do_query('delete role foo from user foo') == expected
    expected = []
    assert _do_query('get user')[1]['roles'] == expected

def test_add_user_permission_database():
    expected = None
    assert _do_query('add user foo with permission read to foo') == expected
    expected = ["bar"]
    assert _do_query('get collection from foo', username='foo', password='bar') == expected

def test_add_user_permission_collection():
    expected = None
    assert _do_query('add user foo with permission read to foo.bar') == expected
    expected = [
        {
            "a": "foobar",
            "b": 0,
            "c": 3.14,
            "d": True
        }
    ]
    out = _do_query('get record from foo.bar', username='foo', password='bar')
    del out[0]['_id']
    assert out == expected

def test_delete_user_permission_collection():
    expected = None
    assert _do_query('delete user foo from foo.bar') == expected
    unexpected = None
    assert _do_query('get record from foo.bar', username='foo', password='bar') != unexpected

def test_delete_user_permission_database():
    expected = None
    assert _do_query('delete user foo from foo') == expected
    unexpected = None
    assert _do_query('get collection from foo', username='foo', password='bar') != unexpected

def test_get_record_all():
    body = {
        "a": "hello world",
        "b": 1,
        "c": 1.618,
        "d": False
    }
    expected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar') == expected
    expected = 2
    assert len(_do_query('get record from foo.bar')) == expected

def test_get_record_filter():
    filter1 = {
        "a": "hello world"
    }
    filter2 = {
        "b": 1
    }
    filter3 = {
        "c": 1.618
    }
    filter4 = {
        "d": False
    }
    expected = 1
    assert len(_do_query(f'get record from foo.bar where {json.dumps(filter1)}')) == expected
    expected = 1
    assert len(_do_query(f'get record from foo.bar where {json.dumps(filter2)}')) == expected
    expected = 1
    assert len(_do_query(f'get record from foo.bar where {json.dumps(filter3)}')) == expected
    expected = 1
    assert len(_do_query(f'get record from foo.bar where {json.dumps(filter4)}')) == expected

def test_get_database():
    expected = ['_auth', 'foo']
    assert sorted(_do_query(f'get database')) == sorted(expected)

def test_get_collection():
    expected = ['bar']
    assert _do_query(f'get collection from foo') == expected

def test_get_user():
    expected = 2
    assert len(_do_query('get user')) == expected

def test_get_schema():
    expected = {
        "a": "string",
        "b": "int",
        "c": "float",
        "d": "bool"
    }
    assert _do_query(f'get schema bar from foo') == expected

def test_delete_record_filter():
    filter = {
        "a": "hello world"
    }
    expected = None
    assert _do_query(f'delete record from foo.bar where {json.dumps(filter)}') == expected
    expected = 1
    assert len(_do_query('get record from foo.bar')) == expected
    
def test_update_group_permission_collection():
    body = {
        "a": "hello world",
        "b": 1,
        "c": 1.618,
        "d": False
    }
    expected = None
    assert _do_query(f'add group foo to user foo') == expected
    expected = None
    assert _do_query(f'add group foo with permission read to foo.bar') == expected
    unexpected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') != unexpected
    expected = None
    assert _do_query(f'update group foo permission to write in foo.bar') == expected
    expected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') == expected
    expected = None
    assert _do_query(f'delete group foo from foo.bar') == expected
    expected = None
    assert _do_query(f'delete group foo from user foo') == expected

def test_update_role_permission_collection():
    body = {
        "a": "hello world",
        "b": 1,
        "c": 1.618,
        "d": False
    }
    expected = None
    assert _do_query(f'add role foo to user foo') == expected
    expected = None
    assert _do_query(f'add role foo with permission read to foo.bar') == expected
    unexpected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') != unexpected
    expected = None
    assert _do_query(f'update role foo permission to write in foo.bar') == expected
    expected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') == expected
    expected = None
    assert _do_query(f'delete user foo from foo.bar') == expected
    expected = None
    assert _do_query(f'delete role foo from user foo') == expected

def test_update_user_permission_collection():
    body = {
        "a": "hello world",
        "b": 1,
        "c": 1.618,
        "d": False
    }
    expected = None
    assert _do_query(f'add user foo with permission read to foo.bar') == expected
    unexpected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') != unexpected
    expected = None
    assert _do_query(f'update user foo permission to write in foo.bar') == expected
    expected = None
    assert _do_query(f'insert record {json.dumps(body)} into foo.bar', username='foo', password='bar') == expected
    expected = None
    assert _do_query(f'delete user foo from foo.bar') == expected

def test_update_user_password():
    filter = {
        "b": 0
    }
    expected = None
    assert _do_query(f'update user foo with password bar2') == expected
    expected = 1
    assert len(_do_query(f'get record from foo.bar with {json.dumps(filter)}', username='foo', password='bar2')) == expected

def test_update_record():
    body = {
        "b": 99
    }
    filter1 = {
        "b": 0
    }
    filter2 = {
        "b": 0
    }
    expected = None
    assert _do_query(f'update record foo.bar with {json.dumps(body)} where {json.dumps(filter1)}') == expected
    expected = 1
    assert len(_do_query(f'get record from foo.bar where {json.dumps(filter2)} ', username='foo', password='bar2')) == expected

def test_delete_collection():
    expected = None
    assert _do_query(f'delete collection bar from foo') == expected
    expected = []
    assert _do_query(f'get collection from foo') == expected

def test_delete_database():
    expected = None
    assert _do_query(f'delete database foo') == expected
    expected = ["_auth"]
    assert _do_query(f'get database') == expected

def test_get_record_filter_ops():
    schema = {
        "a": "string",
        "b": "int",
        "c": "float",
        "d": "bool",
        "e": {
            "f": "string"
        }
    }

    records = [
        {
            "a": "a",
            "b": 0,
            "c": 3.14,
            "d": True,
            "e": {
                "f": "f1"
            }
        },
        {
            "a": "a",
            "b": 1,
            "c": 3.14,
            "d": True,
            "e": {
                "f": "f2"
            }
        },
        {
            "a": "a",
            "b": 1,
            "c": 1.618,
            "d": True,
            "e": {
                "f": "f3"
            }
        }
    ]
    filters = [
        # test and
        {
            "a": "a",
            "b": 1
        },
        # test or
        {
            "b": [
                0,
                1
            ]
        },
        # test not
        {
            "$not": {
                "b": 1
            }
        },
        # test nested
        {
            "e": {
                "f": "f1"
            }
        },
        # test gt
        {
            "$gt": {
                "b": 0
            }
        },
        # test gte
        {
            "$gte": {
                "b": 0
            }
        },
        # test lt
        {
            "$lt": {
                "b": 1
            }
        },
        # test lte
        {
            "$lte": {
                "b": 1
            }
        },
        # test multiple
        {
            "$or": {
                "e": {
                    "f": "f1"
                },
                "b": 1
            }
        }
    ]
    expected = None
    assert _do_query('create database foo') == expected
    expected = None
    assert _do_query(f'create collection bar in foo with schema {json.dumps(schema)}') == expected
    expected = None
    assert _do_query(f'insert record {json.dumps(records)} into foo.bar') == expected
	
    
    expected = [
        2,
        3,
        1,
        1,
        2,
        3,
        1,
        3,
        3
    ]
    for idx, filter in enumerate(filters):
        assert len(_do_query(f'get record from foo.bar where {json.dumps(filter)}')) == expected[idx]
    expected = None
    assert _do_query(f'delete database foo') == expected

def test_get_record_order():
    schema = {
        "a": "string",
        "b": "int",
        "c": "float",
        "d": "bool",
        "e": {
            "f": "string"
        }
    }

    records = [
        {
            "a": "a",
            "b": 0,
            "c": 3.14,
            "d": True,
            "e": {
                "f": "f1"
            }
        },
        {
            "a": "b",
            "b": 1,
            "c": 3.15,
            "d": True,
            "e": {
                "f": "f2"
            }
        },
        {
            "a": "c",
            "b": 2,
            "c": 3.16,
            "d": True,
            "e": {
                "f": "f3"
            }
        }
    ]
    orders = [
        [
            'ascending',
            'a'
        ],
        [
            'descending',
            'a'
        ],
        [
            'ascending',
            'b',
        ],
        [
            'descending',
            'b'
        ],
        [
            'ascending',
            'c'
        ],
        [
            'descending',
            'c'
        ],
        [
            'ascending',
            'e.f'
        ],
        [
            'descending',
            'e.f'
        ]
    ]
    expected = None
    assert _do_query('create database foo') == expected
    expected = None
    assert _do_query(f'create collection bar in foo with schema {json.dumps(schema)}') == expected
    expected = None
    assert _do_query(f'insert record {json.dumps(records)} into foo.bar') == expected
    expected = [
        [0,1,2],
        [2,1,0],
        [0,1,2],
        [2,1,0],
        [0,1,2],
        [2,1,0],
        [0,1,2],
        [2,1,0]
    ]
    for idx, order in enumerate(orders):
        out = _do_query(f'get record from foo.bar ordered {order[0]} by {order[1]}')
        for jdx, el in enumerate(out):
            if el['b'] != expected[idx][jdx]:
                assert order == out
    expected = None
    assert _do_query(f'delete database foo') == expected

