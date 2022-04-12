from ceresdb_python import __version__
from ceresdb_python import Connection
import json
import pytest

CERES_USERNAME="ceres"
CERES_PASSWORD="ceres"
CERES_HOST="localhost"
CERES_PORT=7437

def test_stress_1000():
    conn = Connection(CERES_USERNAME, CERES_PASSWORD, CERES_HOST, CERES_PORT)
    conn.query("post database foo")
    conn.query("post collection foo.bar {\"foo\":\"STRING\",\"idx\":\"INT\"}")
    max_count = 1000

    # populate with records
    timings_post = []
    timings_get = []
    for i in range(0, 100):
        input_data = [{"foo": "bar", "idx": j} for j in range(0, max_count)]
        _, timing = conn.timed_query(f"post record foo.bar {json.dumps(input_data)}")
        timings_post.append(timing)
        _, timing = conn.timed_query(f"get record foo.bar *")
        timings_get.append(timing)
    
    with open(f'timing_{max_count}_post.json', 'w') as f:
        json.dump(timings_post, f, indent=4)
    with open(f'timing_{max_count}_get.json', 'w') as f:
        json.dump(timings_get, f, indent=4)

    conn.query("delete collection foo.bar")
    conn.query("delete database foo")
