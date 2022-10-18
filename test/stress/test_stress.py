from ceresdb_python import __version__
from ceresdb_python import Connection
import json
import pytest
import matplotlib.pyplot as plt

CERESDB_USERNAME="ceresdb"
CERESDB_PASSWORD="ceresdb"
CERESDB_HOST="localhost"
CERESDB_PORT=7437

def test_stress_1000():
    conn = Connection(CERESDB_USERNAME, CERESDB_PASSWORD, CERESDB_HOST, CERESDB_PORT)
    conn.query("post database foo")
    conn.query("post collection foo.bar {\"foo\":\"STRING\",\"idx\":\"INT\"}")
    max_count = 1000
    iterations = 100

    # populate with records
    timings_post = []
    timings_get = []
    for i in range(0, iterations):
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

    get_x = [(i+1) * max_count for i in range(0, iterations)]
    get_y = [float(datum) for datum in timings_get]

    post_x = [(i+1) * max_count for i in range(0, iterations)]
    post_y = [float(datum) for datum in timings_post]

    plt.plot(get_x, get_y)
    plt.legend()
    plt.savefig('get.png')
    plt.clf()

    plt.plot(post_x, post_y)
    plt.legend()
    plt.savefig('post.png')
