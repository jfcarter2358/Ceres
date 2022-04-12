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

    get_send_x = [(i+1) * max_count for i in range(0, iterations)]
    get_send_y = [float(datum["send"]) for datum in timings_get]
    get_process_x = [(i+1) * max_count for i in range(0, iterations)]
    get_process_y = [float(datum["process"]) for datum in timings_get]
    get_receive_x = [(i+1) * max_count for i in range(0, iterations)]
    get_receive_y = [float(datum["receive"]) for datum in timings_get]

    post_send_x = [(i+1) * max_count for i in range(0, iterations)]
    post_send_y = [float(datum["send"]) for datum in timings_post]
    post_process_x = [(i+1) * max_count for i in range(0, iterations)]
    post_process_y = [float(datum["process"]) for datum in timings_post]
    post_receive_x = [(i+1) * max_count for i in range(0, iterations)]
    post_receive_y = [float(datum["receive"]) for datum in timings_post]

    plt.plot(get_send_x, get_send_y, label = "Send")
    plt.plot(get_process_x, get_process_y, label = "Process")
    plt.plot(get_receive_x, get_receive_y, label = "Receive")
    plt.legend()
    plt.savefig('get.png')
    plt.clf()

    plt.plot(post_send_x, post_send_y, label = "Send")
    plt.plot(post_process_x, post_process_y, label = "Process")
    plt.plot(post_receive_x, post_receive_y, label = "Receive")
    plt.legend()
    plt.savefig('post.png')
