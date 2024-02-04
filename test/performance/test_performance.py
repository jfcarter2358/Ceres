import json
import requests
import base64
import time
import random
import matplotlib.pyplot as plt
import numpy as np

CERESDB_USERNAME="ceresdb"
CERESDB_PASSWORD="ceresdb"
CERESDB_HOST="localhost"
MAX_B=1000
N_RECORDS=10000
SAMPLE_SIZE = 1
AVERAGE_WINDOW = 4

def _do_query(query, port: int):
    uri=f'http://{CERESDB_HOST}:{port}/api/query'
    auth_string = base64.b64encode(f'{CERESDB_USERNAME}:{CERESDB_PASSWORD}'.encode("utf-8")).decode('utf-8')
    body = {
        "auth": auth_string,
        "query": query
    }
    response = requests.post(uri, json=body)
    if response.status_code != 200:
        raise ValueError(f'status code is not 200 for query: {query}')
    return response.json(), int(response.elapsed.total_seconds() * 1000)

def mov_avg(x):
    # for m in range(len(x)-(AVERAGE_WINDOW-1)):
    #     yield sum(np.ones(AVERAGE_WINDOW) * x[m:m+AVERAGE_WINDOW]) / AVERAGE_WINDOW 
    m = len(x) - (AVERAGE_WINDOW - 1) - 1
    return sum(np.ones(AVERAGE_WINDOW) * x[m:m+AVERAGE_WINDOW]) / AVERAGE_WINDOW

def create_database(port: int):
    _do_query('create database foo', port)

def create_collection(port: int):
    schema = {
        "a": "int",
        "b": "int",
    }
    _do_query(f'create collection bar in foo with schema {json.dumps(schema)}', port)

def insert_record(port: int):
    body = {
        "a": int(time.time()),
        "b": random.randint(0, MAX_B)
    }
    
    query = f'insert record {json.dumps(body)} into foo.bar'
    
    return _do_query(query, port)

def get_record(port: int, cold_storage=False, filter=None, order="", order_field=""):
    db_prefix = "$coldstorage__" if cold_storage else ""
    
    query = f'get record from {db_prefix}foo.bar'
    
    if filter:
        query = f'{query} where {json.dumps(filter)}'

    if order:
        query = f'{query} order {order} {order_field}'

    return _do_query(f'{query} count', port)

def run_test(test_name: str, port: int):
    create_database(port)
    create_collection(port)

    # cold storage
    crt =   [] # all
    cfrt =  [] # filter
    cort =  [] # order
    cfort = [] # filter order

    # cold storage moving average
    acrt =   [] # all
    acfrt =  [] # filter
    acort =  [] # order
    acfort = [] # filter order

    # index
    irt =   [] # all
    ifrt =  [] # filter
    iort =  [] # order
    ifort = [] # filter order

    # index moving average
    airt =   [] # all
    aifrt =  [] # filter
    aiort =  [] # order
    aifort = [] # filter order

    xs = []

    filter = {"$lte": {"b": 50}}
    order = 'ascending'
    order_field = 'b'


    for idx in range(0, N_RECORDS):
        insert_record(port)
        print(f'Record {idx+1} of {N_RECORDS}.......', end=".")

        tcrt =   []
        tcfrt =  []
        tcort =  []
        tcfort = []

        tirt =   []
        tifrt =  []
        tiort =  []
        tifort = []

        for jdx in range(0, SAMPLE_SIZE):
            _, timing = get_record(port, cold_storage=True)
            tcrt.append(timing)
            _, timing = get_record(port, cold_storage=True, filter=filter)
            tcfrt.append(timing)
            # _, timing = get_record(cold_storage=True, order=order, order_field=order_field)
            # tcort.append(timing)
            # _, timing = get_record(cold_storage=True, filter=filter, order=order, order_field=order_field)
            # tcfort.append(timing)
            _, timing = get_record(port)
            tirt.append(timing)
            _, timing = get_record(port, filter=filter)
            tifrt.append(timing)
            # _, timing = get_record(order=order, order_field=order_field)
            # tiort.append(timing)
            # _, timing = get_record(filter=filter, order=order, order_field=order_field)
            # tifort.append(timing)

        print('\u2705')

        crt.append(np.average(tcrt))
        cfrt.append(np.average(tcfrt))
        # cort.append(np.average(tcort))
        # cfort.append(np.average(tcfort))

        irt.append(np.average(tirt))
        ifrt.append(np.average(tifrt))
        # iort.append(np.average(tiort))
        # ifort.append(np.average(tifort))
        # acrt.append(mov_avg(crt))
        # acfrt.append(mov_avg(cfrt))
        # acort.append(mov_avg(tcort))
        # acfort.append(mov_avg(tcfort))

        # airt.append(mov_avg(irt))
        # aifrt.append(mov_avg(ifrt))
        # aiort.append(mov_avg(tiort))
        # aifort.append(mov_avg(tifort))

        xs.append(idx)

    ax = plt.axes()

    line_crt, = ax.plot(xs, crt, '#ffff00', label='Cold storage')
    line_cfrt, = ax.plot(xs, cfrt, '#ff8800', label='Cold storage with filter')
    # line_cort, = ax.plot(xs, cort, '#ff0000', label='cort')
    # line_cfort, = ax.plot(xs, cfort, '#ff0088', label='cfort')

    line_irt, = ax.plot(xs, irt, '#00ff00', label='Index')
    line_ifrt, = ax.plot(xs, ifrt, '#00ffff', label='Index with filter')
    # line_iort, = ax.plot(xs, iort, '#0088ff', label='iort')
    # line_ifort, = ax.plot(xs, ifort, '#0000ff', label='ifort')

    # line_acrt, = ax.plot([x for x in range(0, len(acrt))], acrt, '#ffff00', label='avg cold storage', linestyle='dashed')
    # line_acfrt, = ax.plot([x for x in range(0, len(acfrt))], acfrt, '#ff8800', label='avg cold storage with filter', linestyle='dashed')
    # line_acort, = ax.plot(xs, [acort for _ in range(0, SAMPLE_SIZE)], '#ff0000', label='acort')
    # line_acfort, = ax.plot(xs, [acfort for _ in range(0, SAMPLE_SIZE)], '#ff0088', label='acfort')

    # line_airt, = ax.plot([x for x in range(0, len(airt))], airt, '#00ff00', label='avg index', linestyle='dashed')
    # line_aifrt, = ax.plot([x for x in range(0, len(aifrt))], aifrt, '#00ffff', label='avg index with filter', linestyle='dashed')
    # line_aiort, = ax.plot(xs, [aiort for _ in range(0, SAMPLE_SIZE)], '#0088ff', label='aiort')
    # line_aifort, = ax.plot(xs, [aifort for _ in range(0, SAMPLE_SIZE)], '#0000ff', label='aifort')
    
    # ax.legend(handles=[line_crt, line_cfrt, line_cort, line_cfort, line_irt, line_ifrt, line_iort, line_ifort, line_acrt, line_acfrt, line_acort, line_acfort, line_airt, line_aifrt, line_aiort, line_aifort])
    # ax.legend(handles=[line_crt, line_cfrt, line_acrt, line_acfrt, line_irt, line_ifrt, line_airt, line_aifrt])
    ax.set_title('Record Count vs. Response Time for Query Configurations', fontweight ="bold") 
    ax.legend(handles=[line_crt, line_cfrt, line_irt, line_ifrt])
    ax.set_xlabel('Record Count')
    ax.set_ylabel('Respone time (ms)')
    # plt.show()

    plt.savefig(f'graphs/performance_{test_name}.png')

def test_old():
    run_test('old', 7441)

def test_low_index_low_storage():
    run_test('low_index_low_storage', 7437)

def test_low_index_high_storage():
    run_test('low_index_high_storage', 7438)

def test_high_index_low_storage():
    run_test('high_index_low_storage', 7439)

def test_high_index_high_storage():
    run_test('high_index_high_storage', 7440)

