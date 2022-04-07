import socket
import sys
import json
import readline

username = 'ceres'
password = 'ceres'
port_string = '7437'

host = socket.gethostname()
port = int(port_string)

def communicate(username, password, query):
    payload = {
        "_auth": f"{username}:{password}",
        "query": query
    }

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    s.sendall((json.dumps(payload)+'\n').encode('utf-8'))
    output_string = ''
    data = s.recv(1024)
    if not data:
        print('E  The server closed the connection unexpectedly')
        sys.exit(1)
    output_string += data.decode('utf-8')
    while not data.decode('utf-8').endswith('EOD'):
        data = s.recv(1024)
        output_string += data.decode('utf-8')
    output_string = output_string[:-3]
    s.close()
    if output_string != 'null':
        parsed = json.loads(output_string)
        if type(parsed) == dict:
            if 'error' in parsed.keys():
                print(f'  E  {parsed["error"]}')
                sys.exit(1)
            else:
                print('  <  ', output_string)
        else:
            print('  <  ', output_string)

communicate(username, password, 'DBADD foo')
communicate(username, password, 'COLADD foo.bar {"foo":"STRING","index":"INT"}')

for i in range(0, 1000):
    print(f"Iteration {i+1} of 1000")
    data = json.dumps(list([{"foo":"bar","index":j} for j in range(0, 1000)]))
    
    communicate(username, password, f'POST foo.bar {data}')
