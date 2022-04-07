import socket
import sys
import json
import readline
import time

username = sys.argv[1]
password = sys.argv[2]
port_string = sys.argv[3]

host = socket.gethostname()
port = int(port_string)
in_string = input(">  ")

while in_string != "exit()":
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    payload = {
        "_auth": f"{username}:{password}",
        "query": in_string
    }
    s.sendall((json.dumps(payload)+'\n').encode('utf-8'))
    start_time = time.time()
    output_string = ''
    data = s.recv(1024)
    if not data:
        print('E  The server closed the connection unexpectedly')
    output_string += data.decode('utf-8')
    while not data.decode('utf-8').endswith('EOD'):
        data = s.recv(1024)
        output_string += data.decode('utf-8')
    output_string = output_string[:-3]
    s.close()
    end_time = time.time()
    if output_string != 'null':
        parsed = json.loads(output_string)
        if type(parsed) == dict:
            if 'error' in parsed.keys():
                print(f'  E  {parsed["error"]}')
            else:
                print('  <  ', output_string)
        else:
            print('  <  ', output_string)
    print("...  %s seconds" % (time.time() - start_time))
    in_string = input(">  ")
