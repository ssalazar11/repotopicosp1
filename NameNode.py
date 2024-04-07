import socket
import threading
import sys
import json

class NameNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data_nodes = []
        self.block_locations = {}

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f'NameNode listening at {self.host}:{self.port}')

        while True:
            client_socket, _ = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        with client_socket as sock:
            data = sock.recv(4096).decode('utf-8')
            command, _, payload=data.partition('|')
            if command == 'GET_DATA_NODES':
                sock.sendall(json.dumps(self.data_nodes).encode('utf-8'))
            elif command=='GET_BLOCK_LOCATIONS':
                file_name=json.loads(payload)['file_name']
                print(f"Received GET_BLOCK_LOCATIONS for: {file_name}")
                block_locations=self.get_block_locations(file_name)
                print(f"Sending block locations: {block_locations}")
                sock.sendall(json.dumps(block_locations).encode('utf-8'))
            else:
                command, payload = data.split('|', 1)
                if command == 'REGISTER':
                    self.register_datanode(payload)
                    print(f"DataNodes Registered: {self.data_nodes}")
                elif command == 'STORE_BLOCK':
                    block_info = json.loads(payload)
                    self.store_block(block_info['block_name'], block_info['data_node_address'])
                    print(f"Block locations stored: {self.block_locations}")
    
    def get_block_locations(self, file_name):
        return {block_name: self.block_locations[block_name] for block_name in self.block_locations if file_name in block_name}

    def register_datanode(self, address):
        if address not in self.data_nodes:
            self.data_nodes.append(address)
            print(f"Registered DataNode at {address}")

    def store_block(self, block_name, data_node_address):
        print(f"Storing block {block_name} at {data_node_address}")
        self.block_locations.setdefault(block_name, []).append(data_node_address)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python NameNode.py HOST PORT")
        sys.exit(1)
    host, port = sys.argv[1], int(sys.argv[2])
    NameNode(host, port).start_server()