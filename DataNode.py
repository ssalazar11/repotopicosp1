import socket
import threading
import os
import sys
import json

class DataNode:
    def __init__(self, host, port, namenode_host, namenode_port):
        self.host = host
        self.port = port
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port
        self.data_directory = f"data_node_dir_{port}/"
        os.makedirs(self.data_directory, exist_ok=True)

    def register_with_namenode(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.namenode_host, self.namenode_port))
                message = f'REGISTER|{self.host}:{self.port}'
                sock.sendall(message.encode('utf-8'))
        except Exception as e:
            print(f"Failed to register with NameNode: {e}")

    def start_server(self):
        self.register_with_namenode()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f'DataNode listening at {self.host}:{self.port}')

        while True:
            client_socket, _ = server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        with client_socket as sock:
            try:
                data = sock.recv(4096).decode('utf-8')
                message = json.loads(data)
                command = message['command']
                block_name = message['block_name']
                
                if command == 'STORE':
                    block_data = bytes.fromhex(message['block_data'])
                    self.store_block(block_name, block_data)
                    response = f"Block {block_name} stored successfully."
                    client_socket.sendall(response.encode('utf-8'))
                elif command == 'RETRIEVE':
                    self.retrieve_block(block_name, client_socket)
            except Exception as e:
                print(f"An error has occurred while handling the client: {e}")

    def retrieve_block(self, block_name, client_socket):
        path=os.path.join(self.data_directory, block_name)
        if os.path.exists(path):
            with open(path, 'rb') as block_file:
                block_data=block_file.read()
                response={'status':'success', 'block_data':block_data.hex()}
        else:
            response={'status':'Error', 'message':f"block {block_name} not found"}
        client_socket.sendall(json.dumps(response).encode('utf-8'))

    def store_block(self, block_name, block_data):
        path = os.path.join(self.data_directory, block_name)
        with open(path, 'wb') as block_file:
            block_file.write(block_data)
        print(f"Stored block {block_name}.")
        self.report_block_to_namenode(block_name)

    def report_block_to_namenode(self, block_name):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.namenode_host, self.namenode_port))
                block_info = json.dumps({'block_name': block_name, 'data_node_address': f'{self.host}:{self.port}'})
                message = f'STORE_BLOCK|{block_info}'
                sock.sendall(message.encode('utf-8'))
        except Exception as e:
            print(f"Failed to report block to NameNode: {e}")

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("Usage: python DataNode.py HOST PORT NAMENODE_HOST NAMENODE_PORT")
        sys.exit(1)
    host, port = sys.argv[1], sys.argv[2]
    namenode_host, namenode_port = sys.argv[3], int(sys.argv[4])
    DataNode(host, int(port), namenode_host, namenode_port).start_server()