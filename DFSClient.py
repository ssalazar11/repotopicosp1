import socket
import sys
import os
import json

class DFSClient:
    def __init__(self, namenode_host, namenode_port):
        self.namenode_host = namenode_host
        self.namenode_port = namenode_port

    def retrieve_file(self, file_name):
        block_locations=self.get_block_locations(file_name)
        print("Block locations:", block_locations)
        if not block_locations:
            print(f"Could not retrieve block for {file_name}")
            return
        with open(file_name, 'wb') as f:
            for block_name, data_node_address in block_locations.items():
                block_data=self.retrieve_block(block_name, data_node_address)
                if block_data is not None:
                    f.write(block_data)
                else:
                    print(f"Failed to retrieve block {block_name}")
                    return
        print(f"File {file_name} has been succesfully retrieved")

    def get_block_locations(self, file_name):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.namenode_host, self.namenode_port))
                request = f'GET_BLOCK_LOCATIONS|{json.dumps({"file_name": file_name})}'
                sock.sendall(request.encode('utf-8'))
                response = sock.recv(4096).decode('utf-8')
                if response:
                    block_locations = json.loads(response)
                    return block_locations
                else:
                    print("No response received from NameNode")
                    return {}
        except Exception as e:
            print(f"An error occurred while getting block locations: {e}")
            return {}
        
    def retrieve_block(self, block_name, data_node_address):
        data_node_host, data_node_port = data_node_address[0].split(':') if isinstance(data_node_address, list) else data_node_address.split(':')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((data_node_host, int(data_node_port)))
                request = json.dumps({'command': 'RETRIEVE', 'block_name': block_name})
                sock.sendall(request.encode('utf-8'))
                response_data = sock.recv(4096)
                response = json.loads(response_data.decode('utf-8'))
                if response['status'] == 'success':
                    block_data = bytes.fromhex(response['block_data'])
                    return block_data
                else:
                    print(f"Error retrieving block {block_name}: {response['message']}")
                    return None
        except Exception as e:
            print(f"Error retrieving block {block_name}: {e}")
            return None

    def store_file(self, file_path):
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, 'rb') as f:
                file_data = f.read()
        except FileNotFoundError:
            print(f"File {file_path} does not exist.")
            return

        block_size = 1024  # Set the block size to 1 KB for the example
        for i in range(0, len(file_data), block_size):
            block_name = f"{file_name}_block_{i//block_size}"
            block_data = file_data[i:i+block_size]
            print(f"Storing block: {block_name}")
            data_nodes = self.get_data_nodes()
            if data_nodes:
                self.store_block(block_name, block_data, data_nodes)

    def get_data_nodes(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.namenode_host, self.namenode_port))
                sock.sendall(b'GET_DATA_NODES')
                response = sock.recv(4096).decode('utf-8')
                data_nodes = json.loads(response)
                return data_nodes
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    def store_block(self, block_name, block_data, data_nodes):
        for node in data_nodes:
            self.send_data('STORE', block_name, block_data, node)

    def send_data(self, command, block_name, block_data, data_node_address):
        data_node_host, data_node_port = data_node_address.split(':')
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((data_node_host, int(data_node_port)))
                print(f"Sending {command} command for block {block_name} to DataNode {data_node_address}")
                message = {'command': command, 'block_name': block_name, 'block_data': block_data.hex()}
                sock.sendall(json.dumps(message).encode('utf-8'))
                response = sock.recv(4096).decode('utf-8')
                print(response)
        except Exception as e:
            print(f"Error sending data to DataNode {data_node_address}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python DFSClient.py NAMENODE_HOST NAMENODE_PORT STORE [FILE_PATH]")
        sys.exit(1)

    namenode_host, namenode_port = sys.argv[1], int(sys.argv[2])
    action = sys.argv[3].upper()
    file_path = sys.argv[4]

    client = DFSClient(namenode_host, namenode_port)

    if action == 'STORE':
        client.store_file(file_path)
    elif action=='RETRIEVE':
        client.retrieve_file(file_path)