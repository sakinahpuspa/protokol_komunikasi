# Simulator acc

import socket
import sys
import time

# Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Bind the socket to the port
server_address = ('127.0.0.1', 10002)
print ('starting up on %s port %s' % server_address)
sock.bind(server_address)

# Listen for incoming connections
sock.listen(1)


while True:
    # Wait for a connection
    print('waiting for a connection')
    connection, client_address = sock.accept()

    try:
        print('connection from', client_address)

        # Receive the data in small chunks and retransmit it
        while True:

            x = "-1035.86"
            y = "15.68"
            z = "-11.76"
            tail = "0"
            data_to_send = x + "," + y + "," + z + "," + tail

            connection.sendall(data_to_send.encode())
            print("send " + data_to_send + "\n")

            time.sleep(0.007)
            
    finally:
        # Clean up the connection
        connection.close()
