import socket
from time import sleep

mySocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = ("localhost", 12345)
mySocket.bind(("localhost", 12345))

mySocket.listen(1)
conn, addr = mySocket.accept()
print("Connection from: " + str(addr))

while True:
    with open('fares.csv', 'rb') as csvfile:
        line_count = 0
        for row in csvfile:
            if line_count == 0:
                line_count += 1
                continue
            else:
                conn.send(row)
                line_count += 1
                sleep(0.1)
                print(row)

conn.close

