import socket
import threading 
users ={}
def send_int(sock,value):
	value = str(value)
	i = len(value)
	while i < 5:
		value+=" "
		i+=1
	sock.send(value.encode())
def recv_ack(sock):
	value = sock.recv(5)
	return
def send_ack(sock):
	sock.send("12345".encode())
	return
def recv_int(sock):
	value = sock.recv(5).decode()
	value = value.strip()
	return int(value)
def send_string(sock,value):
	send_int(sock,len(value))
	recv_ack(sock)
	sock.send(value.encode())
	recv_ack(sock)
	return
def recv_string(sock):
	length = recv_int(sock)
	send_ack(sock)
	value = sock.recv(length)
	send_ack(sock)
	return value.decode()
def handle_login(conn):
	global users
	username = recv_string(conn)
	password = recv_string(conn)
	if username in users and users[username]==password:
		send_string(conn,"OK")
	else:
		send_string(conn,"NO")
	return
def handle_signin(conn):
	global users
	username = recv_string(conn)
	password = recv_string(conn)

	users[username] = password 
	send_string(conn,"OK")
	return
def handle_users(conn):
	global users
	send_int(conn,len(users.keys()))
	for x in users:
		send_string(conn,x)
	return

def handle_isvalid(conn):
	usr = recv_string(conn)
	if usr in users:
		send_string(conn,"OK")
	else:
		send_string(conn,"NO")

def client(conn):
	while(True):
		print("waiting for query")
		query = recv_string(conn)
		print(query)
		if query=="login":
			handle_login(conn)
		elif query=="signin":
			handle_signin(conn)
		elif query=="users":
			handle_users(conn)
		elif query=="isvalid":
			handle_isvalid(conn)

	return
print("Enter PORT")
port = int(input())
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.bind(("localhost",port))
sock.listen(2)
while True:
	conn, addr = sock.accept()
	t1 = threading.Thread(target=client,args=(conn,))
	t1.start()



