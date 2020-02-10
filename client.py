import sys
import pymongo
from kafka import KafkaConsumer
from json import loads
from time import sleep
from json import dumps
from kafka import KafkaProducer
import threading 
import socket
import os

def get_data_base(username):
	myclient = pymongo.MongoClient("mongodb://localhost:27017/")
	mydb = myclient[username]
	return mydb


def get_producer():
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))
	return producer


def get_consumer(username):
	consumer = KafkaConsumer(username,bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',enable_auto_commit=True,group_id=username,value_deserializer=lambda x: loads(x.decode('utf-8')))
	return consumer


def print_previous_messages(frnd):
	chat_conv = database[frnd]
	for message in chat_conv.find():
		print("{} : {}".format(message['who'],message['msg']))
	return


def get_messages():
	global consumer,database,current_friend,exit_from_thread
	for message in consumer:
		message = message.value
		
		frnd = message['frnd']
		message = message['msg']
		chat_conv = database[frnd]
		chat_conv.insert_one({'who':frnd,'msg':message})
		if current_friend==frnd:
			print(frnd," : ", message)
		elif current_friend!="$$":
			beep()
		if exit_from_thread:
			break


def message_mode(user,frnd):
	global producer,current_friend,database
	print_previous_messages(frnd)
	current_friend= frnd
	while True:
		message = input()
		if message == "exit":
			break
		data = {'frnd':user,'msg' : message}
		chat_conv = database[frnd]
		chat_conv.insert_one({'who':'Me','msg':message})
		producer.send(frnd, value=data)
	current_friend = "$$"
	return




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

def beep():
	duration = 1 
	freq = 440  
	os.system('play -nq -t alsa synth {} sine {}'.format(duration, freq))

def login(sock):
	global username
	username = input("Enter UserName : ")
	uname = username
	pswd = input("Enter Password : ")
	send_string(sock,"login")
	send_string(sock,uname)
	send_string(sock,pswd)
	rec = recv_string(sock)
	if rec=="OK":
		print("Logged In")
		return True
	else:
		print("Wrong Crediantials")
	return False
	
def signin(sock):
	uname = input("Enter UserName : ")
	pswd = input("Enter Password : ")
	send_string(sock,"signin")
	send_string(sock,uname)
	send_string(sock,pswd)
	rec = recv_string(sock)
	print(rec)
	if rec=="OK":
		print("Account Created Successfully")
def is_valid_user(user,sock):
	send_string(sock,"isvalid")
	send_string(sock,user)
	rec = recv_string(sock)
	if rec=="OK":
		return True
	else:
		return False
def handle_message(sock):
	while(True):
		frnd = input("Enter Friends Username : ")
		if frnd=="exit":
			break
		if is_valid_user(frnd,sock):
			message_mode(username,frnd)
		else:
			print("Not Valid User")
		return

def users(sock):
	send_string(sock,"users")
	leng = recv_int(sock)
	for i in range(leng):
		print(recv_string(sock))
	print("\n\n")

def logged_in_mode(sock):
	global database,producer,consumer,current_friend,exit_from_thread
	database = get_data_base(username)
	producer = get_producer()
	consumer = get_consumer(username)
	current_friend = "$$"
	t1 = threading.Thread(target=get_messages)
	exit_from_thread = False
	t1.start()
	while True:
		print("choice \n users \t\tmessage \t\t exit")
		choice = input()
		if choice=="users":
			users(sock)
		elif choice=="message":
			handle_message(sock)
			current_frnd = "$$"
		elif choice == "exit":
			break
	exit_from_thread = True
	t1.join()
print("Enter Port of Server")
port = int(input())
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost",port))

First = True
database = None
producer = None
consumer = None
current_friend = None

while First:
	print("choice \n login \t\tsignin \t\t exit")
	choice = input()
	if choice == "login":
		if(login(sock)):
			First = False
			logged_in_mode(sock)
	elif choice == "signin":
		signin(sock)
	elif choice == "exit":
		break
sock.close()

print("Application Closed")
