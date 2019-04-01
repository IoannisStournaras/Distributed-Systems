#!bin/python3
import socket
import sys
import threading
import time

class Daemon(threading.Thread):
	def __init__(self, obj, method):
		threading.Thread.__init__(self)
		self.obj_ = obj
		self.method_ = method

	def run(self):
		getattr(self.obj_, self.method_)()


class test(object):
	def __init__(self):
		self.socktest = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.kati = None
		
	def connect(self,port):
		retry_count = 0
		while retry_count < 100:
			try:
				self.socktest.connect(('127.0.0.1', port))
				break
			except:
				time.sleep(retry_count)
				retry_count+= 1
		if retry_count == 100:
			print("UPPER LIMIT REACHED")		


	def run(self):
		self.connect(self.kati)
		msg = 'eftasa'
		data1 =self.socktest.recv(2048)
		#msg =str(data1) + msg
		msg = msg.encode('utf-8')
		while 1: 
			self.socktest.send(msg)
		sys.exit(1)
