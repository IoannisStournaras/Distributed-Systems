#!bin/python3

import sys
import socket
import random
from daemons import Daemon
from daemons import Worker
from nodes import Node

#This class holds all the nodes and their respective threads that run on them.
#All join and depart requests go through the first node that joined the circle.
#The other requests are distributed randomly to all nodes.
class Mini_Chord(object):
	def __init__(self):
		self.nodes = []
		self.daemons_ = {}
		self.k='1'
		self.type='even'
	
	def node_join(self, nodeID):
		exists = False
		for node in self.nodes:
			if node.nodeID == nodeID:
				exists = True
				break
		if not exists:
			new_node = Node(nodeID)
			self.nodes.append(new_node)
			self.set()
			self.set_type()
			self.daemons_[new_node.nodeID] = Daemon(new_node, 'run')
			self.daemons_[new_node.nodeID].start()
			#self.daemons_[new_node.nodeID] = Worker(new_node, 'run')
			#self.daemons_[new_node.nodeID].start()
			#The first join needs not to be handled
			if len(self.nodes) is not 1:
				primal = self.nodes[0]
				#Requests to nodes are sent through sockets.
				#In this case all join requests go through
				#the primal node.
				s = socket.socket(
				socket.AF_INET, socket.SOCK_STREAM)
				s.connect(('127.0.0.1', primal.port))
				req = "join(" + nodeID + ")"
				s.sendall(req.encode())
				#reply = s.recv(1024)
				#print(reply)
				s.shutdown(socket.SHUT_RDWR)
				s.close()
		else:
			print("Node %s already exists." % (nodeID))

	def node_depart(self, nodeID):
		#Departure of the primal node is prohibited for simplicity.
		if nodeID == self.nodes[0].nodeID:
			print("Node %s cannot depart because it is "\
			"the primal node." % (nodeID))
		else:
			primal = self.nodes[0]
			found = False
			for node in self.nodes:
				if node.nodeID == nodeID:
					found = True
					self.nodes.remove(node)
					break
			if found:
				#All depart requests also go through
				#the primal node.
				s = socket.socket(
				socket.AF_INET, socket.SOCK_STREAM)
				s.connect(('127.0.0.1', primal.port))
				req = "depart(" + nodeID + ")"
				s.sendall(req.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			else:
				print("Node %s not found." % (nodeID))
 
	def insert(self, req_args,c=None):
		node = random.choice(self.nodes)
		if c!=None:
			if c==0: node.timer_start()
			if c>470: node.timer_stop()
		s = socket.socket(
		socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', node.port))
		req = "insert(" + req_args + ":" + node.nodeID + ":" + self.k + ")"
		#print(req)
		s.sendall(req.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def insert_f(self, req_arg):
		with open(req_arg, 'r') as f:
			c = 0
			for line in f:
				node = random.choice(self.nodes)
				#print("HEREE1111")
				if c == 0: node.timer_start()
				elif c > 470: node.timer_stop()
				s = socket.socket(
				socket.AF_INET, socket.SOCK_STREAM)
				#print("HERE555")
				s.connect(('127.0.0.1', node.port))
				line = line.replace(", ",",")
				line = line.rstrip()
				req = "insert(" + line + ":" + \
				node.nodeID + ":" + self.k + ")"
				s.sendall(req.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
				c = c + 1
		print("Insert from file complete.")
	
	def set(self):
		for node in self.nodes:
			s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			s.connect(('127.0.0.1', node.port))
			req="Replication_factor(" + self.k + ")"
			s.sendall(req.encode())
			s.shutdown(socket.SHUT_RDWR)
			s.close()

	def set_type(self):
		for node in self.nodes:
			s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			s.connect(('127.0.0.1', node.port))
			req="Consistency(" + self.type + ")"
			s.sendall(req.encode())
			s.shutdown(socket.SHUT_RDWR)
			s.close()

	def query(self, key,c=None):
		num_nodes = len(self.nodes)
		#print(num_nodes)
		node = random.choice(self.nodes)
		if c!=None:
			if c==0: node.timer_start()
			if c>470: node.timer_stop()
		s = socket.socket(
		socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', node.port))
		req = "query(" + key + ":" + node.nodeID + ":" + str(num_nodes) + ")"
		s.sendall(req.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def query_f(self, req_arg):
		num_nodes=len(self.nodes)
		with open(req_arg, 'r') as f:
			c=0
			for line in f:
				node = random.choice(self.nodes)
				if c == 0: node.timer_start()
				if c > 470: node.timer_stop()
				s = socket.socket(
				socket.AF_INET, socket.SOCK_STREAM)
				s.connect(('127.0.0.1', node.port))
				line = line.rstrip()
				req = "query(" + line + ":" + node.nodeID + ":" + str(num_nodes) + ")"
				s.sendall(req.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
				c=c+1

	def request(self,req_arg):
		with open(req_arg, 'r') as f:
			c=0
			for line in f:
				line = line.replace(", " , ",")
				line = line.rstrip()
				tipos = line.split(',')[0]
				if (tipos == 'insert'):
					req= line.split(',')[1] + ',' + line.split(',')[2]
					self.insert(req,c)
					c=c+1
				elif (tipos=='query'):
					self.query(line.split(',')[1],c)
					c=c+1
		print('Request from file complete')

	def delete(self, key):
		node = random.choice(self.nodes)
		s = socket.socket(
		socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', node.port))
		req = "delete(" + key + ":" + node.nodeID + ":" + self.k + ")"
		#print(req)
		s.sendall(req.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def delete_all(self):
		num_nodes = len(self.nodes)
		node = random.choice(self.nodes)
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', node.port))
		req = "delete_all(" + str(num_nodes) + ":" + node.nodeID + ")"
		s.sendall(req.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def time_elapsed(self, arg):
		with open(arg, 'rb') as f:
			first = next(f).decode()
			f.seek(-64, 2)
			last = f.readlines()[-1].decode()
			start= first.split(" ")[3]
			stop = last.split(" ")[3]
			elapsed = float(stop) - float(start)
			print("Time elapsed : %s sec." % (str(elapsed))) 

if __name__ == "__main__":
	Mini_Chord = Mini_Chord()
	while 1:
		request = input("Make a request: \n")
		req_type = request.split('(')[0]
		try :
			req_arg = request.split('(')[1]
			req_arg = req_arg[:-1]
			if req_type == "join":
				Mini_Chord.node_join(req_arg)
			elif req_type== "rep":
				Mini_Chord.k=req_arg
				Mini_Chord.set()
			elif req_type == "depart":
				Mini_Chord.node_depart(req_arg)
			elif req_type == "insert":
				Mini_Chord.insert(req_arg)
			elif req_type == "insert_f":
				Mini_Chord.insert_f(req_arg)
			elif req_type == "query":
				Mini_Chord.query(req_arg)
			elif req_type == "query_f":
				Mini_Chord.query_f(req_arg)
			elif req_type == "delete":
				Mini_Chord.delete(req_arg)
			elif req_type == "con":
				Mini_Chord.type = req_arg
				Mini_Chord.set_type()
			elif req_type == "time":
				Mini_Chord.time_elapsed(req_arg)
			elif req_type == "request":
				Mini_Chord.request(req_arg)
			elif req_type == "delete_all":
				Mini_Chord.delete_all()
			else: 
				print("Invalid input.\n"\
				"Usage: join(nodeId) | depart(nodeId) "\
				"| insert(key,value) | query(key) "\
				"| delete(key)" "| Replication_factor(k)")
		except:
			print("Invalid input.\n"\
			"Usage: join(nodeId) | depart(nodeId) "\
			"| insert(key,value) | query(key) "\
			"| delete(key)" "| Replication_factor(k)")
