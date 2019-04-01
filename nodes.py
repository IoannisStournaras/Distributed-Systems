#!bin/python3

import sys
import hashlib
import time
from daemons import Daemon
import socket
from threading import Lock

class Node(object):
	def __init__(self, nodeID):
		self.mutex={}
		self.rep_tot = '1'
		self.type = None
		self.nodeID = nodeID
		self.ID = self.hash_sha1(nodeID)
		#Each node has a server socket
		#which listens to a unique address
		#addr = (('127.0.0.1'), self.port)
		self.port = 3000 + int(nodeID)
		self.socket_ = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.bind(('127.0.0.1', self.port))
		self.socket_.listen(75)
		#These pointers will hold the proper nodeIDs
		#so that each node knows how to reach its 
		#neighbors
		self.predecessor = None
		self.successor = None
		self.database = {}
		self.t_stop = False
		self.query_done = False

	#This function just returns a given string hashed 
	#in the form of a bytes array
	def hash_sha1(self, s):
		m = hashlib.sha1()
		s = s.encode('utf-8')
		m.update(s)
		return m.digest()

	def set_rep(self,k):
		self.rep_tot=k
		print(self.rep_tot)

	#The node salutes its (new) successor so that it knows who its
	#new predecessor is.
	def send_salute(self):
		s = socket.socket(
			socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', 3000 + int(self.successor)))
		salute = "salute(" + self.nodeID + ")"
		s.sendall(salute.encode())
		#reply = s.recv(1024)
		#print(reply)
		s.shutdown(socket.SHUT_RDWR)
		s.close()
	
	#When a salute reaches the node, it has a new predecessor
	def recv_salute(self, nodeID, conn=None):
		self.predecessor = nodeID
		print("Node %s says: New Predecessor = %s."\
				% (self.nodeID, self.predecessor))
		#conn.sendall(b'Salute successful!')
	
	#When a node needs to be informed about who its new successor is
	def set_successor(self, new_nodeID, suc_nodeID):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', 3000 + int(new_nodeID)))
		new_suc = "new_successor(" + suc_nodeID + ")"
		s.sendall(new_suc.encode())
		#reply = s.recv(1024)
		#print(reply)
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	#This node has a new successor. It salutes the successor
	#so that it knows of its new predecessor
	#Einai panta o neos komvos plhn periptwshs depart
	def new_successor(self, nodeID, conn=None):
		req_keys = False
		if self.successor == None:
			req_keys = True
		self.successor = nodeID
		print("Node %s says: New Successor = %s."\
				%(self.nodeID, self.successor))
		#conn.sendall(b'Successor set successfully')
		self.send_salute()
		#time.sleep(1)
		if req_keys:
			self.get_keys(self.successor)

	def send_keys(self, suc_ID):
		check=int(self.rep_tot)
		for key in self.database:
			check2=int(self.database[key][1])+1
			if (check==check2):
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(suc_ID)))
				data = "insert(" + key + "," + self.database[key][0] + \
				":" + suc_ID + ":" + str(self.database[key][1]+1) + ")"
				#print(data)
				s.sendall(data.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			else:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(suc_ID)))
				data = "replica(" + key + "," + self.database[key][0] + \
				":" + suc_ID + ":" + str(int(self.database[key][1])+1) + ")"
				#print(data)
				s.sendall(data.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()

	def get_keys(self, suc_ID):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', 3000 + int(suc_ID)))
		req = "get_keys(" + self.nodeID + ")"
		s.sendall(req.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def handle_get_keys(self, req_ID, conn=None):
		hash_req_ID = self.hash_sha1(req_ID)
		check=int(self.rep_tot)-1
		if self.database != {}:
			remove = {}
			for key in self.database:
				check2=int(self.database[key][1])
				if (self.hash_sha1(key) <= hash_req_ID and hash_req_ID<self.ID and check==check2):
					s=socket.socket(socket.AF_INET,
					socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + \
					int(req_ID)))
					data = "insert(" + key + "," + self.database[key][0] + ":" + req_ID + ":" + str(self.database[key][1]+1) + ")"
					#print(data)
					s.sendall(data.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
					remove[key]=self.database[key]
				elif (self.hash_sha1(key)<=hash_req_ID and hash_req_ID > self.ID and self.hash_sha1(key)>self.ID and check==check2):
					s=socket.socket(socket.AF_INET,
					socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + \
					int(req_ID)))
					data = "insert(" + key + "," + self.database[key][0] + ":" + req_ID + ":" + str(self.database[key][1]+1) + ")"
					#print(data)
					s.sendall(data.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
					remove[key]=self.database[key]
				elif (self.hash_sha1(key)>self.ID and self.ID>hash_req_ID and self.hash_sha1(key)>hash_req_ID and check==check2):
					s=socket.socket(socket.AF_INET,
					socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + \
					int(req_ID)))
					data = "insert(" + key + "," + self.database[key][0] + ":" + req_ID + ":" + str(self.database[key][1]+1) + ")"
					#print(data)
					s.sendall(data.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
					remove[key]=self.database[key]
				elif (check2<check):
					s=socket.socket(socket.AF_INET,
					socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + \
					int(req_ID)))
					data = "replica(" + key + "," + self.database[key][0] + ":" + req_ID + ":" + str(int(self.database[key][1])+1) + ")"
					#print(data)
					s.sendall(data.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
					remove[key]=self.database[key]
			for k in remove: del self.database[k]


	#When a node is not responsible for a request, it just forwards
	#the request to the successor
	def forward_req(self, request):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', 3000 + int(self.successor)))	
		s.sendall(request.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()

	def backward_req(self, request):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(('127.0.0.1', 3000 + int(self.predecessor)))
		s.sendall(request.encode())
		s.shutdown(socket.SHUT_RDWR)
		s.close()
		print(request)		
	
	#This function determines if the current node is capable of handling
	#the join of the new node. If so, it also handles the necessary updates
	#by sending proper requests. Otherwise, it just forwards the request.
	def join(self, ID):
		new_ID = self.hash_sha1(ID)
		#if this is the primal node handling the first join
		if self.successor is None:
			#if new_ID > self.ID:
				#print("Checkpoint 0.1!")
			#else:
				#print("Checkpoint 0.2!")
			self.successor = ID
			self.set_successor(ID, self.nodeID)
			self.send_salute()
		else:
			successor_ID = self.hash_sha1(self.successor)
			if new_ID > self.ID:
				#The case this node was the last node
				#in the ring is included.
				if (self.ID > successor_ID) or \
				(new_ID < successor_ID):
					#Tell new node to set as its successor
					#the current node's previous successor
					#and set new node as the current node's
					#successor
					#print("Checkpoint 1!")
					#if new_ID < successor_ID:
						#print("Checkpoint 1.1!")
					#else:
						#print("Checkpoint 1.2!")
					self.set_successor(ID, self.successor)
					self.successor = ID
					print("Node %s says: New Successor "\
					"= %s." % (self.nodeID, self.successor))
					self.send_salute()
				else:
					#print("Checkpoint 2!")
					self.forward_req("join(" + ID + ")")
			else:
				#This case is resolved either by the last 
				#or the first node of the ring.
				#Forward until the last node is reached
				if (self.ID > successor_ID) and \
				(new_ID < successor_ID):
					#print("Checkpoint 3!")
					self.set_successor(ID, self.successor)
					self.successor = ID
					print("Node %s says: New Successor "\
					"= %s." % (self.nodeID, self.successor))
					self.send_salute()
				else:
					#print("Checkpoint 4!")
					self.forward_req("join(" + ID + ")")
	
	#If the departure is about the current node, depart peacefully.
	#Otherwise forward the depart request.
	def depart(self, ID):
		req_id = self.hash_sha1(ID)
		if self.ID == req_id:
			#Properly introduce new neighbors.
			self.set_successor(self.predecessor, self.successor)
			#An ta valw anapoda gamietai o dias!
			#Trekse tin send_keys me time.sleep(1) na to ksanadeis
			self.send_keys(self.successor)
			return True
		else:
			self.forward_req("depart(" + ID + ")")
			return False

	def insert(self, req_args):
		key = req_args.split(",")[0]
		temp = req_args.split(",")[1]
		value = temp.split(":")[0]
		req_ID = temp.split(":")[1]
		rep_fact = temp.split(":")[2]
		h_key = self.hash_sha1(key)
		pre_ID = self.hash_sha1(self.predecessor)
		if ((h_key > pre_ID) and (h_key <= self.ID))\
		or ((h_key > pre_ID) and (pre_ID > self.ID))\
		or ((h_key <= self.ID) and (pre_ID > self.ID)):
			rep_fact=int(rep_fact)
			rep_fact-=1
			self.database[key] = [value,rep_fact]
			#print("Node %s says: Insert <%s,%s>" % \
			#(self.nodeID, key, self.database[key]))
			if (req_ID != self.nodeID and self.type=='even'):
				s = socket.socket(socket.AF_INET, 
				socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(req_ID)))
				reply = "reply(Insert <" + key + "," +\
				self.database[key][0] +"> OK from Node " + \
				self.nodeID + ".)"	
				s.sendall(reply.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			elif(self.type == 'even'):
				self.recv_reply("Insert <" + key + "," +\
				self.database[key][0] + "> OK.", self.t_stop)
				#print("Insert OK.")
			#rep_fact=int(rep_fact)
			#rep_fact-=1
			if (rep_fact!=0):
				req_args = key + "," + value + ":" + str(self.nodeID) 
				req = "replica(" + req_args + ":" + str(rep_fact) + ")"
				self.forward_req(req)
		else:
			req = "insert(" + req_args + ")"
			self.forward_req(req)

	def replica(self,req_args):
		key = req_args.split(",")[0]
		temp = req_args.split(",")[1]
		value = temp.split(":")[0]
		req_ID = temp.split(":")[1]
		rep_fact = temp.split(":")[2]
		rep_fact = int(rep_fact)
		if (rep_fact != 0):
			rep_fact-=1
			self.database[key]=[value,rep_fact]
			if (self.type == 'linear' and rep_fact==0):
				s = socket.socket(socket.AF_INET,
				socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(req_ID)))
				reply = "reply( Last replica <" + key + "," +\
				self.database[key][0] +"> OK from Node " +\
				self.nodeID + ".)"
				s.sendall(reply.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			if (rep_fact!=-1):
				req_args = key + "," + value + ":" +str(self.nodeID)
				req = "replica(" + req_args + ":" + str(rep_fact) + ")"
				self.forward_req(req)
			#den leitourgei stin akraia periptosi pou chord_length=replication_factor
		elif (rep_fact==0 and (key in self.database)):
			#print('eftasa')
			del self.database[key]
		


	def query_linear(self, req_args):
		key = req_args.split(":")[0]
		req_ID = req_args.split(":")[1]
		if key == "*":
			if  not self.query_done:
				for key_ in self.database:
					print("Node %s: <%s, %s>." % (self.nodeID, key_, self.database[key_][0]))
				req = "query(" + req_args + ")"
				if req_ID == self.nodeID:
					self.query_done = True
				self.forward_req(req)
			#Reset to False for future query(*) to the same Node
			else:
				self.query_done = False
		else:
			num_nodes = req_args.split(":")[2]
			found = False
			value_ = self.database.get(key)
			if value_ != None:
				found = True
				value =value_[0]
	
			num_nodes=int(num_nodes)
			num_nodes -= 1
		
			if (found == True and int(value_[1])==0):
				#check = "Check(" + key + ":" + value + ":" + str(self.nodeID) + \
				#":" + req_ID + ")"
				#self.backward_req(check)
				reply = "reply(Query <" + key + "> : <" + key + "," + value + ">"
				if req_ID != self.nodeID:
					s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + int(req_ID)))
					reply= reply  + " from Node " + self.nodeID + ".)"
					s.sendall(reply.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
				else:
					reply = reply + ".)"
					self.recv_reply(reply)
			elif (found==True and int(value_[1])!=0):
				req = "query(" + key + ":" + req_ID +":" + str(num_nodes) + ")"
				self.forward_req(req)
			elif (found==False and num_nodes!=0):
				req = "query(" + key + ":" + req_ID +":" + str(num_nodes) + ")"
				self.forward_req(req)
				reply = "reply(Forward request to my successor Node " + self.successor
				#print(reply)
			elif (found==False and num_nodes==0):
				reply = "reply(Query <" + key + "> : " + "<No such entry>"
				if req_ID != self.nodeID:
					s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + int(req_ID)))
					reply= reply  + " from Node " + self.nodeID + ".)"
					s.sendall(reply.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
				else:
					reply = reply + ".)"
					self.recv_reply(reply)

	def check(self,req_args):
		key = req_args.split(":")[0]
		value = req_args.split(":")[1]
		req_ID = req_args.split(":")[2]
		basic_ID = req_args.split(":")[3]
		fact=int(self.rep_tot)-1
		if (int(self.database[key][0]) == int(value)):
			if (int(self.database[key][1]) != fact):
				req="Check(" + req_args + ")"
				backward_req(req)
			elif(int(self.database[key][1]) == fact):
				print('psit')
				s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(req_ID)))
				reply= "OK(" + basic_id + ")"
				s.sendall(reply.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
						

	def OK(self,req_args):
		req_ID=req_args
		print('OK')
		reply = "reply(Query <" + key + "> : <" + key + "," + value + ">"
		if req_ID != self.nodeID:
			s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
			s.connect(('127.0.0.1', 3000 + int(req_ID)))
			reply= reply  + " from Node " + self.nodeID + ".)"
			s.sendall(reply.encode())
			s.shutdown(socket.SHUT_RDWR)
			s.close()
		else:
			reply = reply + ".)"
			self.recv_reply(reply)
	
	def query_even(self, req_args):
		key = req_args.split(":")[0]
		req_ID = req_args.split(":")[1]
		if key == "*":
			if  not self.query_done:
				for key_ in self.database:
					print("Node %s: <%s, %s>." % (self.nodeID, key_, self.database[key_][0]))
				req = "query(" + req_args + ")"
				if req_ID == self.nodeID:
					self.query_done = True
				self.forward_req(req)
			#Reset to False for future query(*) to the same Node
			else:
				self.query_done = False
		else:
			num_nodes = req_args.split(":")[2]
			found = False
			value_ = self.database.get(key)
			if value_ != None:
				found = True
				value = value_[0]

			num_nodes=int(num_nodes)
			num_nodes -= 1
		
			if (found == True):
				reply = "reply(Query <" + key + "> : <" + key + "," + value + ">"
				if req_ID != self.nodeID:
					s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + int(req_ID)))
					reply= reply  + " from Node " + self.nodeID + ".)"
					s.sendall(reply.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
				else:
					reply = reply + ".)"
					self.recv_reply(reply)
			elif (found==False and num_nodes!=0):
				req = "query(" + key + ":" + req_ID +":" + str(num_nodes) + ")"
				self.forward_req(req)
				reply = "reply(Forward request to my successor Node " + self.successor
				#print(reply)
			elif (found==False and num_nodes==0):
				reply = "reply(Query <" + key + "> : " + "<No such entry>"
				if req_ID != self.nodeID:
					s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					s.connect(('127.0.0.1', 3000 + int(req_ID)))
					reply= reply  + " from Node " + self.nodeID + ".)"
					s.sendall(reply.encode())
					s.shutdown(socket.SHUT_RDWR)
					s.close()
				else:
					reply = reply + ".)"
					self.recv_reply(reply)

	def delete_all(self, req_args):
		num_nodes = int(req_args.split(':')[0])
		req_ID = req_args.split(':')[1]
		self.database={}
		num_nodes -=1
		if (num_nodes != 0):
			req = "delete_all(" + str(num_nodes) + ":" + req_ID + ")"
			self.forward_req(req)
		elif (num_nodes ==0):
			reply = "reply(Database Deleted"
			if req_ID != self.nodeID:
				s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + int(req_ID)))
				reply= reply  + " from Node " + self.nodeID + ".)"
				s.sendall(reply.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			else:
				reply = reply + ".)"
				self.recv_reply(reply)

	def delete(self, req_args):
		key = req_args.split(":")[0]
		req_ID = req_args.split(":")[1]
		rep_fact = req_args.split(":")[2]
		h_key = self.hash_sha1(key)
		pre_ID = self.hash_sha1(self.predecessor)
		if ((h_key > pre_ID) and (h_key <= self.ID))\
		or ((h_key > pre_ID) and (pre_ID > self.ID))\
		or ((h_key <= self.ID) and (pre_ID > self.ID)):
			del_value = self.database.get(key)
			del_value =del_value[0]
			if del_value != None:
				del self.database[key]
				reply = "reply(Delete <" + key + \
				"> : <" + key + "," + del_value + \
				"> deleted successfuly"
			else:
				reply = "reply(Delete <" + key + "> : "\
				"<No such entry>"
			if req_ID != self.nodeID:
				s = socket.socket(socket.AF_INET, 
				socket.SOCK_STREAM)
				s.connect(('127.0.0.1', 3000 + \
				int(req_ID)))
				reply = reply + " from Node " + \
				self.nodeID + ".)"
				s.sendall(reply.encode())
				s.shutdown(socket.SHUT_RDWR)
				s.close()
			else:
				reply = reply + "."
				self.recv_reply(reply)
			rep_fact=int(rep_fact)
			rep_fact-=1
			if (rep_fact!=0):
				req_args = key + ":" + str(self.nodeID) 
				req = "delete_replica(" + req_args + ":" + str(rep_fact) + ")"
				self.forward_req(req)
		else:
			req = "delete(" + req_args + ")"
			self.forward_req(req)

	def delete_replica(self,req_args):
		key = req_args.split(":")[0]
		req_ID = req_args.split(":")[1]
		rep_fact = req_args.split(":")[2]
		if (rep_fact != 0):
			del_value = self.database.get(key)
			del_value=del_value[0]
			del self.database[key]
			#rep_fact=int(rep_fact)
			#rep_fact-=1
			s = socket.socket(socket.AF_INET,
			socket.SOCK_STREAM)
			s.connect(('127.0.0.1', 3000 + int(req_ID)))
			reply = "reply(Delete Replica <" + key + "," + del_value +\
			"> deleted succesfully from Node " + self.nodeID + ")"
			s.sendall(reply.encode())
			s.shutdown(socket.SHUT_RDWR)
			s.close()
			if (rep_fact!=0):
				rep_fact=int(rep_fact)
				rep_fact-=1
				req_args = key + ":" + str(self.nodeID)
				req = "delete_replica(" + req_args + ":" + str(rep_fact) + ")"
				self.forward_req(req)		
	
	def recv_reply(self, arg, stop=None):
		reply = "Node " + self.nodeID + \
		" received " + arg
		print(reply)
		#print(stop)
		if stop == True:
			stop_time ="Timer stopped at " +\
			str(time.time()) + "\n"
			f = open('times', 'a')
			f.write(stop_time)
			f.close()	
			#stop_time = time.time()
			#f = open('times', 'r')
			#line = f.readline()
			#f.close()
			#start_time = long(line.split(" ")[3])
			#elapsed_time = start_time - stop_time
			#print("Time elapsed: %s" % (str(elapsed_time)))

	#Clear previous values in case the node rejoins.
	def clear(self):
		self.ID = None
		self.successor = None
		self.predecessor = None
		self.database = {}
		self.port = None
		self.socket_.shutdown(socket.SHUT_RDWR)
		self.socket_.close()
		print("Node %s departed." % (self.nodeID))
		self.nodeID = None

	def timer_start(self):
		start_time = 'Timer started at ' + str(time.time()) + '\n'
		f = open ('times', 'w')
		f.write(start_time)
		f.close()

	def timer_stop(self):
		#print('Im in stop')
		self.t_stop = True

	def run(self):
		print("Node %s running..." % (str(self.nodeID)))
		while 1:
			try:
				conn, addr = self.socket_.accept()
				data = conn.recv(1024)
				data = data.decode()
				req_type = data.split('(')[0]
				req_arg = data.split('(')[1]
				req_arg = req_arg[:-1]	
				#print(req_type)		
				if req_type == "join":
					self.join(req_arg)
				elif req_type =="Replication_factor":
					self.rep_tot = req_arg
				elif req_type == "Check":
					self.check(req_arg)
				elif req_type == "OK":
					self.OK(req_arg)
				elif req_type == "Consistency":
					self.type = req_arg
				elif req_type =="replica":
					self.replica(req_arg)
				elif req_type =="delete_replica":
					self.delete_replica(req_arg)
				elif req_type == "depart":
					if self.depart(req_arg):
						break
				elif req_type == "insert":
					self.insert(req_arg)
				elif req_type =="Unlock":
					self.mutex[req_arg].release()
				elif req_type == "query":
					if (self.type == 'linear'):
						self.query_linear(req_arg)
					elif (self.type == 'even'):
						self.query_even(req_arg)
				elif req_type == "delete":
					self.delete(req_arg)
				elif req_type == "delete_all":
					self.delete_all(req_arg)
				elif req_type == "salute":
					self.recv_salute(req_arg, conn)
				elif req_type == "new_successor":
					self.new_successor(req_arg, conn)
				#elif req_type == "send_keys":
				#	self.handle_send_keys(req_arg)
				elif req_type == "get_keys":
					self.handle_get_keys(req_arg)
				elif req_type == "reply":
					self.recv_reply(req_arg, self.t_stop)
					self.t_stop = False

				else:
					print("Something went really wrong.")
					break
			except:
				pass
		#After the loop breaks and self.clear() finishes,
		#the current thread automatically stops. All clear.
		self.clear()
	
	#A proper way to represent a node.
	def __repr__(self):
		return "Node id: %s, Hashed ID: %s, Primal: %s, "\
		"Predecessor: %s, Successor: %s" % (self.nodeID, \
		self.ID, str(self.primal_node), str(self.predecessor), \
		str(self.successor))
