#!bin/python3

import threading
import multiprocessing

class Daemon(threading.Thread):
	def __init__(self, obj, method):
		threading.Thread.__init__(self)
		self.obj_ = obj
		self.method_ = method

	def run(self):
		getattr(self.obj_, self.method_)()

class Worker(multiprocessing.Process):
	def __init__(self, obj, method):
		multiprocessing.Process.__init__(self)
		self.obj_ = obj
		self.method_ = method

	def run(self):
		getattr(self.obj_, self.method_)()
