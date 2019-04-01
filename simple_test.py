#!bin/python3
from collections import defaultdict
from threading import Lock
mutex={}
key='key'
#print(key)
#key=int(key)
#print(key)
value ='new_value'
ID = '1'
req=  key + ":" + value + ':' + ID + ")"
key1 =req.split(':')[0]
value1 = req.split(':')[1]
#print(value1)
try:
	req_ID=req.split(':')[2]
	req_ID=req_ID[:-1]
except:
	value1 = value1[:-1]
#print(key1)
#print(value1)
#print(req_ID)
data_dict={}
data_dict['key']="value"
#print(data_dict)
data_dict.update({key:value})
#print(data_dict)
data_dict["key"]= ["kainourgio",'1']
data_dict["key"]= ["value"]
#print(data_dict["key2"][0])
#print(data_dict['key2'][1])
print(data_dict)
mutex[key] = Lock()
mutex[key].acquire(1)
mutex[key].release()
