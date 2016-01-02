
import os
"""
#redis Verison
import redis

RS = redis.Redis(host='localhost', port=6379, db=1)

def isUrlExist(url):
	if RS.exists(url):
		return True
	else:
		return False

def insertIntoDB(url):
	RS.set(url, 1)
"""

#MongoDB Version
from pymongo import MongoClient
HOST = os.environ['OPENSHIFT_MONGODB_DB_HOST']
PORT = os.environ['OPENSHIFT_MONGODB_DB_PORT']
connection = HOST + ":" + PORT
conn = MongoClient(connection)
#conn = MongoClient()
leadScrapyDB = conn.leadScrapy
urlCol = leadScrapyDB.urlCol

def isUrlExist(url):
	isExist = False
	ele = {"url":url}
	for flag in urlCol.find(ele):
		isExist = True
	if isExist ==  True:
		print url
		pause = raw_input("PAUSE")
	return isExist

def insertIntoDB(url):
	ele = {"url":url}
	urlCol.insert(ele)