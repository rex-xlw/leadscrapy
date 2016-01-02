import redis

RS = redis.Redis(host='localhost', port=6379, db=1)

def isUrlExist(url):
	if RS.exists(url):
		return True
	else:
		return False

def insertIntoDB(url):
	RS.set(url, 1)


