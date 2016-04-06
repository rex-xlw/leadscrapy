import MySQLdb
from DBUtils.PooledDB import PooledDB

MYSQL_HOST = '***'
MYSQL_DBNAME = '***'
MYSQL_USER = '***'
MYSQL_PASSWD = '***'

class DbManager(object):
	
	_pool = None

	def __init__(self):
		self._conn = DbManager.__getConn()
		self._cursor = self._conn.cursor()

	@staticmethod
	def __getConn():
		if DbManager._pool is None:
			connKwargs = {'host':MYSQL_HOST, 'user':MYSQL_USER, 'passwd':MYSQL_PASSWD, 'db':MYSQL_DBNAME, 'charset':"utf8"}
			_pool = PooledDB(MySQLdb, mincached=0, maxcached=0, maxshared=0, maxusage=0, **connKwargs)
		return _pool.connection()

	def execute(self, sql, param=None):
		cursor = self._cursor
		if param == None:
			rowCount = cursor.execute(sql)
		else:
			rowCount = cursor.execute(sql, param)
		return rowCount
	
	def executeAndGetId(self, sql, param=None):
		conn = self._conn
		cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
		if param == None:
			cursor.execute(sql)
		else:
			cursor.execute(sql, param)
		id = cursor.lastrowid
		cursor.close()
		conn.close()
		return id
	
	def queryOne(self, sql, param=None):
		conn = self._conn
		cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
		if param == None:
			rowCount = cursor.execute(sql)
		else:
			rowCount = cursor.execute(sql, param)  
		if rowCount > 0:
			res = cursor.fetchone()
		else:
			res = None
		return res

	def queryAll(self, sql, param=None):
		conn = self._conn
		cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
		if param == None:
			rowCount = cursor.execute(sql)
		else:
			rowCount = cursor.execute(sql, param)
		if rowCount > 0:
			res = cursor.fetchall()
		else:
			res = None
		return res

	def dispose(self, isEnd = 1):
		self._cursor.close()
		self._conn.close()
	
def execute(sql, param=None):
	DB = DbManager()
	rowCount = DB.execute(sql, param)
	return rowCount

def executeAndGetId(sql, param=None):
	DB = DbManager()
	id = DB.executeAndGetId(sql, param)
	return id
	
def queryOne(sql, param=None):
	DB = DbManager()
	res = DB.queryOne(sql, param)
	return res
  
def queryAll(sql, param=None):
	DB = DbManager()
	res = DB.queryAll(sql, param)
	return res

def dispose():
	DB = DbManager()
	DB.dispose()

#test
#if __name__ == "__main__":
	"""
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res

	dispose()
	dispose()
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	item = {'phone': '2022844829', 'address': '1400 N Loyal st., Fairfax, VA; 2800 Connecticut Ave., Washington, DC', 'date':'20160101'}
	res = execute("select * from crawled_info where phone = " + item['phone'])
	print res
	"""
#	print queryOne("insert into register_info values (null, '1234567891')")
#	flag = False
#	executeAndGetId("INSERT INTO user_info VALUES ( NULL, %s, %s )", ["test address", flag])
#	uid = 3
#	execute("INSERT INTO messenger VALUES (%s, NULL)", [uid])
