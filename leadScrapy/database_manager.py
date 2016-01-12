#from leadScrapy_cache import database_execution
import database_execution
import MySQLdb
import MySQLdb.cursors
import time

#create crawled Info Table
def createCrawledInfoTable ():
	res = database_execution.queryOne("""
		SELECT count(*)
		FROM information_schema.TABLES
		WHERE table_name = 'crawled_info'
		AND TABLE_SCHEMA = 'movingen_lead_info'
		""")
#	print 'res=', res
	if not res['count(*)']:
		database_execution.execute ("""
			CREATE TABLE crawled_info (
				id INT NOT NULL auto_increment,
				phone VARCHAR(200) NOT NULL,
				address VARCHAR(1000) NOT NULL,
				date DATE NOT NULL,
				PRIMARY KEY (`id`)
			);
		""")
		print "a new table crawled_info is created!"
		return True
	else:
		print "table crawled_info exists"
		return False

#create User Info Table
def createUserInfoTable ():
	res = database_execution.queryOne("""
		SELECT count(*)
		FROM information_schema.TABLES
		WHERE table_name = 'user_info'
		AND TABLE_SCHEMA = 'movingen_lead_info'
		""")
#	print 'res=', res
	if not res['count(*)']:
		database_execution.execute ("""
			CREATE TABLE user_info (
				uid INT NOT NULL auto_increment,
				address VARCHAR(1000) NOT NULL,
				flag BOOL NOT NULL,
				PRIMARY KEY (`uid`)
			);
		""")
		print "a new table user_info is created!"
		return True
	else:
		print "table user_info exists"
		return False

#create Phone Info Table
def createPhoneInfoTable ():
	res = database_execution.queryOne("""
		SELECT count(*)
		FROM information_schema.TABLES
		WHERE table_name = 'phone_info'
		AND TABLE_SCHEMA = 'movingen_lead_info'
		""")
#	print 'res=', res
	if not res['count(*)']:
		database_execution.execute ("""
			CREATE TABLE phone_info (
				id INT NOT NULL auto_increment,
				uid INT NOT NULL,
				phone CHAR(10) NOT NULL,
				carrier VARCHAR(20) NULL,
				PRIMARY KEY (`id`, `phone`),
				FOREIGN KEY(`uid`) REFERENCES user_info(`uid`)
			);
		""")
		print "a new table phone_info is created!"
		return True
	else:
		print "table phone_info exists"
		return False

#create Messenger Table
def createMessengerTable ():
	res = database_execution.queryOne("""
		SELECT count(*)
		FROM information_schema.TABLES
		WHERE table_name = 'messenger'
		AND TABLE_SCHEMA = 'movingen_lead_info'
		""")
#	print 'res:',res
	if not res['count(*)']:
		database_execution.execute ("""
			CREATE TABLE messenger (
				uid INT NOT NULL,
				date DATE NULL,
				PRIMARY KEY  (`uid`),
				FOREIGN KEY(`uid`) REFERENCES user_info(`uid`)
			)
		""")
		print "a new table messenger is created!"
		return True
	else:
		print "table messenger exists"
		return False

#create Register Info Table		
def createRegisterInfoTable ():
	res = database_execution.queryOne("""
		SELECT count(*)
		FROM information_schema.TABLES
		WHERE table_name = 'register_info'
		AND TABLE_SCHEMA = 'movingen_lead_info'
		""")
#	print 'res:',res
	if not res['count(*)']:
		database_execution.execute ("""
			CREATE TABLE register_info (
				id INT NOT NULL auto_increment,
				phone CHAR(10) NOT NULL,
				PRIMARY KEY  (`id`)
			)
		""")
		print "a new table register_info is created!"
		return True
	else:
		print "table register_info exists"
		return False

def getDate ():
	return time.strftime('%Y%m%d',time.localtime(time.time()))

def formatDate (date):
	return date[0:4]+'-'+date[4:6]+'-'+date[6:8]

'''
connector
fuction: create all tables in database
input: None
output: None
'''
def initDatabase ():
	createCrawledInfoTable ()
	createUserInfoTable ()
	createPhoneInfoTable ()
	createMessengerTable ()
	createRegisterInfoTable ()

'''
connector
fuction: drop all tables in database
input: None
output: None
'''
def dropAllTable ():
	database_execution.execute("DROP TABLE IF EXISTS crawled_info")
	database_execution.execute("DROP TABLE IF EXISTS user_info")
	database_execution.execute("DROP TABLE IF EXISTS phone_info")
	database_execution.execute("DROP TABLE IF EXISTS messenger")
	database_execution.execute("DROP TABLE IF EXISTS register_info")

'''
connector
fuction: upload a row single crawled item into database
input: the crawled item
output: a boolean indicating whether succeed
'''
def uploadCrawledInfo (item):
	
	if not 'date' in item or not 'phone' in item or not 'address' in item:
		return False
	phone = ""
	address = "" 
	for element in item['phone']:
		phone += str(element) + ";"
	phone = phone[:-1]
	for element in item['address']:
		address += str(element) + ";"
	address = address[:-1]
	date = formatDate(str(item['date'][0]))
	res = database_execution.execute("SELECT * FROM crawled_info WHERE phone = %s", [phone])
	if res:
		return False
	else:
		if not database_execution.execute("INSERT INTO crawled_info VALUES ( NULL, %s, %s, %s )", [phone, address, date]):
			return False
	return True

'''
connector
fuction: upload a row single crawled item into database, and add it to user and phone info list
input: the crawled item
output: a boolean indicating whether succeed
'''

def CrawlAndUploadUserAndPhoneInfo (item):

	if not 'date' in item or not 'phone' in item or not 'address' in item:
		return False
	phone = ""
	address = "" 
	for element in item['phone']:
		phone += str(element) + ";"
	phone = phone[:-1]
	for element in item['address']:
		address += str(element) + ";"
	address = address[:-1]
	date = formatDate(str(item['date'][0]))
	res = database_execution.execute("SELECT * FROM crawled_info WHERE phone = %s", [phone])
	if res:
		return False
	else:
		if not database_execution.execute("INSERT INTO crawled_info VALUES ( NULL, %s, %s, %s )", [phone, address, date]):
			#print "hello"
			#pause = raw_input("PAUSE")
			return False
	item['phone'] = phone
	item['address'] = address
	return uploadUserAndPhoneInfo(item)

'''
connector
fuction: upload row info of a fixed day to user and phone info list
input: a date indicating the crawling date, if left blank, the date would be the system date
output: a boolean indicating whether succeed
'''
def uploadAllUserAndPhoneInfo(date = None):
	if date == None:
		date = getDate()
	date = formatDate(str(date))
	res = database_execution.queryAll("SELECT phone, address FROM crawled_info WHERE date = %s", [date])
	if not res:
		return False
	for item in res:
		if not uploadUserAndPhoneInfo(item):
			return False
	return True
		
def uploadUserAndPhoneInfo(item):
	if not 'phone' in item or not 'address' in item:
		return False
	#pause = raw_input("q2312")
	phone_list = item['phone'].split(";")
	uid = None
	for phone in phone_list:
		res2 = database_execution.queryOne("SELECT uid FROM phone_info WHERE phone = %s", [phone])
		if res2:
			uid = int(res2['uid'])
			break
	if not uid: #uid does not exists
		flag = False
		for phone in phone_list:
			res4 = database_execution.execute("SELECT * FROM register_info WHERE phone = %s", [phone])
			if res4:
				flag = True
				break
		# if the user has registered, we do not need to add his or her phone number into phone_info
		res5 = database_execution.executeAndGetId("INSERT INTO user_info VALUES ( NULL, %s, %s )", [str(item['address']), flag])
		uid = int(res5) #new uid
		if flag:
			return True
	for phone in phone_list:
		if not database_execution.execute("INSERT INTO phone_info VALUES (NULL, %s, %s, NULL)", [uid, phone]):
			#pause = raw_input("adsawd")
			return False
	return True

#test
if __name__ == "__main__":
	dropAllTable()
	initDatabase()
'''
	item1 = {'phone': [u'2024450008'], 'address': [u'2800 Washington St., Washington, DC'], 'date': [u'20160103']}
	item2 = {'phone': [u'2024450018', u'2026645342'], 'address': [u'2800 Washington St., Washington, DC'], 'date': [u'20160103']}
	item3 = {'phone': [u'2024450088', u'2026645315', u'2025800099'], 'address': [u'2800 Washington St., Washington, DC'], 'date': [u'20160103']}
	item4 = {'phone': [u'2024450018', u'2026645352'], 'address': [u'2800 Washington St., Washington, DC'], 'date': [u'20160103']}
	item5 = {'phone': [u'1234567890', u'9998887771'], 'address': [u'2800 Washington St., Washington, DC'], 'date': [u'20160103']}
	register_item = {'phone': u'1234567890'}
	print uploadRegisterInfo(register_item)
	print CrawlAndUploadUserAndPhoneInfo (item1)
	print CrawlAndUploadUserAndPhoneInfo (item2)
	print CrawlAndUploadUserAndPhoneInfo (item3)
	print CrawlAndUploadUserAndPhoneInfo (item4)
	print CrawlAndUploadUserAndPhoneInfo (item5)
#	register_item = {'phone': u'1234567890'}
#	print uploadRegisterInfo(register_item)
#	print uploadAllUserAndPhoneInfo('20160103')
#	print getAllNoCarrierPhoneInfo()
#	phone_info_list = (
#		{'phone': '2024450008', 'carrier': 'AT&T', 'uid': 1},
#		{'phone': '2024450088', 'carrier': 'T-Mobile', 'uid': 2},
#		{'phone': '2026645315', 'carrier': 'AT&T', 'uid': 2},
#		{'phone': '2025800099', 'carrier': 'Verizon', 'uid': 2},
#		{'phone': '2024450018', 'carrier': None, 'uid': 3},
#		{'phone': '2026645342', 'carrier': 'AT&T', 'uid': 3},
#		{'phone': '2026645352', 'carrier': 'AT&T', 'uid': 3}
#	)
#	print updateAllCarrier(phone_info_list)
#	message_list = getPhonesToBeMessagedAndRefreshMessenger()
#	for element in message_list:
#		print element
'''