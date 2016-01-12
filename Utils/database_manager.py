#!/usr/bin/env python
import sys
sys.path.append("..")
import leadScrapy.database_execution
import MySQLdb
import MySQLdb.cursors
import time

'''
connector
fuction: Get all phone numbers without carrier information, with corresponding uid and carrier (all None)
input: None
output: A List of dictionary, each has uid, phone and carrier(None)
'''
def getAllNoCarrierPhoneInfo():
	phone_info_list = leadScrapy_cache.database_execution.queryAll("SELECT uid, phone, carrier FROM phone_info WHERE carrier is NULL ORDER BY uid")
	for item in phone_info_list:
		item["uid"] = int(item["uid"])
		item['phone'] = str(item['phone'])
	return phone_info_list

'''
connector
fuction: Get all phone numbers, with corresponding uid and carrier
input: None
output: A List of dictionary, each has uid, phone and carrier
'''
def getAllPhoneInfo():
	phone_info_list = leadScrapy_cache.database_execution.queryAll("SELECT uid, phone, carrier FROM phone_info ORDER BY uid")
	for item in phone_info_list:
		item['uid'] = int(item['uid'])
		item['phone'] = str(item['phone'])
	return phone_info_list

'''
connector
fuction: update the carrier info into database, and update messaging target(so that they could be messaged!)
input: A List of dictionary, each has uid, phone and carrier(must be filled)
output: a integer indicating the amount of new messaging target
'''
def updateAllCarrier(phone_info_list):
	total = 0
	for phone_info in phone_info_list:
		if updateCarrier(phone_info):
			total += 1
	print str(total) + ' messaging target updated'
	return total

def updateCarrier(phone_info):
	if not 'uid' in phone_info or not 'carrier' in phone_info or not 'phone' in phone_info:
		return False
	if phone_info['carrier'] == None:
		return False
	if not leadScrapy_cache.database_execution.execute("UPDATE phone_info SET carrier = %s WHERE phone = %s", [str(phone_info['carrier']), str(phone_info['phone'])]):
		return False
	return updateMessenger(int(phone_info['uid']))
#	return True

def updateMessenger(uid):
	res = leadScrapy_cache.database_execution.execute("SELECT * FROM messenger WHERE uid = %s", [uid])
	if res:
#		print 'uid ' + str(uid) + ' has already exists in messenger!'
		return False
	if not leadScrapy_cache.database_execution.execute("INSERT INTO messenger VALUES (%s, NULL)", [uid]):
		return False
	return True


'''
connector
fuction: get all phones need to be messaged and corresponding uid, carrier and address, and refresh the last messaging date in database
input: None
output: A List of dictionary, each has uid, phone and carrier, and address, those are message target of this time 
'''
def getPhonesToBeMessagedAndRefreshMessenger():
	message_info_list = leadScrapy_cache.database_execution.queryAll("""
		SELECT messenger.uid, phone, carrier, address
		FROM messenger, phone_info, user_info 
		WHERE messenger.uid = phone_info.uid
		and messenger.uid = user_info.uid
		and carrier IS NOT NULL
		"""
	)
	uid_list = []
	for item in message_info_list:
		item['uid'] = int(item['uid'])
		item['phone'] = str(item['phone'])
		item['carrier'] = str(item['carrier'])
		item['address'] = str(item['address'])
		if not item['uid'] in uid_list:
			uid_list.append(item['uid'])
	messageAllSent(uid_list)
	return message_info_list

#after message sent, refresh the date in messenger 
def messageAllSent(uid_list):
	total = 0
	for uid in uid_list:
		if messageSent(uid):
			total += 1
	print str(total) + ' users are messaged!'

def messageSent(uid):
	date = formatDate(getDate())
	if not leadScrapy_cache.database_execution.execute("UPDATE messenger SET date = %s WHERE uid = %s", [str(date), str(uid)]):
		return False
	return True

'''
connector
fuction: upload the register info into register_info, for test at present(other information needed are not clear)
input: a dictionary of the register info
output: a boolean indicating whether succeed
'''
def uploadRegisterInfo(item):
	if not 'phone' in item:
		return False
	res = database_execution.execute("SELECT * FROM register_info WHERE phone = %s", [str(item['phone'])])
	if res:
		return False
	if not database_execution.execute("INSERT INTO register_info VALUES (NULL, %s)", [str(item['phone'])]):
		return False
	res2 = database_execution.queryOne("SELECT uid from phone_info WHERE phone = %s", [str(item['phone'])])
	if not res2:
	# indicating that this is a new user without being messaged
		return True
	if not database_execution.execute("DELETE FROM messenger WHERE uid = %s", [int(res2['uid'])]):
		return False
	if not database_execution.execute("UPDATE user_info SET flag = true WHERE uid = %s", [int(res2['uid'])]):
		return False
	return True


def formatDate (date):
	return date[0:4]+'-'+date[4:6]+'-'+date[6:8]

def getDate ():
	return time.strftime('%Y%m%d',time.localtime(time.time()))