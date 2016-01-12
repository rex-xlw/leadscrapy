#!/usr/bin/env python
# 325 row data = 355.8 s

import database_manager

def getCarrier(phone):
	return "AT&T"

if __name__ == '__main__':
	phoneInfoList = database_manager.getAllNoCarrierPhoneInfo()
	resultInfoList = []
	for phoneInfo in phoneInfoList:
		phoneInfo['carrier'] = getCarrier(phoneInfo['phone'])
		resultInfoList.append(phoneInfo)

	database_manager.updateAllCarrier(resultInfoList)