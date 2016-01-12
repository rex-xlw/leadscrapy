#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from leadScrapy.database import insertIntoDB
from leadScrapy.database_manager import uploadCrawledInfo
from leadScrapy.database_manager import CrawlAndUploadUserAndPhoneInfo

class FilterInvalidPipeline(object):
	def process_item(self, item, spider):
		if item['phone'] == []:
			raise DropItem("Drop useless item")
			#return item
		else:
			if item['phone'][0]=='1' and len(item['phone'])==11:
				item['phone'] = item['phone'][1:]
			return item
		#return item

class InsertIntoDatabase(object):
	def process_item(self, item, spider):	
		print "Data Inserting..."
		CrawlAndUploadUserAndPhoneInfo(item)
		'''
		print result
		if result == False:
			print item
			pause = raw_input("PAUSE")
		'''
		print "Data Inserted finished..."		
		return item

class FilterUrlPipline(object):
	""" use redis to fileter url """
	def process_item(self, item, spider):
		insertIntoDB(item['url'])
		return item