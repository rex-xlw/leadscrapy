# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
class DetailedItem(scrapy.Item):
	url = scrapy.Field()
	name = scrapy.Field()
	phone = scrapy.Field()
	address = scrapy.Field()
	date = scrapy.Field()

class LeadItem(scrapy.Item):
	phone = scrapy.Field()
	address = scrapy.Field()
	date = scrapy.Field()
	url = scrapy.Field()
	
class BasicItem(scrapy.Item):
	name = scrapy.Field()
	phone = scrapy.Field()

class TestItem(scrapy.Item):
	url = scrapy.Field()