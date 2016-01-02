import scrapy
import urllib2
import os
import re
import time

from scrapy.spiders import CrawlSpider,Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.http import Request
from leadScrapy.items import LeadItem

MAXIMUMPAGE = 300

class LeadSpider(CrawlSpider):
	name = "lead_spider"
	allow_domains = [
		"www.owners.com",
		"www.forsalebyowner.com",
		"www.isoldmyhouse.com",
		"www.homesbyowner.com",
		]
	start_urls = [
		"http://www.homesbyowner.com",
		"http://www.owners.com/search",
		"http://www.isoldmyhouse.com/browse/",
		"http://www.forsalebyowner.com/homes-for-sale",
	]
	
	pattern1 = re.compile(r'\(\d{3}\)\s\d{3}-\d{4}')
	pattern2 = re.compile(r'\d{3}-\d{3}-\d{4}')
	pattern3 = re.compile(r'\d{10}')
	pattern4 = re.compile(r'\d{3}\s\d{3}\s\d{4}')
	pattern5 = re.compile(r'\d{3}\.\d{3}\.\d{4}')

	rules = (
		Rule(
			LinkExtractor(
				allow = (
					'\w+/$',
					),
				deny = (
					'canada/$',
					),
				allow_domains = ("www.homesbyowner.com"),
				restrict_xpaths = ("//div[@id='state-list']"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'\w+/home.asp$',
					),
				allow_domains = ("www.homesbyowner.com"),
				restrict_xpaths = ("//div[@id='city-list']"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'Search_Listings.asp$',
					),
				allow_domains = ("www.homesbyowner.com"),
				restrict_xpaths = ("//div[@id='mainnav']/ul/li[2]"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'home_view.asp\?whichpage=\d&pagesize=\d&$',
					),
				allow_domains = ("www.homesbyowner.com"),
				restrict_xpaths = ("//dl[@class='listings']"),
				),

			callback = 'parse_homesbyowner_lead',
			follow = False,
			),

		Rule(
			LinkExtractor(
				allow = (
					'homes-for-sale-in-\w{2}$',
					),
				allow_domains = ("www.isoldmyhouse.com"),
				restrict_xpaths = ("//map[@name='Map']"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'browse/[\S\s]*$',
					),
				allow_domains = ("www.isoldmyhouse.com"),
				restrict_xpaths = ("//a[@class='page right']"),
				),

			),
		
		Rule(
			LinkExtractor(
				allow = (
					'[\w-]+/[\w-]+/\w{2}/\d{6}$',
					),
				allow_domains = ("www.isoldmyhouse.com"),
				restrict_xpaths = ("//div[@class='caption']"),
				),
			callback = 'parse_isoldmyhouse_lead',
			follow = False,
			),

		Rule(
			LinkExtractor(
				allow = (
					'\w{2}$',
					),
				allow_domains = ("www.owners.com"),
				restrict_xpaths = ("//div[@class='statelinks']"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'for-sale-by-owner/\w{2}/[\S\s]+$',
					),
				allow_domains = ("www.owners.com"),
				restrict_xpaths = ("//div[@class='cities']"),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'for-sale-by-owner/\w{2}/[\S\s]+/\w{7}/\d+-[\S\s]+$',
					),
				allow_domains = ("www.owners.com"),
				restrict_xpaths = ('//h2[@class="addressInfo"]'),
				),
			),

		Rule(
			LinkExtractor(
				allow = (
					'for-sale-by-owner/\w{2}/[\S\s]+/\w{7}/\d+-[\S\s]+/print$',
					),
				allow_domains = ("www.owners.com"),
				restrict_xpaths = ('//li[@class="printListing"]'),
				),
			callback = 'parse_owners_lead',
			follow = False,
			),

		Rule(
			LinkExtractor(
				allow = (
					'homes-for-sale/\w+$'
					),
				allow_domains = ("www.forsalebyowner.com"),
				restrict_xpaths = ("//div[@class='stateList']"), 
				), 
			callback = 'parse_salebyowner_first_page' 
			),
		)
	
	def parse_homesbyowner_lead(self, response):
		item = LeadItem()
		nowTime = time.localtime()
		phone = response.xpath('//div[@class="options"]/p/strong/text()').extract()
		addressList = response.xpath('//p[@class="address"]/text()').extract()
		date = str(nowTime[0]) + str(nowTime[1]) + str(nowTime[2])

		resultPhone = []
		resultAddress = []
		resultDate = []

		for phoneItem in phone:
			phoneItem = phoneItem.replace('(','').replace(')','').replace('-','').replace(' ','')
			phoneItem = phoneItem[:10]
			resultPhone.append(phoneItem)

		resultAddress.append(addressList[0]+', '+addressList[1])
		resultDate = date

		item['url'] = response.url
		item['phone'] = resultPhone
		item['address'] = resultAddress
		item['date'] = resultDate
		return item

	def parse_owners_lead(self, response):
		nowTime = time.localtime()
		item = LeadItem()

		phoneList = response.xpath('//div[@class="info"]/div/text()').extract()
		addressLine1 = response.xpath('//div[@class="f-left"]/h1/text()').extract()
		addressLine2 = response.xpath('//div[@class="f-left"]/h2/text()').extract()
		date = str(nowTime[0]) + str(nowTime[1]) + str(nowTime[2])

		resultPhone = []
		resultAddress = []
		resultDate = []


		for phoneItem in phoneList:
			phoneItem = phoneItem.replace('(','').replace(')','').replace('-','').replace(' ','')
			phoneItem = phoneItem[:10]
			resultPhone.append(phoneItem)
		
		address = addressLine1[0]+ ", " + addressLine2[0]
		resultAddress.append(address)

		resultDate.append(date)

		item['url'] = response.url
		item['phone'] = resultPhone
		item['address'] = resultAddress
		item['date'] = resultDate
		return item

	def parse_isoldmyhouse_lead(self, response):
		item = LeadItem()
		nowTime = time.localtime()
		phone = []
		phoneContent = ""
		phoneContentList = response.xpath('//div[@class="content"]/p/text()').extract()
		address = response.xpath('//div[@class="details-address"]/text()').extract()
		date = str(nowTime[0]) + str(nowTime[1]) + str(nowTime[2])

		resultPhone = []
		resultAddress = []
		resultDate = []

		for phoneContentItem in phoneContentList:
			phoneContent = phoneContent + " " + phoneContentItem
		
		phonePattern1 = self.pattern1.findall(phoneContent)
		phonePattern2 = self.pattern2.findall(phoneContent)
		phonePattern3 = self.pattern3.findall(phoneContent)
		phonePattern4 = self.pattern4.findall(phoneContent)
		phonePattern5 = self.pattern5.findall(phoneContent)

		phoneList = phonePattern1 + phonePattern2 + phonePattern3 + phonePattern4 + phonePattern5

		for phoneItem in phoneList:
			phoneItem = phoneItem.replace('(','').replace(')','').replace('-','').replace(' ','')
			if phoneItem not in resultPhone:
				resultPhone.append(phoneItem)

		for addressItem in address:
			resultAddress.append(addressItem)
		
		resultDate = date

		item['url'] = response.url
		item['phone'] = resultPhone
		item['address'] = resultAddress
		item['date'] = resultDate
		return item

	def parse_salebyowner_first_page(self, response):
		page = 1
		for pageMax in response.xpath('//div/div/ol[@class="pager-pages"]/li[last()]/a/text()').extract():
			pageMax = int(pageMax)
			if pageMax > page:
				if pageMax > MAXIMUMPAGE:
					pageMax = MAXIMUMPAGE
				page = pageMax

		for x in range(page):
			pageUrl = response.url + '/' + str(x+1) + '-page'
			#print url
			yield scrapy.Request(url = pageUrl, callback = self.parse_salebyowner_pages)
	
	def parse_salebyowner_pages(self, response):
		#print response.url
		urlResource = response.xpath('//a[@class="estate-bd"]')
		for sel in urlResource:
			pageUrlList = sel.xpath('@href').extract()
			pageUrl = pageUrlList[0]
			isOtherSourceXpath = "//a[@href=\"" + pageUrl + "\"]//div[@class=\"estateSummary-sm\"]"
			
			isOtherSource = response.xpath(isOtherSourceXpath).extract()
			if isOtherSource == []:
				regex = 'http://www.forsalebyowner.com/listing/[\w+|-]+/\w+$'
				if re.match(regex, pageUrl):
					yield scrapy.Request(url = pageUrl, callback = self.parse_salebyowner_lead_items)

	def parse_salebyowner_lead_items(self, response):
		item = LeadItem()
		nowTime = time.localtime()
		flag = 0
		addressInfoList = []
		addressInfoList = response.xpath('//div[@class="grid_XL grid_XL_gutter"]//h2/span[@itemprop="address"]/span/text()').extract()
		contactInfoList = response.xpath('//div[@class="grid_MD-col grid_MD-col_aside"]//ol/li/ul/li/text()').extract()
		date = str(nowTime[0]) + str(nowTime[1]) + str(nowTime[2])

		phoneList = []
		addressList = []

		for contactInfoItem in contactInfoList:
			if flag == 0:
				flag = 1
			else:
				contactInfoItem = contactInfoItem.replace("+","").replace("(","").replace(")","").replace("-", "").replace(" ","").replace(".","")
				if contactInfoItem.isdigit():
					phoneList.append(contactInfoItem)
		
		if addressInfoList != []:
			address = addressInfoList[0][4:] + ", " + addressInfoList[1] + ", " + addressInfoList[2] + " " + addressInfoList[3]
			addressList.append(address)

		resultAddress = []
		resultPhone = []
		resultDate = []

		resultAddress = addressList
		resultPhone = phoneList
		resultDate.append(date)

		item['url'] = response.url
		item['date'] = date
		item['address'] = resultAddress
		item['phone'] = resultPhone
		return item