#!/usr/bin/env python
# -*- coding: utf-8 -*-

from scrapy.exceptions import IgnoreRequest
from leadScrapy.database import isUrlExist

class IngoreHttpRequestMiddleware(object):
	""" use download middleware to filter url """
	def process_response(self, request, response, spider):
		
		if isUrlExist(response.url):
			raise IgnoreRequest("IgnoreRequest : %s" % response.url)
		else:
			return response