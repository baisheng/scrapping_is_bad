 #coding=utf-8 

import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

class merge_mymajor_icu():

	global pg_table

	global root_url

	root_url = 'http://www.4icu.org'

	pg_table = 'schools'

	# private function

	def __connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	def replace_school_id_in_mymajor()