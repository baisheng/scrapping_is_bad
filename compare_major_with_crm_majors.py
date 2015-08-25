import pandas as pd
from random import randint
import requests
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

reload(sys)
sys.setdefaultencoding("utf-8")

class compare_major_with_crm_majors():

	# private function

	def __connect(self):

		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:

			conn = psycopg2.connect(conn_string)

		except:

			print 'I am not able to connect to the database'

		return conn 

	def get_majors(self):

		conn = self.__connect()

		cr = conn.cursor()

		query_beijing_school = "SELECT major_name FROM crm_gz_major"

		count = 0

		commun_majors = []

		try:

			cr.execute(query_beijing_school)

		except Exception, e:

			print '############# We could not find Guangzhou CRM majors, here are the reasons: ', e

		crm_gz_majors = cr.fetchall()

		if crm_gz_majors is not None:

			for idx, major in enumerate(crm_gz_majors):

				major = str(major[0]).strip().encode('utf-8')

				query_majors = "SELECT DISTINCT(english_name) FROM major WHERE english_name LIKE %s"

				try:

					cr.execute(query_majors, ( str(major) + '%' ,))

				except Exception, e:

					print '############# We could not find majors, here are the reasons: ', e

				result = cr.fetchone()

				if result is not None:

					count += 1

					if major not in commun_majors:

						commun_majors.append(major)

					print major

		else:

			print 'There are no majors for Guangzhou'

		print count

		print len(commun_majors)



Compare = compare_major_with_crm_majors()

Compare.get_majors()

