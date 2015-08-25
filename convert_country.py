 #coding=utf-8 

import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

class convert_country():

	# private function

	def __connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	def replace_country_in_school_table(self):

		conn = self.__connect()

		cr = conn.cursor()

		query_schools = "SELECT school_id, country FROM schools ORDER BY school_id ASC"

		try:

			cr.execute(query_schools)

		except Exception, e:

			print "We couldn't execute the query on schools, here are the reasons: ", e

		schools = cr.fetchall()

		for school in schools:

			country = school[1].upper()

			print country

			query_country = "SELECT country_id FROM countries WHERE iso2 = %s"

			try:

				cr.execute(query_country, (str(country),))

			except Exception, e:

				print 'The country selection failed, here are the reasons: ', e

			country_id = cr.fetchone()

			if country_id is not None:

				print country_id[0]

				query_update_schools = "UPDATE schools SET (country) = (%s) WHERE school_id = %s"

				try:

					cr.execute(query_update_schools, (int(country_id[0]), int(school[0]),))

					print 'You successfully updated the country'

				except Exception, e:

					print 'The update of the school failed, here are the reason: ', e

		conn.commit()

		conn.close()

	def another_stuff(self):

		conn = self.__connect()

		cr = conn.cursor()

		query_schools = "SELECT crm.introduction, sc.school_id FROM schools AS sc INNER JOIN crm_beijing_school AS crm ON sc.english_name = crm.english_name WHERE crm.english_name IS NOT NULL; "

		try:

			cr.execute(query_schools)

		except Exception, e:

			print "We couldn't execute the query on schools, here are the reasons: ", e

		schools = cr.fetchall()

		for school in schools:

			print school[0]

			origin = 'Beijing CRM'

			query_update_schools = "UPDATE schools SET (school_profile_chinese, school_profile_origin) = (%s, %s) WHERE school_id = %s"

			try:

				cr.execute(query_update_schools, (str(school[0]), origin , int(school[1]),))

				print 'You successfully updated the country'

			except Exception, e:

				print 'The update of the school failed, here are the reason: ', e

		conn.commit()

		conn.close()


New_object = convert_country()

New_object.another_stuff()