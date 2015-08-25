 #coding=utf-8 
import pandas as pd
import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

class link_crm_school_schools():

	# private function

	def __connect(self):

		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:

			conn = psycopg2.connect(conn_string)

		except:

			print 'I am not able to connect to the database'

		return conn 

	def fetch_crm_gz_schools(self):

		conn = self.__connect()

		cr = conn.cursor()

		query_school_gangzhou = "SELECT chinese_name, english_name, school_profile_chinese, tuition_fees, gz_crm_id FROM crm_gz_school"

		try:

			cr.execute(query_school_gangzhou)

			columns = [desc[0] for desc in cr.description]

		except Exception, e:

			print 'We could not look for gangzhou schools, here are the reasons: ', e

		schools_gz = cr.fetchall()

		guangzhou_school_not_in_db = []

		count = 0 

		if schools_gz is not None:

			for school in schools_gz:

				query_schools = 'SELECT DISTINCT(school_id) FROM schools WHERE english_name LIKE %s OR chinese_name LIKE %s'

				update_schools = 'UPDATE schools SET (school_profile_chinese, tuition_fees, school_profile_origin, crm_guangzhou_id) = (%s, %s, %s, %s) WHERE school_id = %s'

				origin = 'Gangzhou Database'

				try:

					cr.execute(query_schools, ( str(school[1]).strip() + '%', str(school[0]).strip() + '%',))

				except Exception, e:

					print "We could not fetch the schools, here are the reasons: ", e

				school_id = cr.fetchone()

				if school_id is not None:

					try:

						cr.execute(update_schools, ( str(school[2]), str(school[3]), str(origin), int(school[4]), int(school_id[0]), ))

						print "You succesfully updated this table"

						count += 1

					except Exception, e:

						print "We could not update the schools, here are the reasons: ", e

				else:

					print 'This school is not commun to any of those tables'

					guangzhou_school_not_in_db.insert(0,school)

		print count

		print guangzhou_school_not_in_db

		print len(guangzhou_school_not_in_db)

		df_schools_gangzhou = pd.DataFrame(list(guangzhou_school_not_in_db), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_schools_guangzhou = pd.ExcelWriter('guangzhou_not_found_school_list.xlsx')

		df_schools_gangzhou.to_excel(writer_schools_guangzhou, sheet_name='bar', encoding='utf-8')

		writer_schools_guangzhou.save()

		conn.commit()

		conn.close()

	def fetch_crm_be_schools(self):

		conn = self.__connect()

		cr = conn.cursor()

		beijing_school_not_in_db = []

		query_school_beijing = "SELECT chinese_name, english_name, be_crm_id FROM crm_be_school"

		try:

			cr.execute(query_school_beijing)

			columns = [desc[0] for desc in cr.description]

		except Exception, e:

			print 'We could not look for beijing schools, here are the reasons: ', e

		schools_be = cr.fetchall()

		count = 0 

		if schools_be is not None:

			for school in schools_be:

				query_schools = 'SELECT DISTINCT(school_id) FROM schools WHERE english_name LIKE %s OR chinese_name LIKE %s'

				update_schools = 'UPDATE schools SET (crm_beijing_id) = (%s) WHERE school_id = %s'

				try:

					cr.execute(query_schools, ( str(school[1]).strip() + '%', str(school[0]).strip() + '%',))

				except Exception, e:

					print "We could not fetch the schools, here are the reasons: ", e

				school_id = cr.fetchone()

				if school_id is not None:

					try:

						cr.execute(update_schools, ( str(school[2]), int(school_id[0]), ))

						print "You succesfully updated this table"

						count += 1

					except Exception, e:

						print "We could not update the schools, here are the reasons: ", e

				else:

					print 'This school is not commun to any of those tables'

					beijing_school_not_in_db.insert(0,school)

		print count

		print beijing_school_not_in_db

		print len(beijing_school_not_in_db)
		
		df_schools_beijing = pd.DataFrame(list(beijing_school_not_in_db), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_schools_beijing = pd.ExcelWriter('beijing_not_found_school_list.xlsx')

		df_schools_beijing.to_excel(writer_schools_beijing, sheet_name='bar', encoding='utf-8')

		writer_schools_beijing.save()

		conn.commit()

		conn.close()



Go = link_crm_school_schools()

Go.fetch_crm_gz_schools()

Go.fetch_crm_be_schools()