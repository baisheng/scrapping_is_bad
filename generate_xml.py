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

class generate_xml():

	def generate_my_random_number(self):

		random_number = randint(0, 2000)

		return random_number

	def connect(self):

		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:

			conn = psycopg2.connect(conn_string)

		except:

			print 'I am not able to connect to the database'

		return conn 

	def generate_xls_file(self):

		conn = self.connect()

		cr = conn.cursor()

		# for table in ["mymajor_school","icu_school","school_crm","tschoolbe","school_crm"]:

		final_table_mymajor = []

		final_table_ic = []

		final_table_gangzhou = []

		final_table_beijing = []

		final_table_51 = []

		for i in range(0,600):

			my_random_number = self.generate_my_random_number()

			#print 'You have been generated xml file here are the random number: ', my_random_number

			query_select_random_school_mymajor = "SELECT my.school_id, my.english_name, my.chinese_name, my.introduction, my.tuition_fees, my.country, my.link, my.address, my.city, my.logo, my.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name  WHERE my.school_id = %s "

			query_select_random_school_icu = "SELECT ic.school_id, ic.english_name, ic.chinese_name, ic.introduction, ic.tuition_fees, ic.country, ic.link, ic.address, ic.city, ic.logo, ic.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name  WHERE my.school_id = %s "

			query_select_random_school_gangzhou = "SELECT schcrmga.school_id, schcrmga.english_name, schcrmga.chinese_name, schcrmga.introduction, schcrmga.tuition_fees, schcrmga.country, schcrmga.link, schcrmga.address, schcrmga.city, schcrmga.logo, schcrmga.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name  WHERE my.school_id = %s "

			query_select_random_school_beijing = "SELECT schcrmbe.school_id, schcrmbe.english_name, schcrmbe.chinese_name, schcrmbe.introduction, schcrmbe.tuition_fees, schcrmbe.country, schcrmbe.link, schcrmbe.address, schcrmbe.city, schcrmbe.logo, schcrmbe.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name  INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name WHERE my.school_id = %s "
			#query_select_random_school = "SELECT my.school_id, my.english_name, my.chinese_name FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name WHERE my.school_id = %s"

			query_select_random_school_51 = "SELECT offer.school_id, offer.english_name, offer.chinese_name, offer.introduction, offer.tuition_fees, offer.country, offer.link, offer.address, offer.city, offer.logo, offer.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name  WHERE my.school_id = %s "

			try:

				cr.execute(query_select_random_school_mymajor % (my_random_number,))

			except Exception, e:

				print 'The school selection did fail, here is the reason', e

			data_mymajor = cr.fetchall()


			try:

				cr.execute(query_select_random_school_icu % (my_random_number,))

			except Exception, e:

				print 'The school selection did fail, here is the reason', e

			data_icu = cr.fetchall()


			try:

				cr.execute(query_select_random_school_gangzhou % (my_random_number,))

			except Exception, e:

				print 'The school selection did fail, here is the reason', e

			data_gangzhou = cr.fetchall()


			try:

				cr.execute(query_select_random_school_beijing % (my_random_number,))

			except Exception, e:

				print 'The school selection did fail, here is the reason', e

			data_beijing = cr.fetchall()

			try:

				cr.execute(query_select_random_school_51 % (my_random_number,))

			except Exception, e:

				print 'The school selection did fail, here is the reason', e

			data_51 = cr.fetchall()

			# if data:

			# 	final_table = final_table.insert(0, data)

			columns = [desc[0] for desc in cr.description]

			final_table_mymajor += data_mymajor

			final_table_ic += data_icu

			final_table_gangzhou += data_gangzhou

			final_table_beijing += data_beijing

			final_table_51 += data_51

			#print final_table

		print len(final_table_mymajor)

		print len(final_table_ic)

		print len(final_table_gangzhou)

		print len(final_table_beijing)

		print len(final_table_51)

		df_mymajor = pd.DataFrame(list(final_table_mymajor), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_major = pd.ExcelWriter('mymajor.xlsx')

		df_mymajor.to_excel(writer_major, sheet_name='bar', encoding='utf-8')

		df_icu= pd.DataFrame(list(final_table_ic), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_icu = pd.ExcelWriter('icu.xlsx')

		df_icu.to_excel(writer_icu, sheet_name='bar', encoding='utf-8')

		df_beijing = pd.DataFrame(list(final_table_beijing), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_beijing = pd.ExcelWriter('beijing.xlsx')

		df_beijing.to_excel(writer_beijing, sheet_name='bar', encoding='utf-8')

		df_guangzhou = pd.DataFrame(list(final_table_gangzhou), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_gangzhou = pd.ExcelWriter('gangzhou.xlsx')

		df_guangzhou.to_excel(writer_gangzhou, sheet_name='bar', encoding='utf-8')

		df_51 = pd.DataFrame(list(final_table_51), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer_51 = pd.ExcelWriter('51.xlsx')

		df_51.to_excel(writer_51, sheet_name='bar', encoding='utf-8')

		writer_major.save()

		writer_icu.save()

		writer_beijing.save()

		writer_gangzhou.save()

		writer_51.save()

	def try_joint(self):

		conn = self.connect()

		cr = conn.cursor()

		query_select_common_school_mymajor = "SELECT my.school_id, my.english_name, my.chinese_name, my.introduction, my.tuition_fees, my.country, my.link, my.address, my.city, my.logo, my.acronym FROM mymajor_school AS my INNER JOIN icu_school AS ic ON ic.english_name = my.english_name INNER JOIN tschoolbe AS schcrmbe ON schcrmbe.english_name = my.english_name INNER JOIN school_crm AS schcrmga ON schcrmga.english_name = my.english_name INNER JOIN school_51_offers AS offer ON offer.english_name = my.english_name"

		try:

			cr.execute(query_select_common_school_mymajor)

		except Exception, e:

			print 'The school selection did fail, here is the reason', e

		data_mymajor = cr.fetchall()

		columns = [desc[0] for desc in cr.description]

		df = pd.DataFrame(list(data_mymajor), columns=columns)

		#writer = pd.ExcelWriter('test.xlsx')

		writer = pd.ExcelWriter('common.xlsx')

		df.to_excel(writer, sheet_name='bar', encoding='utf-8')

		writer.save()


# New_object = generate_xml()

# New_object.generate_xls_file()

# New_object.try_joint()


