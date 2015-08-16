from sqlalchemy import create_engine
import pandas as pd
import sys
import csv
from StringIO import StringIO
import psycopg2
from lxml.html.clean import clean_html
import lxml.html
import bs4

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

class clean_this_html():

	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	def clean_my_major(self):

		conn = self.connect()

		cr = conn.cursor()

		query_mymajor_table = "SELECT school_id, introduction, logo, link FROM mymajor_school"

		err = 0

		mymajorurl = 'http://www.mymajors.com/'

		try:

			cr.execute(query_mymajor_table)

		except Exception, e:

			print 'The requeset select failled, here the reason: ', e

			err += 1

		if err == 0:

			for idx, val in enumerate(cr.fetchall()):

				if val[1]:

					introduction = lxml.html.fromstring(val[1])

					introduction = introduction.text_content()

					logo = mymajorurl + str(val[2])

					link = mymajorurl + str(val[3])

					print '############################# INDEX: ', idx, '################ logo: ', logo ,'################## link: ', link ,'#################### introduction: ',introduction

					query_update = "UPDATE mymajor_school SET (introduction, logo, link) = (%s, %s, %s) WHERE school_id = %s"

					try:

						cr.execute(query_update, (introduction, logo, link , val[0]))

						print 'successfully updated'

					except Exception, e:

						print 'The update did fail, here are the reasons: ', e

		conn.commit()

		conn.close()

		print 'DONNNNNNNNEEEEEE'


	def clean_51_offer(self):

		conn = self.connect()

		cr = conn.cursor()

		query_mymajor_table = "SELECT school_id, logo, link FROM school_51_offers"

		err = 0

		offerurl = 'http://www.51offer.com/'

		try:

			cr.execute(query_mymajor_table)

		except Exception, e:

			print 'The requeset select failled, here the reason: ', e

			err += 1

		if err == 0:

			for idx, val in enumerate(cr.fetchall()):

				if val[1]:

					logo = offerurl + str(val[2])

					soup = bs4.BeautifulSoup(val[1], "lxml")

					link = soup.select('img')

					link = link[0].get('data-lazy',None)

					print '############################# INDEX: ', idx, '################ logo: ', logo ,'################## link: ', link 

					query_update = "UPDATE school_51_offers SET (logo, link) = (%s, %s) WHERE school_id = %s"

					try:

						cr.execute(query_update, (logo, link , val[0]))

						print 'successfully updated'

					except Exception, e:

						print 'The update did fail, here are the reasons: ', e

		conn.commit()

		conn.close()

		print 'DONNNNNNNNEEEEEE'


	def clean_4icu(self):

		conn = self.connect()

		cr = conn.cursor()

		query_mymajor_table = "SELECT school_id, introduction, logo, link FROM icu_school"

		err = 0

		icu = 'http://www.4icu.org'

		try:

			cr.execute(query_mymajor_table)

		except Exception, e:

			print 'The requeset select failled, here the reason: ', e

			err += 1

		if err == 0:

			for idx, val in enumerate(cr.fetchall()):

				if val[1]:

					introduction = lxml.html.fromstring(val[1])

					introduction = introduction.text_content()

					logo = str(val[2]).replace('//','')

					link = icu + val[3]

					print '############################# INDEX: ', idx, '################ logo: ', logo ,'################## link: ', link ,'#################### introduction: ',introduction

					query_update = "UPDATE icu_school SET (introduction, logo, link) = (%s, %s, %s) WHERE school_id = %s"

					try:

						cr.execute(query_update, (introduction, logo, link , val[0]))

						print 'successfully updated'

					except Exception, e:

						print 'The update did fail, here are the reasons: ', e

		conn.commit()

		conn.close()

		print 'DONNNNNNNNEEEEEE'


New_object = clean_this_html()

#New_object.clean_my_major()

#New_object.clean_51_offer()

New_object.clean_4icu()