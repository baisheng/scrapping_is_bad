from parse_csv_file import parse_csv_file
from lxml.html.clean import clean_html
from sqlalchemy import create_engine
from HTMLParser import HTMLParser
import pandas as pd
import numpy as np
import psycopg2
import requests
import bs4
import sys  
import sys
import csv

reload(sys)
sys.setdefaultencoding("utf-8")


class link_crm_tables():

	global major_crm_table

	global school_crm_table

	major_crm_table = 'major_crm'

	school_crm_table = 'school_crm'


	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		#cr = conn.cursor()
		return conn 

	def create_crm_many2many(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE crm_many2many (school_id int NOT NULL, major_id int NOT NULL)"   

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'crm_many2many' and relkind='r')"

		try:

			cr.execute(query_test)

			print 'Yes the table crm_many2many doesn\'t exit'

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'You did create your table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()		


	def populate_crm_many2many(self):

		conn = self.connect()	

		self.create_crm_many2many()

		cr = conn.cursor()

		query_major = "SELECT * FROM major_crm"

		try:

			cr.execute(query_major)

			majors = cr.fetchall()

		except Exception, e:

			print 'Ouppppppssss we cannot connect to the table major, here the reason: ', e

		if majors:

			for major in majors:

				test_foreign_key = "SELECT crm_scho_crmid FROM school_crm WHERE crm_scho_crmid=%s"

				if major[13] != None:

					print int(major[13]), major[0]

					try:

						cr.execute(test_foreign_key, [str(int(major[13]))])

						match = cr.fetchall()

					except Exception, e:

						print 'Ouppppppssss we cannot connect to the table major, here the reason: ', e

					if match:

						insertion_crm_many2many = 'INSERT INTO crm_many2many (school_id,major_id) VALUES (%s,%s)'

						try:

							cr.execute(insertion_crm_many2many, (major[13], major[0], ))

						except Exception, e:

							print 'Ouppppppssss we cannot insert into many2many table, here the reason: ', e

					else:
						print 'no match'
					conn.commit()
		conn.close()

Import_crm_tables =  parse_csv_file()
Import_crm_tables.import_the_table('/home/vagrant/sails_api_newpathway/DBCRM/school.csv','school_crm','school_id')
Import_crm_tables.import_the_table('/home/vagrant/sails_api_newpathway/DBCRM/Major.csv','major_crm','major_id')
Link_the_table = link_crm_tables()
Link_the_table.populate_crm_many2many()

