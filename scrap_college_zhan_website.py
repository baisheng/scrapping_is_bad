 #coding=utf-8 

import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import lxml.html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

class scrap_college_zhan_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://college.zhan.com/'

	pg_table = 'college_zhan_school'

	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	def create_school_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE college_zhan_school (school_id serial PRIMARY KEY, english_name varchar(150), chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text, acronym varchar)"  

		query_test = "select exists(select relname from pg_class where relname = 'college_zhan_school' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'you created you table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()

	def create_major_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE college_zhan_major (major_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, link varchar, introduction text, major_category_id integer"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'college_zhan_major' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'you created you table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()			

	def create_degree_type_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE college_zhan_degree_type (degree_type_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'college_zhan_degree_type' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'you created you table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()	

	def create_many_to_many_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE college_zhan_many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id int, FOREIGN KEY (major_id) REFERENCES college_zhan_major (major_id), FOREIGN KEY (school_id) REFERENCES college_zhan_school (school_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'college_zhan_many2manytable' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print "You finally created your table"

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()

	def create_major_category_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE niuschools_major_cateogry (major_category_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'niuschools_major_cateogry' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did fail... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'you created you table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()	

	def __statment_function(self, arg):

		if arg == 'c3':

			return 'US'

		if arg == 'c28':

			return 'CA'

		if arg == 'c19':

			return 'UK'

		if arg == 'c55':

			return 'AU'

		if arg == 'c45':

			return 'GE'

		if arg == 'c46':

			return 'FR'

		if arg == 'c51':

			return 'JA'


	def thank_you_for_your_schools_college_zhan(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		countries = ['c3', 'c28', 'c19', 'c55', 'c45', 'c46' ,'c51']

		for idx, val in enumerate(countries):

			if idx > 1: 

				school_detail = str(root_url) + 'list/' + str(val) + '-p'

				for i in range(1,64):

					final_url = school_detail + str(i) + '/'

					print final_url

					response_school = requests.get(final_url)

					if response_school:

						soup = bs4.BeautifulSoup(response_school.text, "lxml")

						universities = soup.select('div.row.row-right-left-no div.col-lg-12.col-md-12.col-xs-12')

						print len(universities)

						for university in universities:

							country = self.__statment_function(str(val))

							logo = university.select("img")[0].get('src', None)

							chinese_name = university.select("div.pull-left.title a")[0]

							chinese_name.hidden = True

							link = university.select("div.pull-left.title a")[0].get('href', None)

							query_test_exist = "SELECT * FROM college_zhan_school WHERE chinese_name = %s"

							query_insert_school = "INSERT INTO college_zhan_school (chinese_name, logo, link, country) VALUES (%s, %s, %s, %s)"

							query_update_school = "UPDATE college_zhan_school SET (chinese_name, logo, link, country) = (%s, %s, %s, %s) WHERE chinese_name = %s"

							try:

								cr.execute(query_test_exist, (str(chinese_name).encode('utf-8'),))

							except Exception, e:

								print 'The request select did fail, here are the reasons: ', e

							if cr.fetchone() is None:

								try:

									cr.execute(query_insert_school, (str(chinese_name).encode('utf-8'), logo, link, country))

									print 'You did create a new school'

								except Exception, e:

									print "We cannot do the insertion, here are the reasons: ", e

							else:

								print 'Exists'

								try:

									cr.execute(query_update_school, (str(chinese_name).encode('utf-8'),str(logo), str(link), country, str(chinese_name),) )

									print 'You did update your school'

								except Exception, e:

									print "We cannot do the update, here are the reasons: ", e


							print "###################### logo: ", logo, "###################### chinese_name: ", chinese_name, "###################### link: ", link					

					conn.commit()

		conn.close()	

	def get_the_school_detail_page(self):

		# self.create_major_table()

		# self.create_many_to_many_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_school = "SELECT chinese_name, link, school_id FROM college_zhan_school ORDER BY school_id" 

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e


		for idx,value in enumerate(cr.fetchall()):

				# First we fetch all the require information on the page of 4icu website details page of the school

				link = value[1]

				link = link.replace('\n','')

				print link

				response_school = requests.get(link)

				soup = bs4.BeautifulSoup(response_school.text, "lxml")

				name = soup.select("div.big-title-big")[0]

				name.hidden = True

				name = str(name).split('/')

				chinese_name = name[0]

				english_name = name[1]

				tuition_fees = soup.select('div.instruction.fee em')[0]

				tuition_fees.hidden = True

				location = soup.select('div.location-map')[0]

				location.hidden = True

				location = str(location).replace('所在地：','')

				location = lxml.html.fromstring(location)

				location = location.text_content()

				query_update = 'UPDATE college_zhan_school SET (english_name, chinese_name, tuition_fees, address) = (%s, %s, %s, %s) WHERE school_id = %s'

				try:

					cr.execute(query_update, (str(english_name), str(chinese_name), str(tuition_fees), str(location), str(value[2]),))

					print 'YOUPPPPPPIIEEE you udapted your school information'

				except Exception, e:

					print "We cannot update the school here is the reason: ", e

				print "############### Chinese name: ", chinese_name, "############### English name: ", english_name, "############### Tuition fees: ", tuition_fees, "############### Address: ", location

		conn.commit()

		conn.close()	

	def get_the_ranking(self):

		conn = self.connect()	

		cr = conn.cursor()

		world_ranking_url = 'http://college.zhan.com/ranking/r21-p'

		for i in range(1,15):

			world_ranking_url_new = world_ranking_url + str(i) + '/'

			print world_ranking_url_new

			response_school = requests.get(world_ranking_url_new)

			soup = bs4.BeautifulSoup(response_school.text,'lxml')

			schools = soup.select('div.row.row-right-left-no div.col-lg-12.col-md-12.col-xs-12')

			for idx, val in enumerate(schools):

				global_rank = val.select('div.col-lg-2.col-md-3.col-sm-3.col-xs-12.no-padding.position-r span')[0]

				global_rank.hidden = True

				name = val.select('div.index-middle-info-1 a')[0]

				name.hidden = True

				name = str(name).split('/')

				chinese_name = name[0]

				english_name = name[1]

				SAT_is_there = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list')[1]

				if 'SAT成绩' in str(SAT_is_there):

					SAT_score = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list span')[5]

					SAT_score.hidden = True

				else:

					SAT_score = 'NOT COMMUNICATED'

				print '############# CHINESE NAME: ', chinese_name.encode('utf-8'), '############# GLOBAL RANK: ', global_rank, '############# ENGLISH NAME: ', english_name, '################ SAT SCORE: ', SAT_score

				query_update = 'UPDATE college_zhan_school SET (world_ranking) = (%s) WHERE english_name = %s'

				try:

					cr.execute(query_update, (str(global_rank), str(english_name),))

					print 'YOUPPPPPPIIEEE you udapted your school information'

				except Exception, e:

					print "We cannot update the school here is the reason: ", e

		canada_ranking_url = 'http://college.zhan.com/ranking/r25-p'

		for i in range(1,2):

			canada_ranking_url_new = canada_ranking_url + str(i) + '/'

			print canada_ranking_url_new

			response_school = requests.get(canada_ranking_url_new)

			soup = bs4.BeautifulSoup(response_school.text,'lxml')

			schools = soup.select('div.row.row-right-left-no div.col-lg-12.col-md-12.col-xs-12')

			for idx, val in enumerate(schools):

				global_rank = val.select('div.col-lg-2.col-md-3.col-sm-3.col-xs-12.no-padding.position-r span')[0]

				global_rank.hidden = True

				name = val.select('div.index-middle-info-1 a')[0]

				name.hidden = True

				name = str(name).split('/')

				chinese_name = name[0]

				english_name = name[1]

				SAT_is_there = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list')[1]

				if 'SAT成绩' in str(SAT_is_there):

					SAT_score = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list span')[5]

					SAT_score.hidden = True

				else:

					SAT_score = 'NOT COMMUNICATED'

				print '############# CHINESE NAME: ', chinese_name.encode('utf-8'), '############# GLOBAL RANK: ', global_rank, '############# ENGLISH NAME: ', english_name, '################ SAT SCORE: ', SAT_score

				query_update = 'UPDATE college_zhan_school SET (national_ranking) = (%s) WHERE english_name = %s'

				try:

					cr.execute(query_update, (str(global_rank), str(english_name),))

					print 'YOUPPPPPPIIEEE you udapted your school information'

				except Exception, e:

					print "We cannot update the school here is the reason: ", e

		uk_ranking_url = 'http://college.zhan.com/ranking/r24-p'

		for i in range(1,4):

			uk_ranking_url_new = uk_ranking_url + str(i) + '/'

			print uk_ranking_url_new

			response_school = requests.get(uk_ranking_url_new)

			soup = bs4.BeautifulSoup(response_school.text,'lxml')

			schools = soup.select('div.row.row-right-left-no div.col-lg-12.col-md-12.col-xs-12')

			for idx, val in enumerate(schools):

				global_rank = val.select('div.col-lg-2.col-md-3.col-sm-3.col-xs-12.no-padding.position-r span')[0]

				global_rank.hidden = True

				name = val.select('div.index-middle-info-1 a')[0]

				name.hidden = True

				name = str(name).split('/')

				chinese_name = name[0]

				english_name = name[1]

				SAT_is_there = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list')[1]

				if 'SAT成绩' in str(SAT_is_there):

					SAT_score = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list span')[5]

					SAT_score.hidden = True

				else:

					SAT_score = 'NOT COMMUNICATED'

				print '############# CHINESE NAME: ', chinese_name.encode('utf-8'), '############# GLOBAL RANK: ', global_rank, '############# ENGLISH NAME: ', english_name, '################ SAT SCORE: ', SAT_score

				query_update = 'UPDATE college_zhan_school SET (national_ranking) = (%s) WHERE english_name = %s'

				try:

					cr.execute(query_update, (str(global_rank), str(english_name),))

					print 'YOUPPPPPPIIEEE you udapted your school information'

				except Exception, e:

					print "We cannot update the school here is the reason: ", e


		us_ranking_url = 'http://college.zhan.com/ranking/r17-p'

		for i in range(1,5):

			us_ranking_url_new = us_ranking_url + str(i) + '/'

			print us_ranking_url_new

			response_school = requests.get(us_ranking_url_new)

			soup = bs4.BeautifulSoup(response_school.text,'lxml')

			schools = soup.select('div.row.row-right-left-no div.col-lg-12.col-md-12.col-xs-12')

			for idx, val in enumerate(schools):

				global_rank = val.select('div.col-lg-2.col-md-3.col-sm-3.col-xs-12.no-padding.position-r span')[0]

				global_rank.hidden = True

				name = val.select('div.index-middle-info-1 a')[0]

				name.hidden = True

				name = str(name).split('/')

				chinese_name = name[0]

				english_name = name[1]

				SAT_is_there = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list')[1]

				if 'SAT成绩' in str(SAT_is_there):

					SAT_score = val.select('div.txt-grey-xs.index-middle-info-4.school_name_list span')[5]

					SAT_score.hidden = True

				else:

					SAT_score = 'NOT COMMUNICATED'

				print '############# CHINESE NAME: ', chinese_name.encode('utf-8'), '############# GLOBAL RANK: ', global_rank, '############# ENGLISH NAME: ', english_name, '################ SAT SCORE: ', SAT_score

				query_update = 'UPDATE college_zhan_school SET (national_ranking) = (%s) WHERE english_name = %s'

				try:

					cr.execute(query_update, (str(global_rank), str(english_name),))

					print 'YOUPPPPPPIIEEE you udapted your school information'

				except Exception, e:

					print "We cannot update the school here is the reason: ", e

		conn.commit()

		conn.close()


# New_object = scrap_college_zhan_website()

# New_object.thank_you_for_your_schools_college_zhan()

# New_object.get_the_school_detail_page()

# New_object.get_the_ranking()
