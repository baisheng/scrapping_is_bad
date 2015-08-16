import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

class scrap_niuschools_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://www.niuschools.com'

	pg_table = 'niuschools_school'

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

		query_create_table = "CREATE TABLE niuschools_school (school_id serial PRIMARY KEY, english_name varchar(150) NOT NULL, chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text, acronym varchar)"  

		query_test = "select exists(select relname from pg_class where relname = 'niuschools_school' and relkind='r')"

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

		query_create_table = "CREATE TABLE niuschools_major (major_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, link varchar, introduction text, major_category_id integer"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'niuschools_major' and relkind='r')"

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

		query_create_table = "CREATE TABLE niuschools_degree_type (degree_type_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'niuschools_degree_type' and relkind='r')"

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

		query_create_table = "CREATE TABLE niuschools_many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id int, FOREIGN KEY (major_id) REFERENCES niuschools_major (major_id), FOREIGN KEY (school_id) REFERENCES niuschools_school (school_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'niuschools_many2manytable' and relkind='r')"

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

	def thank_you_for_your_schools_niuschools(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		countries = ['US','CA','UK','AU','SG','HK']

		for idx, val in enumerate(countries):

			school_detail = str(root_url) + '/rev/index/country_id/' + str(idx + 1) +'/p/'

			for i in range(1,12):

				final_url = school_detail + str(i)

				print final_url

				response_school = requests.get(final_url)

				if response_school:

					soup = bs4.BeautifulSoup(response_school.text, "lxml")

					universities = soup.select('div.b-result-one')

					for university in universities:

						logo = university.select("div.b-rs-logo a img")[0].get('src', None)

						logo = root_url + logo

						chinese_name = university.select("li.b-ren-zh a.b-ren-cnname")[0]

						chinese_name.hidden = True

						english_name = university.select("li.b-ren-en a.b-ren-enname")[0]

						english_name.hidden = True

						global_ranking = university.select("ul.b-reo-top li")[0]

						global_ranking.hidden = True

						link = university.select("div.b-rs-logo a")[0].get('href', None)

						link = root_url + link

						query_test_exist = "SELECT * FROM niuschools_school WHERE english_name = %s"

						query_insert_school = "INSERT INTO niuschools_school (english_name, chinese_name, world_ranking, logo, link, country) VALUES (%s, %s, %s, %s, %s, %s)"

						query_update_school = "UPDATE niuschools_school SET (english_name, chinese_name, world_ranking, logo, link, country) = (%s, %s, %s, %s, %s, %s) WHERE english_name = %s"

						try:

							cr.execute(query_test_exist, (str(english_name),))

						except Exception, e:

							print 'The request select did fail, here are the reasons: ', e

						if cr.fetchone() is None:

							try:

								cr.execute(query_insert_school, (str(english_name), str(chinese_name).encode('utf-8'), str(global_ranking), logo, link, str(val)), )

								print 'You did create a new school'

							except Exception, e:

								print "We cannot do the insertion, here are the reasons: ", e

						else:

							print 'Exists'

							try:

								cr.execute(query_update_school, (str(english_name), str(chinese_name).encode('utf-8'), str(global_ranking), str(logo), str(link),str(val), str(english_name),) )

								print 'You did update your school'

							except Exception, e:

								print "We cannot do the update, here are the reasons: ", e


						print "###################### logo: ", logo, "###################### chinese_name: ", chinese_name.encode('utf-8'), "###################### english_name: ", english_name, "###################### global_ranking: ", global_ranking, "###################### link: ", link					

			conn.commit()

		conn.close()	

	def get_the_school_detail_page(self):

		self.create_major_table()

		self.create_many_to_many_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_school = "SELECT english_name, link, school_id FROM niuschools_school ORDER BY school_id" 

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e


		for idx,value in enumerate(cr.fetchall()):

				# First we fetch all the require information on the page of 4icu website details page of the school

				school_detail = value[1]

				response_school = requests.get(school_detail)

				print school_detail

				soup = bs4.BeautifulSoup(response_school.text, "lxml")

				url = soup.select("p.l_school_link_p a")[0]

				url.hidden = True

				location = soup.select('p.l_school_place_p')[0]

				location.hidden = True

				introduction = soup.select('div.l_school_detaileds_more p.l_school_p_3')[0]

				introduction.hidden = True

				majors = soup.select('ul.hoverp')

				for major in majors:

					major_chinese_name = major.select('li')[0]

					major_chinese_name.hidden = True

					major_english_name = major.select('li')[1]

					major_english_name.hidden = True

					query_exist_major = "SELECT * FROM niuschools_major WHERE english_name = %s"

					query_insert_major = "INSERT INTO niuschools_major (english_name, chinese_name) VALUES (%s, %s)"

					try: 

						cr.execute(query_exist_major, (str(major_english_name),))

					except Exception, e:

						print "The request selection did fail, here are the reasons: ", e

					if cr.fetchone() is None:

						try:

							cr.execute(query_insert_major, (str(major_english_name),str(major_chinese_name),))

							print "you did create a new major"

						except Exception, e:

							print "You could not create a new major, here are the reasons: ", e

					try: 

						cr.execute(query_exist_major, (str(major_english_name),))

					except Exception, e:

						print "The request selection did fail, here are the reasons: ", e

					major_id = cr.fetchone()[0]

					query_relation_exist = "SELECT * FROM niuschools_many2manytable WHERE major_id = %s AND school_id= %s"	

					query_relation_create = "INSERT INTO niuschools_many2manytable (major_id, school_id) VALUES (%s, %s)"		
					
					try: 

						cr.execute(query_relation_exist, (major_id, value[2] ,))

					except Exception, e:

						print "The request selection did fail, here are the reasons: ", e	

					if cr.fetchone() is None:

						try: 

							cr.execute(query_relation_create, (major_id, value[2] ,))

							print "You created a relation"

						except Exception, e:

							print "The request selection did fail, here are the reasons: ", e	

		conn.commit()

		conn.close()	

New_object = scrap_niuschools_website()

New_object.thank_you_for_your_schools_niuschools()

New_object.get_the_school_detail_page()
