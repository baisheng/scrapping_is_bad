 #coding=utf-8 

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

class scrap_nihaowang_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://school.nihaowang.com/'

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

		query_create_table = "CREATE TABLE nihaowang_school (school_id serial PRIMARY KEY, english_name varchar(150) NOT NULL, chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text, acronym varchar)"  

		query_test = "select exists(select relname from pg_class where relname = 'nihaowang_school' and relkind='r')"

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

		query_create_table = "CREATE TABLE nihaowang_major (major_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, link varchar, introduction text, major_category_id integer)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'nihaowang_major' and relkind='r')"

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

		query_create_table = "CREATE TABLE nihaowang_degree_type (degree_type_id serial PRIMARY KEY, english_name varchar, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'nihaowang_degree_type' and relkind='r')"

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

		query_create_table = "CREATE TABLE nihaowang_many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id int, FOREIGN KEY (major_id) REFERENCES nihaowang_major (major_id), FOREIGN KEY (school_id) REFERENCES nihaowang_school (school_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'nihaowang_many2manytable' and relkind='r')"

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

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE nihaowang_major_cateogry (major_category_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'nihaowang_major_cateogry' and relkind='r')"

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

	def thank_you_for_your_schools_nihaowang(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		country_desire = ["4","5","50","58","65","69","70","71","74","85","89","150","151","174","175"]

		for i in country_desire:

			page_to_scrap = root_url + str(i) + '-0-0-0-0-0-0-0-0-0-0-2-2-5-0-0-0-0-0-0-1-01.html'

			print page_to_scrap

			response_school = requests.get(page_to_scrap)

			if response_school:

				soup = bs4.BeautifulSoup(response_school.text,'lxml')

				country = soup.select("#ulParms li")[0]

				country.hidden = True

				print country.encode("utf-8").strip().replace('国家：','')

				schools = soup.select("div.college div.college_list")

				schools2 = soup.select("div.college div.college_list2")

				if len(schools) > 0 or len(schools2) > 0:

					page_count = soup.select('div.fengye #bCount')[0]

					page_count.hidden = True

					for n in range(1,int(str(page_count))):

						page_to_scrap = root_url + str(i) + '-0-0-0-0-0-0-0-0-0-0-2-2-5-0-0-0-0-0-0-' + str(n) + '-01.html'

						response_school = requests.get(page_to_scrap)

						soup = bs4.BeautifulSoup(response_school.text,'lxml')

						schools = soup.select("div.college div.college_list")

						schools2 = soup.select("div.college div.college_list2")

						if len(schools) > 0 or len(schools2) > 0:

							for school in schools:

								logo = school.select('div.college_img a img')[0].get('src',None)

								link = school.select('div.college_img a')[0].get('href',None)

								english_name = school.select('div.en_title a')[1]

								english_name.hidden = True

								chinese_name = school.select('div.en_title a strong')[0]

								chinese_name.hidden = True

								if school.select('div.college_intro_one p'):

									introduction = school.select('div.college_intro_one p')[0]

									introduction.hidden = True

								else:

									introduction = 'Not communicated'

								world_ranking = school.select('div.college_content div.number span.span_c')[0]

								world_ranking.hidden = True

								print "############## logo: ", logo, "############## link: ", link,  "############## english name: ", english_name, "############## chinese name: ", chinese_name, "############## introduction: ", introduction, "############## rank: ", world_ranking

								query_school_exist = "SELECT * FROM nihaowang_school WHERE chinese_name = %s"

								query_insert_school = "INSERT INTO nihaowang_school (english_name, chinese_name, introduction, logo, link, world_ranking, country) VALUES (%s, %s, %s, %s, %s, %s, %s)"

								query_update_school = "UPDATE nihaowang_school SET(english_name, chinese_name, introduction, logo, link, world_ranking, country) = (%s, %s, %s, %s, %s, %s, %s) WHERE chinese_name = %s"

								try:

									cr.execute(query_school_exist, (str(chinese_name),))

								except Exception, e:

									print 'The query select failed, here are the reasons: ', e

								if cr.fetchone() is None:

									try:

										cr.execute(query_insert_school, (str(english_name), str(chinese_name), str(introduction), str(logo), str(link), str(world_ranking),))

										print 'You did insert a new school into the db'

									except Exception, e:

										print 'You school insertion did fail, here are the reasons: ', e

								else:

									print 'exists'

									try:

										cr.execute(query_update_school, (str(english_name), str(chinese_name), str(introduction), str(logo), str(link), str(world_ranking), str(chinese_name),))

										print 'You did update an existing school into the db'

									except Exception, e:

										print 'You school update did fail, here are the reasons: ', e

							
							for school in schools2:

								logo = school.select('div.college_img a img')[0].get('src',None)

								link = school.select('div.college_img a')[0].get('href',None)

								english_name = school.select('div.en_title a')[1]

								english_name.hidden = True

								chinese_name = school.select('div.en_title a strong')[0]

								chinese_name.hidden = True

								if school.select('div.college_intro_one p'):

									introduction = school.select('div.college_intro_one p')[0]

									introduction.hidden = True

								else:

									introduction = 'Not communicated'

								world_ranking = school.select('div.college_content div.number span.span_c')[0]

								world_ranking.hidden = True

								print "############## logo: ", logo, "############## link: ", link,  "############## english name: ", english_name, "############## chinese name: ", chinese_name, "############## introduction: ", introduction, "############## rank: ", world_ranking

								query_school_exist = "SELECT * FROM nihaowang_school WHERE chinese_name = %s"

								query_insert_school = "INSERT INTO nihaowang_school (english_name, chinese_name, introduction, logo, link, world_ranking, country) VALUES (%s, %s, %s, %s, %s, %s, %s)"

								query_update_school = "UPDATE nihaowang_school SET (english_name, chinese_name, introduction, logo, link, world_ranking, country) = (%s, %s, %s, %s, %s, %s, %s) WHERE chinese_name = %s"

								try:

									cr.execute(query_school_exist, (str(chinese_name),))

								except Exception, e:

									print 'The query select failed, here are the reasons: ', e

								if cr.fetchone() is None:

									try:

										cr.execute(query_insert_school, (str(english_name), str(chinese_name), str(introduction), str(logo), str(link), str(world_ranking), str(country),))

										print 'You did insert a new school into the db'

									except Exception, e:

										print 'You school insertion did fail, here are the reasons: ', e

								else:

									print 'exists'

									try:

										cr.execute(query_update_school, (str(english_name), str(chinese_name), str(introduction), str(logo), str(link), str(world_ranking), str(country), str(chinese_name),))

										print 'You did update an existing school into the db'

									except Exception, e:

										print 'You school update did fail, here are the reasons: ', e

								conn.commit()

							print 'Yeeepppppppppp, school id: ', i, 'page: ', n

						else:

							print 'NOOOOOOOOOO school id: ', i, 'page: ', n

				else:

					print 'NOOOOOOOOOO', i

	def get_degree_type(self):

		self.create_degree_type_table()

		conn = self.connect()	

		cr = conn.cursor()

		page = str(root_url) + '2844.html'

		response = requests.get(page)

		soup = bs4.BeautifulSoup(response.text, "lxml")

		degree_type = soup.select("div.intro_content div.degree ul li a")

		for idx, val in enumerate(degree_type):

			if idx < 5:

				val.hidden = True

				query_test = "SELECT * FROM nihaowang_degree_type WHERE chinese_name = %s"

				try:

					cr.execute(query_test, (str(val), ))

				except Exception, e:

					print "Your request for selection did fail, here is the reason: ", e

				test = cr.fetchone()

				if test is None:

					query_insertion = "INSERT INTO nihaowang_degree_type  (chinese_name) VALUES (%s)"

					try:

						cr.execute(query_insertion, (str(val), ))

					except Exception, e:

						print "Your insertion did fail, here is the reason: ", e

				else:

					print 'Degree type Exists'

		conn.commit()

		conn.close()	

	def get_the_school_detail_page(self):

		self.create_major_table()

		#self.create_many_to_many_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_school = "SELECT chinese_name, link, school_id FROM nihaowang_school ORDER BY school_id" 

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did fail... Here the reason: ', e


		for idx,value in enumerate(cr.fetchall()):

				# First we fetch all the require information on the page of 4icu website details page of the school

				school_detail = value[1]

				response_school = requests.get(school_detail)

				print school_detail

				soup = bs4.BeautifulSoup(response_school.text, "lxml")

				url = soup.select("div.school_list div a")[0].get('href', None)

				url = root_url + url

				city = soup.select('ul#ul_InfoBase li')[1]

				city.hidden = True

				city = str(city).replace('<span>城市：</span>','')

				address = soup.select('div.contact div.contact_left p')[0]

				address.hidden = True

				logo = soup.select('div.school_hd_scLogo img')[0].get('src', None)

				national_ranking = soup.select('div.school_hd_rank_self div.rank_numbo')[0]

				national_ranking.hidden = True

				global_ranking = soup.select('div.school_hd_rank div.rank_numbo')[1]

				global_ranking.hidden = True

				print '##################  National ranking: ', national_ranking, '##################  Global ranking: ', global_ranking, '##################  url: ', url,  '##################  city: ', city, '##################  logo: ', logo,

		conn.commit()

		conn.close()	


New_object = scrap_nihaowang_website()

# New_object.thank_you_for_your_schools_nihaowang()

# New_object.get_degree_type()

New_object.get_the_school_detail_page()


