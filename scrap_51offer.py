 #coding=utf-8 

import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

global initial_location

initial_location = ['uk','us','au','nz','jp','sg']

class scrap_51offer(HTMLParser):

	global pg_user

	global pg_db

	global pg_passwd

	global pg_table

	global root_url

	root_url = 'http://www.51offer.com'

	pg_user = 'postgres'

	pg_db = 'test'

	pg_passwd = 'Password1!'

	pg_table = 'school_51_offers'

	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		#cr = conn.cursor()
		return conn 

	def create_school_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE school_51_offers (school_id serial, english_name varchar(150) NOT NULL, chinese_name varchar(150) NOT NULL, logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar)"  

		query_create_table_index = "CREATE UNIQUE INDEX school_id ON school_51_offers (school_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'school_51_offers' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				try:

					exist = cr.execute(query_create_table_index)

					print 'you created you table'

				except Exception, e:

					print 'Ouppppppssss could create the index... Here the reason: ', e

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()		


	def create_major_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE majors_51_offers (major_id serial, english_name varchar NOT NULL, chinese_name varchar, degree_type varchar)"  

		query_create_table_index = "CREATE UNIQUE INDEX major_id ON majors_51_offers (major_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'majors_51_offers' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				try:

					exist = cr.execute(query_create_table_index)

					print 'you created you table'

				except Exception, e:

					print 'Ouppppppssss could create the index... Here the reason: ', e

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()		

	def create_many_to_many_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE majors_many2many  (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id integer NOT NULL, english_name varchar, chinese_name varchar, major_profile_english text,  major_profile_chinese text, url varchar, FOREIGN KEY (major_id) REFERENCES major (major_id), FOREIGN KEY (school_id) REFERENCES schools (school_id), FOREIGN KEY (degree_type_id) REFERENCES degree_type (degree_type_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'majors_many2many' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()

		conn.close()	


	def thank_you_for_your_school_51offer(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		count = len(initial_location)

		counter_in = 0

		counter_out = 0

		for country in range(0 , count - 1):

			for i in range(1 , 20):

				url_to_get =  str(root_url) + '/school/' + str(initial_location[country]) + '-all-' + str(i) + '.html'

				response = requests.get(url_to_get)

				soup = bs4.BeautifulSoup(response.text, "lxml")

				test_page_exist = soup.select('.noResult')

				if not test_page_exist :

					# We want to extract the link the school page

					link_school = soup.select("a.schoolNameEn")

					school_english_name = soup.select('a.schoolNameEn')

					school_chinese_name = soup.select('a.schoolNameEn strong')

					for n in range(len(soup.select('div.schoolLabel li'))):

						link = link_school[2*n].get('href',None)

						if link is not None:

							link = link

						english_name = school_english_name[2*n + 1]

						english_name.hidden = True

						chinese_name = school_chinese_name[n]

						chinese_name.hidden = True

						query_verification = "SELECT * FROM schools WHERE english_name LIKE %s OR chinese_name LIKE %s"

						try: 

							cr.execute(query_verification, ( str(english_name) + '%', str(chinese_name).encode('utf-8') + '%',))

						except Exception, e:

							print 'We could not fetch the school required, here are thre reasons: ', e

						school = cr.fetchone()

						if school is None:

							print 'This school does not exist in our DB for this country: ', initial_location[country]

							counter_out += 1

						else:

							print 'Youhhhouuuuuu this school is in our database for this country: ', initial_location[country]

							counter_in += 1

							query_update = "UPDATE schools SET (link) = (%s) WHERE school_id = %s"

							print int(school[0]), '       ', link

							try:

								cr.execute(query_update , (link.encode("utf-8").strip(), int(school[0]),))

								conn.commit()
									
							except Exception, e:

									print " We could not update your schools link, here are the reasons:" , e
		
		conn.close()
		print '######## We have ',counter_in, ' schools in common with 51 offer'
		print '######## We have ',counter_out, ' schools not in common with 51 offer'


	def get_the_majors(self):

		conn = self.connect()	

		self.create_many_to_many_table()

		cr = conn.cursor()

		query_link = "SELECT english_name, chinese_name, link, school_id FROM schools WHERE link LIKE '%/school/%' ORDER BY school_id ASC"

		try:

			cr.execute(query_link)

		except Exception, e:

			print "your request failled sorry, here the reason: ", e


		links = cr.fetchall()

		nbr_major = 0

		if links is not None:

			for link in links:

				link_array = link[2].split('/')

				new_link = '/' + link_array[1] + '/specialty_' + link_array[2]

				page_detail = str(root_url) + str(new_link)

				print page_detail

				response = requests.get(page_detail)

				soup = bs4.BeautifulSoup(response.text, "lxml")

				majors = soup.select('ul.list-ul.list-specialty li')

				if majors:

					for page_nbr in range(40):

						new_page_detail = page_detail + '?pageNo=' + str(page_nbr)

						response = requests.post(new_page_detail)

						print 'We we go'

						soup = bs4.BeautifulSoup(response.text, "lxml")

						majors = soup.select('ul.list-ul.list-specialty li')

						if majors:

							num_major = 0

							for major in majors:

								major_name = soup.select('ul.list-ul.list-specialty li h6')[num_major]

								major_name.hidden = True

								major_name = str(major_name).replace('）','').split('（')

								major_english_name = major_name[0]	

								major_chinese_name = major_name[1]

								degree_type = soup.select('div.layout.specialty-type span')[2 * num_major]

								degree_type.hidden = True

								degree_type = str(degree_type).split('：')[1].strip()

								num_major += 1

								print "YOP HERE IS THE MAJOR AND HERE IS THE ENGLISH NAME: ", str(major_english_name).strip(), " HERE IS THE CHINESE NAME: ", str(major_chinese_name).strip(), " HERE IS THE DEGREE TYPE: ", str(degree_type).strip()

								# query_verification = "SELECT COUNT(*) FROM majors_many2many WHERE major_english_name LIKE %s OR major_chinese_name LIKE %s"

								# query_creation = "INSERT INTO majors_51_offers (english_name, degree_type) VALUES (%s, %s)"

								# query_update = "UPDATE majors_51_offers SET (english_name, degree_type) = (%s, %s) WHERE english_name = %s"

								# try:

								# 	cr.execute(query_verification, (major_name.encode("utf-8").strip(),))

								# 	test = cr.fetchall()[0][0]

								# 	if int(test) == 0:

								# 		try:

								# 			cr.execute(query_creation , (major_name.encode("utf-8").strip(), degree_type.encode("utf-8").strip(), ))
										
								# 		except Exception, e:

								# 			print ("############ HERE THE error:", e)

								# 			err += 1

								# 	else:

								# 		print "Exists"

								# 		try:

								# 			cr.execute(query_update , (major_name.encode("utf-8").strip(), degree_type.encode("utf-8").strip(), major_name.encode("utf-8").strip(),))
											
								# 			print "Is Being updated"

								# 		except Exception, e:

								# 			print ("############ HERE THE error:", e)

								# 			err += 1

								# except Exception, e:

								# 	print "your request failled sorry, here the reason: ", e

								# 	err += 1

								# conn.commit()

								# print new_page_detail
			conn.close()

			print nbr_major



New_object = scrap_51offer()
print "############### HERE THE TEST MAIN PAGES"
New_object.thank_you_for_your_school_51offer()
# print "############### HERE THE TEST ON A SCHOOL PAGE"
New_object.get_the_majors()
# print "############### HERE THE TEST ON A MAJOR PAGE"
# New_object.get_the_majors()


