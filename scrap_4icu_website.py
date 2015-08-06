import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np
from generate_xml import generate_xml

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

class scrap_4icu_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://www.4icu.org'

	pg_table = 'icu_school'

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

		query_create_table = "CREATE TABLE icu_school (school_id serial PRIMARY KEY, english_name varchar(150) NOT NULL, chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text, acronym varchar)"  

		query_test = "select exists(select relname from pg_class where relname = 'icu_school' and relkind='r')"

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

		query_create_table = "CREATE TABLE icu_major (major_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, link varchar, introduction text, major_category_id integer, FOREIGN KEY (major_category_id) REFERENCES icu_major_cateogry (major_category_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'icu_major' and relkind='r')"

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

		query_create_table = "CREATE TABLE icu_degree_type (degree_type_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'icu_degree_type' and relkind='r')"

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

		query_create_table = "CREATE TABLE icu_many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id int NOT NULL, FOREIGN KEY (major_id) REFERENCES icu_major (major_id), FOREIGN KEY (school_id) REFERENCES icu_school (school_id), FOREIGN KEY (degree_type_id) REFERENCES icu_degree_type (degree_type_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'icu_many2manytable' and relkind='r')"

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

		query_create_table = "CREATE TABLE icu_major_cateogry (major_category_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'icu_major_cateogry' and relkind='r')"

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

	def thank_you_for_your_schools_4icu(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		countries = ['/au','/ca','/gb', '/us']

		for country in countries:

			school_detail = str(root_url) + str(country)

			response_school = requests.get(school_detail)

			soup = bs4.BeautifulSoup(response_school.text, "lxml")

			if country == '/us':

				regions = soup.select('div.col.span_1_of_2 ul li a')

				for region in regions:

					school_detail = region.get('href', None)

					response_school = requests.get(school_detail)

					soup = bs4.BeautifulSoup(response_school.text, "lxml")

					schools = soup.select('div.section.group tr td.i a')

					for idx, val in enumerate(schools):

						link = val.get('href', None)

						school_name = val

						school_name.hidden = True

						city = soup.select('div.section.group tr td.i h5')[idx]

						city.hidden = True

						country = country.replace('/','')

						#print '################### link of the school: ', link, '################### name of the school: ', school_name , '################### city of the school: ', city


						query_verification = "SELECT COUNT(*) FROM icu_school WHERE english_name = %s"

						query_creation = "INSERT INTO icu_school (english_name, city, country, link ) VALUES (%s, %s, %s, %s)"

						query_update = "UPDATE icu_school SET (english_name, city, country, link ) = (%s, %s, %s, %s) WHERE english_name = %s"

						try:

							cr.execute(query_verification, (school_name.encode("utf-8").strip(),))

							test = cr.fetchall()[0][0]

							if int(test) == 0:

								try:

									cr.execute(query_creation , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), country.encode("utf-8").strip(), link.encode("utf-8").strip(),))

									print "You create one new university"

								except Exception, e:

									print ("############ HERE THE error:", e)

									err += 1

							else:

								print "Exists"

								try:

									cr.execute(query_update , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(),country.encode("utf-8").strip(), link.encode("utf-8").strip(), school_name.encode("utf-8").strip(),))

									print "Is Being updated"

								except Exception, e:

									print ("############ HERE THE error:", e)

									err += 1

						except Exception, e:

							print "your request failled sorry, here the reason: ", e

							err += 1

			else:

				schools = soup.select('div.section.group tr td.i a')

				for idx, val in enumerate(schools):

					link = val.get('href', None)

					school_name = val

					school_name.hidden = True

					city = soup.select('div.section.group tr td.i h5')[idx]

					city.hidden = True

					country = country.replace('/','')

					#print '################### link of the school: ', link, '################### name of the school: ', school_name , '################### city of the school: ', city

					query_verification = "SELECT COUNT(*) FROM icu_school WHERE english_name = %s"

					query_creation = "INSERT INTO icu_school (english_name, city, country, link ) VALUES (%s, %s, %s, %s)"

					query_update = "UPDATE icu_school SET (english_name, city, country, link ) = (%s, %s, %s, %s) WHERE english_name = %s"

					try:

						cr.execute(query_verification, (school_name.encode("utf-8").strip(),))

						test = cr.fetchall()[0][0]

						if int(test) == 0:

							try:

								cr.execute(query_creation , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), country.encode("utf-8").strip(), link.encode("utf-8").strip(),))

								print "You create one new university"

							except Exception, e:

								print ("############ HERE THE error:", e)

								err += 1

						else:

							print "Exists"

							try:

								cr.execute(query_update , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(),country.encode("utf-8").strip(), link.encode("utf-8").strip(), school_name.encode("utf-8").strip(),))

								print "Is Being updated"

							except Exception, e:

								print ("############ HERE THE error:", e)

								err += 1

					except Exception, e:

						print "your request failled sorry, here the reason: ", e

						err += 1

			conn.commit()

		conn.close()	

	def get_the_school_detail_page(self):

		self.create_many_to_many_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_school = "SELECT english_name, link, school_id FROM icu_school ORDER BY school_id" 

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e


		for idx,value in enumerate(cr.fetchall()):

			if value[2] > 858:

				# First we fetch all the require information on the page of 4icu website details page of the school

				school_detail = str(root_url) + value[1]

				response_school = requests.get(school_detail)

				print school_detail

				soup = bs4.BeautifulSoup(response_school.text, "lxml")

				table = soup.select('div.maincontent div.section.group div.col.span_1_of_2 table')

				table2 = soup.select('div.maincontent div.section.group div.col.span_2_of_2 table')[4]

				rank_school = soup.select('div.maincontent div.section.group div.col.span_2_of_2 table iframe')[0].get('src', None)

				general_info = table[0].select("h5")

				school_name = general_info[0].select('b')[0]

				school_name.hidden = True

				if general_info[1].select('acronym'):

					acronym = general_info[1].select('acronym')[0]

					acronym.hidden = True

				else:

					acronym = 'Not communicated'

				street = table[1].select("h5")[0]

				street.hidden = True

				city = table[1].select("h5")[1]

				city.hidden = True

				zipcode = table[1].select("h5")[2]

				zipcode.hidden = True

				address = str(street) + str(city) + ', ' + str(zipcode)

				tuition_fees = table2.select('td h6')[3]

				tuition_fees.hidden = True

				majors_applied = soup.select('div.maincontent div.section.group div.col.span_2_of_2 table')[3]

				for i in range(0 , 6):

					for idx, val in enumerate(majors_applied.select('tr')[ i + 5].select('td')):

						if idx > 0:

							if val.select('img')[0].get('src', None) == '/i/1b.gif':

								#print val.select('img')[0].get('src', None), idx, i + 1

								query_majors = "SELECT major_id FROM icu_major WHERE major_category_id = %s"

								try:

									cr.execute(query_majors, (i + 1, ))

								except Exception, e:

									print 'Ouppppppssss the query did failled... Here the reason: ', e

								for major in cr.fetchall():

									query_test_exist = "SELECT * FROM icu_many2manytable WHERE major_id = %s AND school_id = %s AND degree_type_id= %s"

									query_create_relation = "INSERT INTO icu_many2manytable (major_id, school_id, degree_type_id) VALUES (%s, %s, %s)"

									try:

										cr.execute(query_test_exist, (major[0], value[2], idx))

										if cr.fetchone() is None:

											try:

												cr.execute(query_create_relation, (major[0], value[2], idx))

												print 'You did create a new relation'

											except Exception, e:

												print 'Ouppppppssss the insertion did fail... Here the reason: ', e

										else:

											print 'This relation already exists'

									except Exception, e:

										print 'Ouppppppssss the query did fail... Here the reason: ', e

				# Then we go to fetch missing information on the wikipedia page of the school

				wiki_page = soup.select('div.maincontent div.section.group div.col.span_2_of_2')[21]

				size_table = len(wiki_page.select('a'))

				if wiki_page is not None and size_table > 0:

					wiki_page = wiki_page.select('a')[size_table - 1].get('href', None)

					print wiki_page

					if wiki_page is not None and wiki_page != '' and "wikipedia" in str(wiki_page):

						response_school_wiki = requests.get(wiki_page)

						if response_school_wiki:

							soup = bs4.BeautifulSoup(response_school_wiki.text, "lxml")

							if soup.select('table.infobox.vcard .image img'):

								logo = soup.select('table.infobox.vcard .image img')[0].get('src', None)

							else:

								logo = 'Not communicated'

							if soup.select('#mw-content-text p') and len(soup.select('#mw-content-text p')) > 1:

								content = str(soup.select('#mw-content-text p')[0]) + str(soup.select('#mw-content-text p')[1])

							else:

								content = 'Not communicated'

						# Then we go to fetch the rank

				school_rank = str(root_url) + rank_school

				response_school_rank = requests.get(school_rank)

				if response_school_rank:

					soup = bs4.BeautifulSoup(response_school_rank.text, "lxml")

					if soup.select('td'):

						rank = soup.select('td')[1]

						rank.hidden = True

					else:

						rank = 'Not communicated'

				query_update = "UPDATE icu_school SET (logo, acronym, introduction, address, tuition_fees) = (%s, %s, %s, %s, %s) WHERE link = %s"

				try:

					test = cr.execute(query_update , (str(logo), str(acronym), str(content), str(address), str(tuition_fees), value[1].encode("utf-8").strip(),))

					print "Information are Being updated with logo: ", logo, '  acronym: ', acronym, '  content: ', content,'  address: ', address,'  tuition_fees: ', tuition_fees

					conn.commit()

				except Exception, e:

					print ("############ Cannot update the table here is the error:", e)

			conn.commit()

		conn.close()	

	def get_the_majors(self):

		self.create_degree_type_table()

		self.create_major_category_table()

		self.create_major_table()

		conn = self.connect()	

		cr = conn.cursor()


		major_page = str(root_url) + '/reviews/1.htm'

		response = requests.get(major_page)

		soup = bs4.BeautifulSoup(response.text, "lxml")

		majors = soup.select('div.section.group div.col.span_2_of_2')[8]

		majors = majors.select('tr td span')

		majors_category = soup.select('div.section.group div.col.span_2_of_2')[8].select('tr td a')

		#we first crete the degree type

		degree_types = soup.select('div.section.group div.col.span_2_of_2')[8].select('tr td h6 a')

		for idx, val in enumerate(degree_types):

			if idx < 5:

				query_verification = "SELECT COUNT(*) FROM icu_degree_type WHERE english_name = %s"

				query_creation = "INSERT INTO icu_degree_type (english_name ,duration ) VALUES (%s, %s)"

				query_update = "UPDATE icu_degree_type SET (english_name, duration) = (%s, %s) WHERE english_name = %s"	

				val.hidden = True	

				new_value = str(val).replace('<span>','').replace('</span>','').replace('\n','').replace('\t','').replace('\r','').replace('<br/>',' ').replace('(','split').replace(')','')	

				new_val = new_value.split('split')

				try:

					cr.execute(query_verification, (new_val[0],))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (new_val[0].encode("utf-8").strip(), new_val[1]))

						except Exception, e:

							print ("############ HERE THE error:", e)

					else:

						print "Exists"

						try:

							cr.execute(query_update , (new_val[0].encode("utf-8").strip(), new_val[1],new_val[0].encode("utf-8").strip(),))

							print "Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)

				except Exception, e:

					print "your request failled sorry, here the reason: ", e

				# For test purpose

				# print idx, new_val

		# Then we create majors category first

		query_insert_category = "INSERT INTO icu_major_cateogry (english_name) VALUES (%s)"

		query_test_existing_category = "SELECT * FROM icu_major_cateogry WHERE english_name = %s"

		for idx, val in enumerate(majors_category):

			if idx > 4:

				val.hidden= True

				category = str(val).split('<span>')[0].strip()

				try:

					cr.execute(query_test_existing_category, (category.encode("utf-8").strip(),))

					test = cr.fetchone()

					if test is None:

						try:

							cr.execute(query_insert_category , (category.encode("utf-8").strip(),))

							print '###################### MAJOR CATEGORIES: ', category

						except Exception, e:

							print ("############ INSERTION FAILED HERE THE error:", e)

					else:

						print "Exists"


				except Exception, e:

					print "your request failled sorry, here the reason: ", e

				conn.commit()

		for index, val in enumerate(majors):

			if index > 4:

				new_val = str(val).split('<br/>\r\n')

				for idx, val in enumerate(new_val):

					new_value = val.replace('<span>','').replace('</span>','')

					query_verification = "SELECT * FROM icu_major WHERE english_name = %s"

					query_creation = "INSERT INTO icu_major (english_name , major_category_id) VALUES (%s, %s)"

					query_update = "UPDATE icu_major SET (english_name, major_category_id) = (%s,%s) WHERE english_name = %s"

					try:

						cr.execute(query_verification, (new_value,))

						test = cr.fetchone()

						if test is None:

							try:

								cr.execute(query_creation , (new_value.encode("utf-8").strip(), index - 4, ))

							except Exception, e:

								print ("############ HERE THE error:", e)

						else:

							print "Exists"

							try:

								cr.execute(query_update , (new_value.encode("utf-8").strip(), index - 4, new_value,))

								print "Is Being updated"

							except Exception, e:

								print ("############ HERE THE error:", e)


					except Exception, e:

						print "your request failled sorry, here the reason: ", e

		conn.commit()

			#conn.close()


New_object = scrap_4icu_website()

# New_object.thank_you_for_your_schools_4icu()

# New_object.get_the_majors()

New_object.get_the_school_detail_page()

Another_new_object = generate_xml()

Another_new_object.generate_xls_file()


