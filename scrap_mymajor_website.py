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

class scrap_mymajor_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://www.mymajors.com'

	pg_table = 'mymajor_major'

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

		query_create_table = "CREATE TABLE mymajor_school (school_id serial, english_name varchar(150) NOT NULL, chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text, acronym varchar)"  

		query_create_table_index = "CREATE UNIQUE INDEX school_id ON mymajor_school (school_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'mymajor_school' and relkind='r')"

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

		query_create_table = "CREATE TABLE mymajor_major (major_id serial, english_name varchar NOT NULL, chinese_name varchar, degree_type varchar, link varchar, introduction text)"  

		query_create_table_index = "CREATE UNIQUE INDEX major_id ON mymajor_major (major_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'mymajor_major' and relkind='r')"

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

		query_create_table = "CREATE TABLE mymajor_many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id integer NOT NULL, FOREIGN KEY (major_id) REFERENCES mymajor_major (major_id), FOREIGN KEY (school_id) REFERENCES mymajor_school (school_id), FOREIGN KEY (degree_type_id) REFERENCES mymajor_degree_type (degree_type_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'mymajor_many2manytable' and relkind='r')"

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

	def create_degree_type_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE mymajor_degree_type (degree_type_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'mymajor_degree_type' and relkind='r')"

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

	def thank_you_for_your_schools_mymajor(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		err = 0

		for i in range(800):

			page = str(root_url) + "/AJAX_College_Search_Results?pagenumber=" + str(i)

			response = requests.get(page)

			soup = bs4.BeautifulSoup(response.text, "lxml")

			schools = soup.select('div.row')

			for schools in range(len(schools)):

				logo = soup.select('div.row .col-md-2.text-center a img')[schools].get('src',None)

				logo = logo.replace('../','')

				link = soup.select('div.row .col-md-2.text-center a')[schools].get('href',None)

				link = link.replace('../','')

				english_name = soup.select('div.row .col-md-7 h3 a')[schools]

				english_name.hidden = True

				city = soup.select('div.row .col-md-7 address span')[4 * schools]

				city.hidden = True

				address = soup.select('div.row .col-md-7 span')[4*schools + 2]

				address.hidden = True

				region = soup.select('div.row .col-md-7 span')[4*schools + 1]

				region.hidden = True

				zip_code = soup.select('div.row .col-md-7 span')[4*schools + 3]

				zip_code.hidden = True

				complete_address = str(address) + ', ' + str(region) + ' ' + str(zip_code)

				print '####### logo: ', logo, '####### link: ', link, '####### englishname: ', english_name, '####### city: ', city, '####### address: ', complete_address 

				query_verification = "SELECT COUNT(*) FROM mymajor_school WHERE english_name = %s"

				query_creation = "INSERT INTO mymajor_school (english_name, link ,address, city, logo) VALUES (%s, %s, %s, %s, %s)"

				query_update = "UPDATE mymajor_school SET (english_name, link ,address, city, logo) = (%s, %s, %s, %s, %s) WHERE english_name = %s"

				try:

					cr.execute(query_verification, (english_name.encode("utf-8").strip(),))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (english_name.encode("utf-8").strip(), link.encode("utf-8").strip(), complete_address.encode("utf-8").strip(), city.encode("utf-8").strip(), logo.encode("utf-8").strip(),))

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

					else:

						print "School Exists"

						try:

							cr.execute(query_update , (english_name.encode("utf-8").strip(), link.encode("utf-8").strip(), complete_address.encode("utf-8").strip(), city.encode("utf-8").strip(), logo.encode("utf-8").strip(), english_name.encode("utf-8").strip(),))

							print "School Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

				except Exception, e:

					print "your request failled sorry, here the reason: ", e

					err += 1

			conn.commit()

		conn.close()	


	def get_the_school_detail_page(self):

		conn = self.connect()	

		self.create_many_to_many_table()

		cr = conn.cursor()

		query_link = "SELECT english_name, link, school_id FROM mymajor_school ORDER BY school_id ASC"

		err = 0

		try:

			cr.execute(query_link)

		except Exception, e:

			print "your request failled sorry, here the reason: ", e

			err += 1

		if err == 0:

			links = cr.fetchall()

			for idx, val in enumerate(links):

				if val[2] > 3515:

					new_link = val[1]

					new_link= new_link.replace(".","-")

					school_detail = str(root_url) + '/' + str(new_link)

					print school_detail

					school_detail = school_detail.replace("'","-")

					school_detail = school_detail.replace("--","-")

					school_detail = school_detail.replace("-(the)","")

					major_details =  school_detail + 'majors/'

					print school_detail, major_details

					response_school = requests.get(school_detail)

					if response_school:

						soup = bs4.BeautifulSoup(response_school.text, "lxml")

						if soup.select('td.net-items h2'):

							tuition_fees = soup.select('td.net-items h2')[0]

							tuition_fees.hidden = True

						else:

							tuition_fees = 'Not communicated'

						if soup.select('.post.col-md-8.col-sm-12.col-xs-12 p.lead'):

							introduction_container = soup.select('.post.col-md-8.col-sm-12.col-xs-12 p.lead')

							introduction1 = introduction_container[0]

							introduction1.hidden = True

							introduction2 = introduction_container[1]

							introduction2.hidden = True

							introduction3 = introduction_container[2]

							introduction3.hidden = True

							introduction = str(introduction1.encode("utf-8").strip()) + str(introduction2.encode("utf-8").strip()) + str(introduction3.encode("utf-8").strip())

						else:

							introduction = 'Not communicated'

						query_update = "UPDATE mymajor_school SET (introduction, tuition_fees) = (%s, %s) WHERE school_id = %s"

						try:

							cr.execute(query_update, (str(introduction), tuition_fees.encode("utf-8").strip(), val[2],))

						except Exception, e:

							print 'Ouppppppssss the update did fail... Here the reason: ', e

						response_major = requests.get(major_details)

						if response_major:

							new_soup = bs4.BeautifulSoup(response_major.text, "lxml")

							list_of_possible_css = ['tr.MasRow td', 'tr.AssRow.CerRow td', 'tr.BacRow td', 'tr.BacRow.CerRow td', 'tr.AssRow td', 'tr.CerRow td']

							major_nb = 0

							majors_available = 0

							for css_option in list_of_possible_css:

								majors = new_soup.select(css_option + ' a')

								majors_available 

								print '##############################################################: ', len(majors)

								for idx, value in enumerate(new_soup.select(css_option)):

									if len(majors) > 0:

										if idx < 6:

											if idx == 0:

												major_found = value.select('a')[0]

											value.hidden = True

											major_found.hidden = True

											if value.select('span'):

												major_nb += 1

												query_fetch_major = "SELECT major_id FROM mymajor_major WHERE english_name = %s"

												try:

													cr.execute(query_fetch_major, (str(major_found), ))

													this_major_id = cr.fetchone()

												except Exception, e:

													print 'We cannot fetch this major, here is the reason: ', e

												if this_major_id is not None:

													query_relation_exit = "SELECT * FROM mymajor_many2manytable WHERE major_id= %s AND school_id = %s AND degree_type_id = %s"

													query_insert_relation = "INSERT INTO mymajor_many2manytable (major_id, school_id, degree_type_id) VALUES (%s, %s, %s)"

													try:

														cr.execute(query_relation_exit, (this_major_id[0], val[2], idx, ))

														if cr.fetchone() is None:

															try:

																cr.execute(query_insert_relation, (this_major_id[0], val[2], idx, ))

																print 'You did create a new relation'

															except Exception, e:

																print 'We couldnt execute the insertion, here are the reasons: ', e

														else:

															print 'Relation already exists'

													except Exception, e:

														print 'We couldnt execute the select query, here are the reasons: ', e

														
													print '############################# A NEW RELATION WILL BE CREATED WITH THIS MAJOR : ', major_found, 'and its ID: ', this_major_id[0], '  and the school id:  ', val[2], '  and the degree id   ', idx

													conn.commit()

										if idx >= 6:

											new_figure = idx % 6

											if new_figure == 0:

												major_found = value.select('a')[0]

											value.hidden = True

											major_found.hidden = True

											if value.select('span'):

												major_nb += 1

												query_fetch_major = "SELECT major_id FROM mymajor_major WHERE english_name = %s"

												try:

													cr.execute(query_fetch_major, (str(major_found), ))

													this_major_id = cr.fetchone()

												except Exception, e:

													print 'We cannot fetch this major, here is the reason: ', e

												if this_major_id is not None:

													query_relation_exit = "SELECT * FROM mymajor_many2manytable WHERE major_id= %s AND school_id = %s AND degree_type_id = %s"

													query_insert_relation = "INSERT INTO mymajor_many2manytable (major_id, school_id, degree_type_id) VALUES (%s, %s, %s)"

													try:

														cr.execute(query_relation_exit, (this_major_id[0], val[2], new_figure, ))

														if cr.fetchone() is None:

															try:

																cr.execute(query_insert_relation, (this_major_id[0], val[2], new_figure, ))

																print 'You did create a new relation'

															except Exception, e:

																print 'We couldnt execute the insertion, here are the reasons: ', e

														else:

															print 'Relation already exists'

													except Exception, e:

														print 'We couldnt execute the select query, here are the reasons: ', e

														

													print '############################# A NEW RELATION WILL BE CREATED WITH THIS MAJOR : ', major_found, 'and its ID: ', this_major_id[0], '  and the school id:  ', val[2], '  and the degree id   ', new_figure

													conn.commit()

							#print major_nb

								# for major in majors:

								# 	major.hidden = True

								# 	query_majors = "SELECT major_id FROM mymajor_major WHERE english_name=%s"

								# 	try:

								# 		cr.execute(query_majors, (str(major), ))

								# 	except Exception, e:

								# 		print 'Ouppppppssss the query did failled... Here the reason: ', e

								# 	this_major = cr.fetchone()

								# 	if this_major:

								# 		query_verification = "SELECT COUNT(*) FROM mymajor_many2manytable WHERE major_id = %s AND school_id = %s"

								# 		query_creation = "INSERT INTO mymajor_many2manytable (major_id,  school_id) VALUES (%s, %s)"

								# 		try:

								# 			cr.execute(query_verification, (int(this_major[0]), int(val[2]),))

								# 			test = cr.fetchall()

								# 			test = test[0][0]

								# 			if int(test) == 0:

								# 				try:

								# 					cr.execute(query_creation , (int(this_major[0]), int(val[2]),))

								# 					print 'You have created one many 2 many relation'

								# 				except Exception, e:

								# 					print ("############ the insertion failed, here the error:", e)

								# 					err += 1

								# 			else:

								# 				print "Exists"

								# 		except Exception, e:

								# 			print "your request failled sorry, here the reason: ", e

								# 			err += 1

								# 		

					else:

						print 'broken link'

		else:

				print 'broken link'

		conn.close()


	def get_the_majors(self):

		self.create_major_table()

		conn = self.connect()	

		cr = conn.cursor()

		nbr_major = 0

		err = 0

		new_link = str(root_url) + '/college-majors'

		response = requests.get(new_link)

		soup = bs4.BeautifulSoup(response.text, "lxml")

		majors = soup.select('li.expanded.top li.leaf')

		if majors:

			for i in range(len(majors)):

				major = soup.select('li.expanded.top li.leaf a')[i]

				major.hidden = True

				link = soup.select('li.expanded.top li.leaf a')[i].get('href',None)

				link = link.replace('..','')

				nbr_major += 1

				query_verification = "SELECT COUNT(*) FROM mymajor_major WHERE english_name = %s"

				query_creation = "INSERT INTO mymajor_major (english_name, link ) VALUES (%s, %s)"

				query_update = "UPDATE mymajor_major SET (english_name, link ) = (%s, %s) WHERE english_name = %s"

				try:

					cr.execute(query_verification, (major.encode("utf-8").strip(),))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (major.encode("utf-8").strip(), link.encode("utf-8").strip(),))

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

					else:

						print "Major Exists"

						try:

							cr.execute(query_update , (major.encode("utf-8").strip(), link.encode("utf-8").strip(), major.encode("utf-8").strip(),))

							print "Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

				except Exception, e:

					print "your request failled sorry, here the reason: ", e

					err += 1

			conn.commit()

			conn.close()

			print nbr_major


	def get_major_details(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_majors = "SELECT major_id, link FROM mymajor_major ORDER BY major_id"

		try:

			cr.execute(query_majors)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		for major in cr.fetchall():

			if major[0] > 5:

				new_link = str(root_url) + str(major[1])

				response = requests.get(new_link)

				soup = bs4.BeautifulSoup(response.text, "lxml")

				major_description = soup.select('p.lead')

				major_link = major[1]

				name_major = major_link.replace('/college-majors/','')

				name_link = major_link.replace('college-majors','colleges')

				name_link = name_link.replace(name_major,name_major + '-Major/')

				query_verification = "SELECT COUNT(*) FROM mymajor_major WHERE link = %s"

				query_creation = "INSERT INTO mymajor_major (introduction ) VALUES (%s)"

				query_update = "UPDATE mymajor_major SET introduction=%s WHERE link = %s"

				try:

					cr.execute(query_verification, (major_link,))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (major_description[0].encode("utf-8").strip(),))

						except Exception, e:

							print ("############ HERE THE error:", e)

					else:

						print "Major Exists"

						try:

							cr.execute(query_update , (major_description[0].encode("utf-8").strip(), major_link,))

							print "Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)

				except Exception, e:

					print "your request failled sorry, here the reason: ", e


				page_schools = str(root_url) + str(name_link)

				print '############### page school name:', page_schools

				new_response = requests.get(page_schools)

				new_soup = bs4.BeautifulSoup(new_response.text, 'lxml')

				major_description = soup.select('#')

		conn.commit()

		conn.close()	


	def get_degree_type(self):

		self.create_degree_type_table()

		conn = self.connect()	

		cr = conn.cursor()

		page = str(root_url) + '/college/ca/academy-of-art-university/majors/'

		response = requests.get(page)

		soup = bs4.BeautifulSoup(response.text, "lxml")

		degree_type = soup.select("th.rotate span")

		for idx, val in enumerate(degree_type):

			if idx < 5:

				val.hidden = True

				query_test = "SELECT * FROM mymajor_degree_type WHERE english_name = %s"

				try:

					cr.execute(query_test, (str(val), ))

				except Exception, e:

					print "Your request for selection did fail, here is the reason: ", e

				test = cr.fetchone()

				if test is None:

					query_insertion = "INSERT INTO mymajor_degree_type(english_name) VALUES (%s)"

					try:

						cr.execute(query_insertion, (str(val), ))

					except Exception, e:

						print "Your insertion did fail, here is the reason: ", e

				else:

					print 'Degree type Exists'

		conn.commit()

		conn.close()	







New_object = scrap_mymajor_website()
# New_object.get_the_majors()
# print "############### HERE THE TEST ON A MAJOR DETAIL PAGE"
# New_object.get_degree_type()
# print "############### HERE THE TEST ON A MAJOR DETAIL PAGE"
# New_object.get_major_details()
# print "############### HERE THE TEST ON A MAJOR GLOBAL PAGE"
# New_object.thank_you_for_your_schools_mymajor()
# print "############### HERE THE TEST ON A SCHOOL PAGE"
New_object.get_the_school_detail_page()


