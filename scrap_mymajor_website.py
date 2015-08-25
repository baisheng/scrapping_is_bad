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

	pg_table = 'degree_type'

	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		#cr = conn.cursor()
		return conn 

	def create_major_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE major (major_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, major_category_id integer, link varchar, major_profile text, FOREIGN KEY (major_category_id) REFERENCES major_category(major_category_id))"  

		# query_create_table_index = "CREATE UNIQUE INDEX major_id ON degree_type (major_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'major' and relkind='r')"

		try:

			cr.execute(query_test)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table)

				print 'You created the major table'

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()
		conn.close()		

	def create_many_to_many_table(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE many2manytable (major_id integer NOT NULL, school_id integer NOT NULL, degree_type_id integer NOT NULL, FOREIGN KEY (major_id) REFERENCES major (major_id), FOREIGN KEY (school_id) REFERENCES schools (school_id), FOREIGN KEY (degree_type_id) REFERENCES degree_type (degree_type_id))"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'many2manytable' and relkind='r')"

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

	def create_major_category(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE major_category (major_category_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, major_category_profile text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'degree_type' and relkind='r')"

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

		query_create_table = "CREATE TABLE degree_type (degree_type_id serial PRIMARY KEY, english_name varchar NOT NULL, chinese_name varchar, duration varchar, introduction text)"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'degree_type' and relkind='r')"

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

		# self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		for i in range(800):

			page = str(root_url) + "/AJAX_College_Search_Results?pagenumber=" + str(i)

			response = requests.get(page)

			soup = bs4.BeautifulSoup(response.text, "lxml")

			schools = soup.select('div.row')

			for schools in range(len(schools)):

				link = soup.select('div.row .col-md-2.text-center a')[schools].get('href',None)

				link = link.replace('../','')

				english_name = soup.select('div.row .col-md-7 h3 a')[schools]

				english_name.hidden = True

				print '####### link: ', link, '####### name: ', english_name,

				query_verification = "SELECT * FROM schools WHERE english_name LIKE %s"

				query_update = "UPDATE schools SET (link) = (%s) WHERE school_id = %s"

				try:

					cr.execute(query_verification , (str(english_name).encode("utf-8").strip() + '%',))

				except Exception, e:

					print "your request failled sorry, here the reason: ", e

				get_the_schools = cr.fetchone()

				if get_the_schools is None:

					print '------------------------------------------- this school doesnt exist'

				else:

					print "*************************************************************    School Exists"

					try:

						cr.execute(query_update , (link.encode("utf-8").strip(), int(get_the_schools[0]),))

						print "School Is Being updated"

					except Exception, e:

						print "############ We cannot update your school, here are the reasons:", e

			conn.commit()

		conn.close()	


	def get_the_school_detail_page(self):

		conn = self.connect()	

		self.create_many_to_many_table()

		cr = conn.cursor()

		query_link = "SELECT english_name, link, school_id FROM schools WHERE country = 'us' AND link LIKE %s ORDER BY school_id ASC"

		err = 0

		try:

			cr.execute(query_link, ('college/' + '%',))

		except Exception, e:

			print "your request failled sorry, here the reason: ", e

			err += 1

		if err == 0:

			links = cr.fetchall()

			for idx, val in enumerate(links):

			#	if val[2] > 6327:

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

												query_fetch_major = "SELECT major_id FROM major WHERE english_name = %s"

												try:

													cr.execute(query_fetch_major, (str(major_found), ))

													this_major_id = cr.fetchone()

												except Exception, e:

													print 'We cannot fetch this major, here is the reason: ', e

												if this_major_id is not None:

													query_relation_exit = "SELECT * FROM many2manytable WHERE major_id= %s AND school_id = %s AND degree_type_id = %s"

													query_insert_relation = "INSERT INTO many2manytable (major_id, school_id, degree_type_id) VALUES (%s, %s, %s)"

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

												query_fetch_major = "SELECT major_id FROM major WHERE english_name = %s"

												try:

													cr.execute(query_fetch_major, (str(major_found), ))

													this_major_id = cr.fetchone()

												except Exception, e:

													print 'We cannot fetch this major, here is the reason: ', e

												if this_major_id is not None:

													query_relation_exit = "SELECT * FROM many2manytable WHERE major_id= %s AND school_id = %s AND degree_type_id = %s"

													query_insert_relation = "INSERT INTO many2manytable (major_id, school_id, degree_type_id) VALUES (%s, %s, %s)"

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

					else:

						print 'broken link'

		else:

				print 'broken link'

		conn.close()


	def get_the_majors(self):

		self.create_major_category()

		self.create_major_table()

		conn = self.connect()	

		cr = conn.cursor()

		nbr_major = 0

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

				query_verification = "SELECT COUNT(*) FROM major WHERE english_name = %s"

				query_creation = "INSERT INTO major (english_name, link ) VALUES (%s, %s)"

				query_update = "UPDATE major SET (english_name, link ) = (%s, %s) WHERE english_name = %s"

				try:

					cr.execute(query_verification, (major.encode("utf-8").strip(),))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (major.encode("utf-8").strip(), link.encode("utf-8").strip(),))

						except Exception, e:

							print ("############ HERE THE error:", e)

					else:

						print "Major Exists"

						try:

							cr.execute(query_update , (major.encode("utf-8").strip(), link.encode("utf-8").strip(), major.encode("utf-8").strip(),))

							print "Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)


				except Exception, e:

					print "your request failled sorry, here the reason: ", e


			conn.commit()

			conn.close()

			print nbr_major


	def get_major_details(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_majors = "SELECT major_id, link FROM major ORDER BY major_id"

		try:

			cr.execute(query_majors)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		nbr_major = 0

		for major in cr.fetchall():

			nbr_major += 1

			if nbr_major > 5 and major[0] > 1128:

				new_link = str(root_url) + str(major[1])

				response = requests.get(new_link)

				soup = bs4.BeautifulSoup(response.text, "lxml")

				major_description = soup.select('p.lead')

				major_link = major[1]

				name_major = major_link.replace('/college-majors/','')

				name_link = major_link.replace('college-majors','colleges')

				name_link = name_link.replace(name_major,name_major + '-Major/')

				query_verification = "SELECT * FROM major WHERE link = %s"

				query_update = "UPDATE major SET major_profile=%s WHERE link = %s"

				try:

					cr.execute(query_verification, (major_link,))


				except Exception, e:

					print "your request failled sorry, here are the reason: ", e


				if cr.fetchone() is None:

					print "This major doesn't exist"

				else:

					print "Major Exists"

					try:

						cr.execute(query_update , (major_description[0].encode("utf-8").strip(), major_link,))

						print "Is Being updated"

					except Exception, e:

						print "We cannot update the major,  here are the reasons:", e

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

				query_test = "SELECT * FROM degree_type WHERE english_name = %s"

				try:

					cr.execute(query_test, (str(val), ))

				except Exception, e:

					print "Your request for selection did fail, here is the reason: ", e

				test = cr.fetchone()

				if test is None:

					query_insertion = "INSERT INTO degree_type (english_name) VALUES (%s)"

					try:

						cr.execute(query_insertion, (str(val), ))

					except Exception, e:

						print "Your insertion did fail, here is the reason: ", e

				else:

					print 'Degree type Exists'

		conn.commit()

		conn.close()	

	def get_the_majors_categories(self):

		conn = self.connect()	

		cr = conn.cursor()

		new_link = str(root_url) + '/college-majors'

		response = requests.get(new_link)

		soup = bs4.BeautifulSoup(response.text, "lxml")

		major_categorys = [0, 1, 14, 23, 26, 27, 42, 64, 70, 74, 85, 91, 106, 123, 162, 166, 174, 191, 225, 227, 231, 236, 240, 242, 245, 249, 255, 259, 287, 293, 298, 302, 305, 313, 318, 321, 335, 342, 346]

		for major_category in major_categorys:

			query_major_category = "SELECT * FROM major_category WHERE english_name = %s"

			insert_major_category = "INSERT INTO major_category (english_name) VALUES (%s)"

			this_major = soup.select('li.expanded.top')[major_category]

			this_major_category = this_major.select('a')[0]

			this_major_category.hidden = True

			try:

				cr.execute(query_major_category, (str(this_major_category),))

			except Exception, e:

				print 'We could not fetch this category, here are the reasons: ', e

			if cr.fetchone() is None:

				try:

					cr.execute(insert_major_category, (str(this_major_category),))

					print 'You successfully created a new major category'

					conn.commit()

				except Exception, e:

					print 'We could not create this category, here are the reasons: ', e

			else:

				print 'This major category already exists'

			print this_major_category

			majors = this_major.select('li.leaf a')

			for major in majors:

				major.hidden = True

				query_major_category = "SELECT major_category_id FROM major_category WHERE english_name = %s"

				query_major = "SELECT major_id FROM major WHERE english_name = %s"

				update_major = "UPDATE major SET (major_category_id) = (%s) WHERE major_id = %s"

				try:

					cr.execute(query_major, ( str(major),))

				except Exception, e:

					print 'We could not find your major, here are the reasons: ', e

				major_id = cr.fetchone()

				try:

					cr.execute(query_major_category, ( str(this_major_category),))

				except Exception, e:

					print 'We could not find your major category, here are the reasons: ', e

				major_category_id = cr.fetchone()

				if major_id is not None:

					major_id = major_id[0]

					major_category_id = major_category_id[0]

					print major_id

					try:

						cr.execute(update_major, (major_category_id, major_id))

						print 'you successfully updated you major'

						conn.commit()

					except Exception, e:

						print 'We could not update your major, here are the reasons: ',e

				else:

					print 'This major does not exist'

				print major

			print len(this_major.select('li.expanded.top'))

		conn.commit()

		conn.close()


# got = scrap_mymajor_website()

# got.get_the_majors_categories()


