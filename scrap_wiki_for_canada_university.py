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

class scrap_wiki_for_canada_university(HTMLParser):

	global pg_table

	global root_url

	root_url = 'https://en.wikipedia.org'

	pg_table = 'wiki_school'

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

		query_create_table = "CREATE TABLE wiki_school (school_id serial, english_name varchar(150) NOT NULL, chinese_name varchar(150), logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, introduction text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar, address text)"  

		#query_create_table_index = "CREATE UNIQUE INDEX school_id ON wiki_school (school_id);"  

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = 'wiki_school' and relkind='r')"

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

	def thank_you_for_your_schools_wiki(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		school_detail = str(root_url) + '/wiki/List_of_universities_in_Canada'

		response_school = requests.get(school_detail)

		soup = bs4.BeautifulSoup(response_school.text, "lxml")

		schools = soup.select('table.wikitable tr')

		for school in schools:

			schools = school.select('td a')

			if len(schools) > 0:

				link = schools[0].get('href',None)

				school_name = schools[0]

				school_name.hidden = True

				city = schools[1]

				city.hidden = True

				province = schools[2]

				province.hidden = True

				country = 'CA'

				query_verification = "SELECT COUNT(*) FROM wiki_school WHERE english_name = %s"

				query_creation = "INSERT INTO wiki_school (english_name, city, location, country, link ) VALUES (%s, %s, %s, %s, %s)"

				query_update = "UPDATE wiki_school SET (english_name, city, location, country, link ) = (%s, %s, %s, %s, %s) WHERE english_name = %s"

				try:

					cr.execute(query_verification, (school_name.encode("utf-8").strip(),))

					test = cr.fetchall()[0][0]

					if int(test) == 0:

						try:

							cr.execute(query_creation , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), province.encode("utf-8").strip(), country.encode("utf-8").strip(), link.encode("utf-8").strip(),))

							print "You create one new university"

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

					else:

						print "Exists"

						try:

							cr.execute(query_update , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), province.encode("utf-8").strip(), country.encode("utf-8").strip(), link.encode("utf-8").strip(), school_name.encode("utf-8").strip(),))

							print "Is Being updated"

						except Exception, e:

							print ("############ HERE THE error:", e)

							err += 1

				except Exception, e:

					print "your request failled sorry, here the reason: ", e

					err += 1

				#print '#################### Name of the university: ' ,school_name, '#################### link of the university: ' , link, '#################### city of the university: ' , city, '#################### province of the university: ' , province


		conn.commit()
		conn.close()	


	def get_the_school_detail_page(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_school = "SELECT english_name, link FROM wiki_school"

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e

		i = 0

		for school in cr.fetchall():

			school_detail = str(root_url) + school[1]

			print school_detail

			response_school = requests.get(school_detail)

			soup = bs4.BeautifulSoup(response_school.text, "lxml")

			if soup.select('table.infobox.vcard .image img'):

				logo = soup.select('table.infobox.vcard .image img')[0].get('src', None)

			else:

				logo = 'Not communicated'

			query_update = "UPDATE wiki_school SET (logo) = (%s) WHERE english_name = %s"

			try:

				cr.execute(query_update , (logo.encode("utf-8").strip(), school[0].encode("utf-8").strip(),))

				print "Is Being updated"

			except Exception, e:

				print ("############ HERE THE error:", e)

				err += 1


		conn.commit()
		conn.close()	


New_object = scrap_wiki_for_canada_university()

New_object.thank_you_for_your_schools_wiki()

New_object.get_the_school_detail_page()


