 #coding=utf-8 

import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser
import numpy as np
from generate_xml import generate_xml
from get_university_ranking import get_university_ranking
from scrap_mymajor_website import scrap_mymajor_website

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")

class scrap_4icu_website(HTMLParser):

	global pg_table

	global root_url

	root_url = 'http://www.4icu.org'

	pg_table = 'schools'

	def __connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	def create_school_table(self):

		conn = self.__connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE schools (school_id serial PRIMARY KEY, english_name varchar(150), chinese_name varchar(150), logo varchar(250), country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar,  school_profile_english text, school_profile_chinese text, link varchar,url varchar, address text, acronym varchar, chinese_link varchar, school_profile_origin varchar DEFAULT 'wikipedia', crm_gz_id Integer, crm_be_id integer)"  

		query_test = "select exists(select relname from pg_class where relname = 'schools' and relkind='r')"

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

	def thank_you_for_your_schools_4icu(self):

		self.create_school_table()

		conn = self.__connect()	

		cr = conn.cursor()

		# countries = ['/au','/ca','/gb', '/us','/fr']

		countries = ['/cn','/be','/at','/fi','/hk','/it','/jp','/de','/ie','/kr','/my','/nz','/nl','/pl','/no','/se','/ch','/es','/sg','/mu']

		for country in countries:

			school_detail = str(root_url) + str(country)

			response_school = requests.get(school_detail)

			soup = bs4.BeautifulSoup(response_school.text, "lxml")

			print school_detail

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

						country = country.replace('/','').upper()

						query_country = "SELECT country_id FROM countries WHERE iso2 = %s"

						try:

							cr.execute(query_country, (str(country),))

						except Exception, e:

							print 'The country selection failed, here are the reasons: ', e

						country_id = cr.fetchone()

						#print '################### link of the school: ', link, '################### name of the school: ', school_name , '################### city of the school: ', city


						query_verification = "SELECT COUNT(*) FROM schools WHERE english_name = %s"

						query_creation = "INSERT INTO schools (english_name, city, country_id, link ) VALUES (%s, %s, %s, %s)"

						query_update = "UPDATE schools SET (english_name, city, country_id, link ) = (%s, %s, %s, %s) WHERE english_name = %s"

						try:

							cr.execute(query_verification, (school_name.encode("utf-8").strip(),))

							test = cr.fetchall()[0][0]

							if int(test) == 0:

								try:

									cr.execute(query_creation , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), country_id[0], link.encode("utf-8").strip(),))

									print "You create one new university"

								except Exception, e:

									print "############ We cannot create a new university, here are the reasons:", e

							else:

								print "Exists"

								try:

									cr.execute(query_update , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(),country_id[0], link.encode("utf-8").strip(), school_name.encode("utf-8").strip(),))

									print "Is Being updated"

								except Exception, e:

									print "############ We cannot update an existing university, here are the reasons:", e

						except Exception, e:

							print "your request failled sorry, here the reason: ", e

			else:

				schools = soup.select('div.section.group tr td.i a')

				for idx, val in enumerate(schools):

					link = val.get('href', None)

					school_name = val

					school_name.hidden = True

					city = soup.select('div.section.group tr td.i h5')[idx]

					city.hidden = True

					country = country.replace('/','').upper()

					query_country = "SELECT country_id FROM countries WHERE iso2 = %s"

					try:

						cr.execute(query_country, (str(country),))

					except Exception, e:

						print 'The country selection failed, here are the reasons: ', e

					country_id = cr.fetchone()

					if country_id is not None:

						print country_id[0]

					#print '################### link of the school: ', link, '################### name of the school: ', school_name , '################### city of the school: ', city

					query_verification = "SELECT COUNT(*) FROM schools WHERE english_name = %s"

					query_creation = "INSERT INTO schools (english_name, city, country_id, link ) VALUES (%s, %s, %s, %s)"

					query_update = "UPDATE schools SET (english_name, city, country_id, link ) = (%s, %s, %s, %s) WHERE english_name = %s"

					try:

						cr.execute(query_verification, (school_name.encode("utf-8").strip(),))

						test = cr.fetchall()[0][0]

						if int(test) == 0:

							try:

								cr.execute(query_creation , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(), country_id[0], link.encode("utf-8").strip(),))

								print "You create one new university"

							except Exception, e:

								print "############ We cannot create a new university, here are the reasons:", e

						else:

							print "Exists"

							try:

								cr.execute(query_update , (school_name.encode("utf-8").strip(), city.encode("utf-8").strip(),country_id[0], link.encode("utf-8").strip(), school_name.encode("utf-8").strip(),))

								print "Is Being updated"

							except Exception, e:

								print "############ We cannot update an existing university, here are the reasons:", e


					except Exception, e:

						print "your request failled sorry, here the reason: ", e

			conn.commit()

		conn.close()	

	def get_the_school_detail_page(self):

		#self.create_many_to_many_table()

		conn = self.__connect()	

		cr = conn.cursor()

		query_school = "SELECT english_name, link, school_id, country_id FROM schools WHERE country_id <> 840 AND country_id <> 250 ORDER BY school_id ASC" 

		try:

			cr.execute(query_school)

		except Exception, e:

			print 'Ouppppppssss the query did failled... Here the reason: ', e


		for idx,value in enumerate(cr.fetchall()):

			#if value[2] > 4940:

				# First we fetch all the require information on the page of 4icu website details page of the school

				school_detail = str(root_url) + value[1]

				response_school = requests.get(school_detail)

				print school_detail

				soup = bs4.BeautifulSoup(response_school.text, "lxml")

				table = soup.select('div.maincontent div.section.group div.col.span_1_of_2 table')

				table2 = soup.select('div.maincontent div.section.group div.col.span_2_of_2 table')[4]

				general_info = table[0].select("h5")

				school_name = general_info[0].select('b')[0]

				school_name.hidden = True

				school_url = general_info[0].select('a')[0].get('href', None)

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

				wiki_page = soup.select('div.maincontent div.section.group div.col.span_2_of_2')[21]

				size_table = len(wiki_page.select('a'))

				if wiki_page is not None and size_table > 0 and value[3] != 156:

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

							if soup.select('li.interlanguage-link.interwiki-zh a'):

								chinese_link = soup.select('li.interlanguage-link.interwiki-zh a')[0].get('href',None)

								chinese_link = str(chinese_link).encode("utf-8")

								chinese_name= soup.select('li.interlanguage-link.interwiki-zh a')[0].get('title',None)

								chinese_name = str(chinese_name).encode("utf-8").split(' – ')[0]

								#chinese_link = chinese_link.split(' – ')[0]

								print '################################# here is the chinese name: ', chinese_name

							else:

								chinese_link = ''

								chinese_name = ''

				else:

					chinese_link = ''

					chinese_name = table[0].select("h5 a")[1]

					chinese_name.hidden = True	

					logo = 'Not communicated'	

					content = 'Not communicated'		

				query_update = "UPDATE schools SET (logo, acronym, school_profile_english, address, url, chinese_link, chinese_name) = (%s, %s, %s, %s, %s, %s, %s) WHERE link = %s"

				try:

					test = cr.execute(query_update , (str(logo), str(acronym), str(content), str(address), str(school_url), str(chinese_link).encode("utf-8"), str(chinese_name).encode("utf-8"), value[1].encode("utf-8").strip(),))

					print "You successfully updated the school"

					conn.commit()

				except Exception, e:

					print ("############ Cannot update the table here is the error:", e)

				conn.commit()

		conn.close()	


####### WE GET THE SCHOOLS FIRST

New_object = scrap_4icu_website()

# New_object.thank_you_for_your_schools_4icu()

# New_object.get_the_school_detail_page()

# # New_object.get_the_chinese_info()

# ########WE GO TO FETCH THE RANKS NOW

New_object = get_university_ranking()

New_object.get_qs_ranking()

New_object.get_usnews_national_ranking()

New_object.get_usnews_global_ranking()

# ######## WE GOT TO FETCH THE MAJORS NOW

# New_object = scrap_mymajor_website()

# New_object.get_degree_type()

# New_object.get_the_majors()

# New_object.get_major_details()

# New_object.thank_you_for_your_schools_mymajor()

# New_object.get_the_school_detail_page()


######## NO NEED NOW

# Another_new_object = generate_xml()

# Another_new_object.generate_xls_file()


