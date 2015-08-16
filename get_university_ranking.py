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

class get_university_ranking(HTMLParser):

	global root_url_topuniversity

	root_url_topuniversity = 'http://www.topuniversities.com'

	global root_url_usnews_local

	root_url_usnews_local = 'http://www.usnews.com/education/best-global-universities/'


	# Let's go with our private method

	def __connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		return conn 

	# Thank you python for not providing case statement functions :(
	def __statment_function(self, arg):

		if arg == '257':

			return 'US'

		if arg == '222':

			return 'CA'

		if arg == '319':

			return 'UK'

		if arg == '208':

			return 'AU'

	def __typeofvalue(self,var):
	    try:
	        int(var)
	        return var
	    except ValueError:
	        return 0

	def create_rank_type(self):

		conn = self.__connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE rank_type (rank_type_id serial PRIMARY KEY, english_name varchar(150), chinese_name varchar(150), description text)"  

		query_test = "select exists(select relname from pg_class where relname = 'rank_type' and relkind='r')"

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

	def create_school_ranking(self):

		conn = self.__connect()	

		cr = conn.cursor()

		query_create_table = "CREATE TABLE school_ranking (school_id int NOT NULL, rank_type_id int NOT NULL, rank integer, url varchar(255), FOREIGN KEY (rank_type_id) REFERENCES rank_type(rank_type_id), FOREIGN KEY (school_id) REFERENCES school(school_id))"  

		query_test = "select exists(select relname from pg_class where relname = 'school_ranking' and relkind='r')"

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

	def get_qs_ranking(self):

		self.create_rank_type()

		self.create_school_ranking()

		conn = self.__connect()	

		cr = conn.cursor()

		countries = ['257','222','319','208']

		query_check_qs = "SELECT * FROM rank_type"

		query_insert_qs = "INSERT INTO rank_type (english_name, description) VALUES ('QS', 'QS World University Rankings is an annual publication of university rankings by British Quacquarelli Symonds (QS) company')"

		try:

			cr.execute(query_check_qs)

		except Exception, e:

			print "Your query selection did fail, here are the reasons: ", e

		if cr.fetchone() is None:

			try:

				cr.execute(query_insert_qs)

				print "You inserted QS into rank type"

			except Exception, e:

				print "Your insertion did fail, here are the reasons: ", e

		conn.commit()

		for country in countries:

			country_schools = root_url_topuniversity + '/universities/country/' + str(country)

			request_page = requests.get(country_schools)

			soup = bs4.BeautifulSoup(request_page.text, "lxml")

			common_major = 0

			schools = soup.select('ul.universities-search-result .profile-result a')

			for index, val in enumerate(schools):

				link = val.get('href',None)

				school_page = root_url_topuniversity + str(link)

				print school_page

				request_school = requests.get(school_page)

				soup = bs4.BeautifulSoup(request_school.text,'lxml')

				if soup.select('div.branded.pane-node-field-profile-branded-name h1'):

					school_name = soup.select('div.branded.pane-node-field-profile-branded-name h1')[0]

					school_name.hidden = True

					school_name = str(school_name).split('<span>')[0]

					school_name = school_name.split('(')[0]

					school_name = str(school_name).strip()

					print "'" + str(school_name).strip() + "'"

					school_is_there = "SELECT * FROM school WHERE english_name = %s"

					try:

						cr.execute(school_is_there , (school_name,))

					except Exception, e:

						print 'We cannot do the select query, here are the reasons: ', e

					school_id = cr.fetchone()

					if school_id is None:

						print '#####################  This school does not exists'

					else:

						QS_ranking = soup.select('div.rank.views-field-field-ranking-rank span')

						tuition_fees= soup.select('table#uni-tuition tr td.tut p')

						# give up with tuition fees at first

						# if tuition_fees:

						# 	tuition_fees = tuition_fees[3]

						# 	tuition_fees.hidden = True

						# 	print tuition_fees

						# else:

						# 	tuition_fees = 'N/A'

						if QS_ranking:

							QS_ranking = QS_ranking[0]

							QS_ranking.hidden = True

							QS_ranking = self.__typeofvalue(str(QS_ranking))

						else:

							QS_ranking = 0

						print "***************************************************  Yep this is commun school and here is the ranking: ", QS_ranking, '*************** and the tuition_fees are: ', tuition_fees, '************************* country: ', self.__statment_function(str(country))

						query_test_exist = "SELECT rank_type_id FROM rank_type WHERE english_name='QS'"

						try:

							cr.execute(query_test_exist)

						except Exception, e:

							print 'We cannot do the select query, here are the reasons: ', e					

						rank_type_id = int(cr.fetchone()[0])

						school_id = int(school_id[0])

						query_exists = "SELECT * FROM school_ranking WHERE school_id = %s AND rank_type_id = %s"

						query_insert = "INSERT INTO school_ranking (school_id, rank_type_id, rank, url) VALUES (%s, %s, %s, %s)"

						query_update = "UPDATE school_ranking SET (rank, url) = (%s, %s) WHERE school_id = %s AND rank_type_id = %s"

						try:

							cr.execute(query_exists , (school_id, rank_type_id,))

						except Exception, e:

							print 'We cannot do the select query, here are the reasons: ', e

						if cr.fetchone() is None:

							try:

								cr.execute(query_insert , (school_id, rank_type_id, QS_ranking, str(link), ))

								print 'You successufully created a new rank for an existing school'

							except Exception, e:

								print 'We cannot insert the school rank, here are the reasons: ', e		
						else:

							try:

								cr.execute(query_update , (QS_ranking, str(link), school_id, rank_type_id, ))

								print 'You successufully updated rank for an existing school'

							except Exception, e:

								print 'We cannot update the school rank, here are the reasons: ', e	

						common_major += 1

						conn.commit()

			print "Here are the common school", common_major

	def get_usnews_national_ranking(self):

		conn = self.__connect()	

		cr = conn.cursor()

		query_check_usnews_local = "SELECT * FROM rank_type WHERE english_name='US NEWS Local'"

		query_insert_usnews_local  = "INSERT INTO rank_type (english_name, description) VALUES ('US NEWS Local', 'These locales universities have been numerically ranked based on their positions in the overall Best Global Universities rankings. Schools were evaluated based on their research performance and their ratings by members of the academic community around the world and within North America. These are the top global universities in their country.')"

		try:

			cr.execute(query_check_usnews_local)

		except Exception, e:

			print "Your query selection did fail, here are the reasons: ", e

		if cr.fetchone() is None:

			try:

				cr.execute(query_insert_usnews_local)

				print "You inserted QS into rank type"

			except Exception, e:

				print "Your insertion did fail, here are the reasons: ", e

		conn.commit()

		countries = ['canada','united-kingdom','united-states','france','australia-new-zealand']

		for country in countries:

			common_major = 0

			if country == 'australia-new-zealand':

				page_to_check = root_url_usnews_local + "search?region=" + str(country)

			else:

				page_to_check = root_url_usnews_local + "search?country=" + str(country)

			for i in range(1,15):

				page_to_check_new = page_to_check + '&page=' + str(i)

				print page_to_check_new

				page_connection = requests.get(page_to_check_new)

				soup = bs4.BeautifulSoup(page_connection.text, 'lxml')

				schools = soup.select('div#resultsMain div.sep')

				for school in schools:

					rank  = school.select('div.thumb-left span')[0]

					rank.hidden = True

					rank = str(rank).replace('#','')

					rank = rank.split('<span>')[0]

					if rank is not '-':

						rank = rank

					else:

						rank = 0

					school_name  = school.select('div.block.unwrap h2 a')[0]

					school_name.hidden = True

					school_is_there = "SELECT * FROM school WHERE english_name = %s"

					try:

						cr.execute(school_is_there , (str(school_name),))

					except Exception, e:

						print 'We cannot do the select query, here are the reasons: ', e

					school_id = cr.fetchone()

					if school_id is None:

						print '#####################  This school does not exists'

					else:

						print '########## THIS IS THE LOCAL RANK: ', rank, '########## THIS IS THE SCHOOL NAME: ', str(school_name)

						query_test_exist = "SELECT rank_type_id FROM rank_type WHERE english_name='US NEWS Local'"

						try:

							cr.execute(query_test_exist)

						except Exception, e:

							print 'We cannot do the select query, here are the reasons: ', e					

						rank_type_id = int(cr.fetchone()[0])

						school_id = int(school_id[0])

						query_exists = "SELECT * FROM school_ranking WHERE school_id = %s AND rank_type_id = %s"

						query_insert = "INSERT INTO school_ranking (school_id, rank_type_id, rank, url) VALUES (%s, %s, %s, %s)"

						query_update = "UPDATE school_ranking SET (rank, url) = (%s, %s) WHERE school_id = %s AND rank_type_id = %s"

						try:

							cr.execute(query_exists , (school_id, rank_type_id,))

						except Exception, e:

							print 'We cannot do the select query, here are the reasons: ', e


						if cr.fetchone() is None:

							try:

								cr.execute(query_insert , (school_id, rank_type_id, rank, str(page_to_check_new), ))

								print 'You successufully created a new rank for an existing school'

							except Exception, e:

								print 'We cannot insert the school rank, here are the reasons: ', e		
						else:

							try:

								cr.execute(query_update , (rank, str(page_to_check_new), school_id, rank_type_id, ))

								print 'You successufully updated rank for an existing school'

							except Exception, e:

								print 'We cannot update the school rank, here are the reasons: ', e	

						common_major += 1

						conn.commit()

			print "Here are the common school", common_major

	def get_usnews_global_ranking(self):

		conn = self.__connect()	

		cr = conn.cursor()

		query_check_usnews_local = "SELECT * FROM rank_type WHERE english_name='US NEWS Global'"

		query_insert_usnews_local  = "INSERT INTO rank_type (english_name, description) VALUES ('US NEWS Global', 'These locales universities have been numerically ranked based on their positions in the overall Best Global Universities rankings. Schools were evaluated based on their research performance and their ratings by members of the academic community around the world and within North America. These are the top global universities in their country.')"

		try:

			cr.execute(query_check_usnews_local)

		except Exception, e:

			print "Your query selection did fail, here are the reasons: ", e

		if cr.fetchone() is None:

			try:

				cr.execute(query_insert_usnews_local)

				print "You inserted US NEWS Global into rank type"

			except Exception, e:

				print "Your insertion did fail, here are the reasons: ", e

		conn.commit()

		common_major = 0

		for i in range(1,50):

			page_to_check_new = root_url_usnews_local + 'rankings?page=' + str(i)

			print page_to_check_new

			page_connection = requests.get(page_to_check_new)

			soup = bs4.BeautifulSoup(page_connection.text, 'lxml')

			schools = soup.select('div#resultsMain div.sep')

			for school in schools:

				rank  = school.select('div.thumb-left span')[0]

				rank.hidden = True

				rank = str(rank).replace('#','')

				rank = rank.split('<span>')[0]

				if rank is not '-':

					rank = rank

				else:

					rank = 0

				school_name  = school.select('div.block.unwrap h2 a')[0]

				school_name.hidden = True

				school_is_there = "SELECT * FROM school WHERE english_name = %s"

				try:

					cr.execute(school_is_there , (str(school_name),))

				except Exception, e:

					print 'We cannot do the select query, here are the reasons: ', e

				school_id = cr.fetchone()

				if school_id is None:

					print '#####################  This school does not exists'

				else:

					print '########## THIS IS THE LOCAL RANK: ', rank, '########## THIS IS THE SCHOOL NAME: ', str(school_name)

					query_test_exist = "SELECT rank_type_id FROM rank_type WHERE english_name='US NEWS Global'"

					try:

						cr.execute(query_test_exist)

					except Exception, e:

						print 'We cannot do the select query, here are the reasons: ', e					

					rank_type_id = int(cr.fetchone()[0])

					school_id = int(school_id[0])

					query_exists = "SELECT * FROM school_ranking WHERE school_id = %s AND rank_type_id = %s"

					query_insert = "INSERT INTO school_ranking (school_id, rank_type_id, rank, url) VALUES (%s, %s, %s, %s)"

					query_update = "UPDATE school_ranking SET (rank, url) = (%s, %s) WHERE school_id = %s AND rank_type_id = %s"

					try:

						cr.execute(query_exists , (school_id, rank_type_id,))

					except Exception, e:

						print 'We cannot do the select query, here are the reasons: ', e

					if cr.fetchone() is None:

						try:

							cr.execute(query_insert , (school_id, rank_type_id, rank, str(page_to_check_new), ))

							print 'You successufully created a new rank for an existing school'

						except Exception, e:

							print 'We cannot insert the school rank, here are the reasons: ', e		
						
					else:

						try:

							cr.execute(query_update , (rank, str(page_to_check_new), school_id, rank_type_id, ))

							print 'You successufully updated rank for an existing school'

						except Exception, e:

							print 'We cannot update the school rank, here are the reasons: ', e	

					common_major += 1

					conn.commit()

			print "Here are the common school", common_major


# New_object = get_university_ranking()

# New_object.get_qs_ranking()

# New_object.get_usnews_national_ranking()

# New_object.get_usnews_global_ranking()



