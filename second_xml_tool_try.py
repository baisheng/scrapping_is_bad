import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")


reload(sys)  
sys.setdefaultencoding('utf8')

global initial_location

initial_location = ['uk','us','au','nz','jp','sg']

class Enjoy_your_meal_with(HTMLParser):

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

		query_create_table = "CREATE TABLE school_51_offers (school_id serial, english_name varchar(150) NOT NULL, chinese_name varchar(150) NOT NULL, logo varchar(250), rank integer, country varchar(50), location varchar(50), city varchar(50), tuition_fees varchar, SAT_score decimal, school_profile text, world_ranking varchar, national_ranking varchar, cost_of_living varchar, teaching_type text, link varchar)"  

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

		query_create_table = "CREATE TABLE majors_51_offers (major_id serial, english_name varchar(150) NOT NULL, chinese_name varchar(150), degree_type varchar(250))"  

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


	def thank_you_for_your_school_51offer(self):

		self.create_school_table()

		conn = self.connect()	

		cr = conn.cursor()

		count = len(initial_location)

		err = 0

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
					school_location = soup.select('div.schoolLabel ul div div div a')
					school_logo = soup.select('div.schoolLabel ul li a img[src]')
					school_rank = soup.select('div,schoolLabel ul li div.rank')
					for n in range(len(soup.select('div.schoolLabel li'))):
						link = link_school[2*n].get('href',None)
						if link is not None:
							link = link
						english_name = school_english_name[2*n + 1]
						english_name.hidden = True
						chinese_name = school_chinese_name[n]
						chinese_name.hidden = True
						location = school_location[n]
						location.hidden = True
						logo = school_logo[n]
						rank = school_rank[n]
						rank.hidden = True
						query_verification = "SELECT COUNT(*) FROM school_51_offers WHERE english_name = %s OR chinese_name = %s"
						query_creation = "INSERT INTO school_51_offers (english_name, chinese_name, location, logo, link) VALUES (%s, %s, %s, %s, %s)"
						query_update = "UPDATE school_51_offers SET (english_name, chinese_name, location, logo, link) = (%s, %s, %s, %s, %s) WHERE english_name = %s OR chinese_name = %s"
						try:
							cr.execute(query_verification, (english_name.encode("utf-8").strip(), chinese_name.encode("utf-8").strip(),))
							test = cr.fetchall()[0][0]
							if int(test) == 0:
								try:
									cr.execute(query_creation , (english_name.encode("utf-8").strip(), chinese_name.encode("utf-8").strip(), location.encode("utf-8").strip(), logo.encode("utf-8").strip(), link.encode("utf-8").strip(), ))
								except Exception, e:
									print ("############ HERE THE error:", e)
									err += 1
							else:
								print "Exists"
								try:
									cr.execute(query_update , (english_name.encode("utf-8").strip(), chinese_name.encode("utf-8").strip(), location.encode("utf-8").strip(), logo.encode("utf-8").strip(), link.encode("utf-8").strip(), english_name.encode("utf-8").strip(), chinese_name.encode("utf-8").strip(), ))
									print "Is Being updated"
								except Exception, e:
									print ("############ HERE THE error:", e)
									err += 1
						except Exception, e:
							print "your request failled sorry, here the reason: ", e
							err += 1
		conn.commit()
		conn.close()
		print err


	def get_the_school_detail_page(self):

		conn = self.connect()	

		cr = conn.cursor()

		query_link = "SELECT english_name, chinese_name, link, school_id FROM school_51_offers ORDER BY school_id ASC"

		err = 0

		try:

			cr.execute(query_link)

		except Exception, e:

			print "your request failled sorry, here the reason: ", e

			err += 1

		if err == 0:

			links = cr.fetchall()

			for link in links:

				page_detail = str(root_url) + str(link[2])

				response = requests.get(page_detail)

				soup = bs4.BeautifulSoup(response.text, "lxml")
					
				content = soup.select('div.summarize div.text-content')[0]
				content.hidden = True

				country = soup.select('div.mete span label')[0]
				country.hidden = True

				city = soup.select('div.mete span label')[1]
				city.hidden = True

				if soup.select('div.school-ranking div.ranking-con span'):
					world_ranking = soup.select('div.school-ranking div.ranking-con span')[0]
					world_ranking.hidden = True
				else:
					world_ranking = 'Nan'

				if soup.select('div.school-ranking div.ranking-con span'):
					national_ranking = soup.select('div.school-ranking div.ranking-con span')[1]
					national_ranking.hidden = True
				else:
					national_ranking = 'Nan'
				
				if soup.select('div.sidebar_baiduXuqiu div'):
					tuition_fee = soup.select('div.sidebar_baiduXuqiu div')[0]
					tuition_fee.hidden = True
					tuition_fee = tuition_fee
				else:
					tuition_fee = ''
				
				if soup.select('div.sidebar_baiduXuqiu div') and len(soup.select('div.sidebar_baiduXuqiu div')) > 1:
					cost_of_living = soup.select('div.sidebar_baiduXuqiu div')[1]
					cost_of_living.hidden = True
					cost_of_living = tuition_fee
				else:
					cost_of_living = ''

				update_school_info = "UPDATE school_51_offers SET (school_profile, country, city, tuition_fees, cost_of_living, world_ranking, national_ranking) = (%s, %s, %s, %s, %s, %s, %s)  WHERE link = %s"

				try:
					cr.execute(update_school_info , (content.encode("utf-8").strip(), country.encode("utf-8").strip(), city.encode("utf-8").strip(), tuition_fee.encode("utf-8").strip(), cost_of_living.encode("utf-8").strip(), str(world_ranking).encode("utf-8").strip(), str(national_ranking).encode("utf-8").strip(), link[2], ))
					message = 'topppppp, that is working'
					# message +=  '\n##### HERE IS THE SCHOOL CITY:', city.encode("utf-8").strip(), '\n##### HERE ARE THE SCHOOL TUITION FEES:', tuition_fee.strip(), '\n##### HERE ARE THE COST OF LIVING:', cost_of_living.strip()
					conn.commit()
				except Exception, e:
					message = 'OUPPPPsss your update failled here the reason:', e
				print message
		conn.commit()
		conn.close()

	def get_the_majors(self):

		self.create_major_table()

		conn = self.connect()	

		cr = conn.cursor()

		query_link = "SELECT english_name, chinese_name, link, school_id FROM school_51_offers ORDER BY school_id ASC"

		err = 0

		try:

			cr.execute(query_link)

		except Exception, e:

			print "your request failled sorry, here the reason: ", e

			err += 1

		if err == 0:

			links = cr.fetchall()

			nbr_major = 0

			for link in links:

				link_array = link[2].split('/')

				new_link = '/' + link_array[1] + '/specialty_' + link_array[2]

				page_detail = str(root_url) + str(new_link)

				print page_detail

				response = requests.get(page_detail)

				soup = bs4.BeautifulSoup(response.text, "lxml")

				majors = soup.select('ul.list-ul.list-specialty li')

				if majors:

					for page_nbr in range(100):

						new_page_detail = page_detail + '?pageNo=' + str(page_nbr)

						response = requests.post(new_page_detail)

						soup = bs4.BeautifulSoup(response.text, "lxml")

						majors = soup.select('ul.list-ul.list-specialty li')

						if majors:

							num_major = 0

							for major in majors:

								major_name = soup.select('ul.list-ul.list-specialty li h6')[num_major]

								# major_name_array = major_name.split('(')

								degree_type = soup.select('ul.list-ul.list-specialty li h6')[num_major]

								num_major += 1

								query_verification = "SELECT COUNT(*) FROM majors_51_offers WHERE english_name = %s"

								query_creation = "INSERT INTO majors_51_offers (english_name, degree_type) VALUES (%s, %s)"

								query_update = "UPDATE majors_51_offers SET (english_name, degree_type = (%s, %s) WHERE english_name = %s"

								try:

									cr.execute(query_verification, (major_name.encode("utf-8").strip(),))

									test = cr.fetchall()[0][0]

									if int(test) == 0:

										try:

											cr.execute(query_creation , (major_name.encode("utf-8").strip(), degree_type.encode("utf-8").strip(), ))
										
										except Exception, e:

											print ("############ HERE THE error:", e)

											err += 1

									else:

										print "Exists"

										try:

											cr.execute(query_update , (major_name.encode("utf-8").strip(), degree_type.encode("utf-8").strip(), major_name.encode("utf-8").strip(),))
											
											print "Is Being updated"

										except Exception, e:

											print ("############ HERE THE error:", e)

											err += 1

								except Exception, e:

									print "your request failled sorry, here the reason: ", e

									err += 1

								conn.commit()

								print new_page_detail
			conn.close()
			print nbr_major



New_object = Enjoy_your_meal_with()
print "############### HERE THE TEST MAIN PAGES"
New_object.thank_you_for_your_school_51offer()
print "############### HERE THE TEST ON A SCHOOL PAGE"
New_object.get_the_school_detail_page()
print "############### HERE THE TEST ON A MAJOR PAGE"
New_object.get_the_majors()


