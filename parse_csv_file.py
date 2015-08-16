from sqlalchemy import create_engine
import pandas as pd
import sys
import csv
from StringIO import StringIO
import psycopg2

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")


# let's build our main class
class parse_csv_file():

	global pg_user

	global pg_db

	global pg_passwd

	pg_user = 'postgres'

	pg_db = 'test'

	pg_passwd = 'Password1!'

	def connect(self):

		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:

			conn = psycopg2.connect(conn_string)

		except:

			print 'I am not able to connect to the database'

		#cr = conn.cursor()
		return conn

	def create_the_table(self,pg_table):

		conn = self.connect()

		cr = conn.cursor()

		query_create_table = "CREATE TABLE %s"   

		# FIRST WE CHECK IF THE TABLE ALREADY EXISTS

		query_test = "select exists(select relname from pg_class where relname = '%s' and relkind='r')"

		try:

			cr.execute(query_test,pg_table)

		except Exception, e:

			print 'Ouppppppssss the query did fail... Here the reason: ', e

		if cr.fetchall()[0][0] == False:

			try:

				cr.execute(query_create_table,pg_table)

			except Exception, e:

				print 'Ouppppppssss we cannot create this table.... Here the reason: ', e

		conn.commit()

		conn.close()	
		

	def import_the_table(self,file_concerned,pg_table,index_name):

		cr = self.connect()

		#self.create_the_table(pg_table)

		engine = create_engine('postgresql://' + pg_user + ':' + pg_passwd + '@localhost/' + pg_db)

		df = pd.read_csv(file_concerned, error_bad_lines=False)

		df.to_sql(pg_table, engine, index=True, index_label=index_name, if_exists='append')


Import_crm_tables =  parse_csv_file()
# Import_crm_tables.import_the_table('/home/vagrant/sails_api_newpathway/DBCRM/school.csv','school')
# Import_crm_tables.import_the_table('/home/vagrant/sails_api_newpathway/DBCRM/Major.csv','major')
Import_crm_tables.import_the_table('/home/vagrant/sails_api_newpathway/DBCRM/TSchoolBe.csv','tschoolbe','school_2_id')

