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
class Import_sales_tables():
	global fileconcerned
	global pg_user
	global pg_db
	global pg_passwd
	global pg_table
	fileconcerned = '/home/vagrant/sails_api_newpathway/DBCRM/school.csv'
        pg_user = 'postgres'
	pg_db = 'test'
	pg_passwd = 'Password1!'
	pg_table = 'school' 

	def connect(self):
		conn_string = "host='localhost' dbname='test' user='postgres' password='Password1!'"

		try:
			conn = psycopg2.connect(conn_string)
		except:
			print 'I am not able to connect to the database'

		#cr = conn.cursor()
		return conn

	def table_change_name(self):
		request1 = 'ALTER TABLE "tcustomer" DROP COLUMN "CUST_ID" RESTRICT'
		request2 = 'ALTER TABLE "tcustomer" RENAME COLUMN "id" TO "CUST_ID"'
		conn = self.connect()			
		cr = conn.cursor()
		try:
			cr.execute(request1)
                	try:
                	        cr.execute(request2)
				message = 'Yawowwwww'
        	        except:
	                        message = 'The request to drop column worked however the eequest for changing table column name failled'
		except:
			message = 'The request to drop column worked'
		conn.commit()
		conn.close()
		print message

	def import_the_table(self):
		cr = self.connect()
		engine = create_engine('postgresql://' + pg_user + ':' + pg_passwd + '@localhost/' + pg_db)
		df = pd.read_csv(fileconcerned)
		df.to_sql(pg_table, engine, index=True, index_label='id', if_exists='append')

Go =  Import_sales_tables()
Go.import_the_table()
#Go.table_change_name()
