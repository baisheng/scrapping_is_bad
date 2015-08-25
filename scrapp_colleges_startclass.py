import requests
import bs4
import sys  
from lxml.html.clean import clean_html
import psycopg2
from HTMLParser import HTMLParser

# Small trick to reload the system using utf8 rather then ascii2 that will cause some trouble with csv import in postgres
reload(sys)
sys.setdefaultencoding("utf-8")


class scrapp_colleges_startclass():

	global root_url

	root_url = 'http://colleges.startclass.com/ajax_search?_len=200&page=10&app_id=5307'

	def chec_the_json_answer(self):

		main_url = root_url + '?_len=' + str(30) + "&page=" + str(1) + "&app_id=" + str(5307)

		print main_url

		response = requests.get(str(root_url))
		
		soup = bs4.BeautifulSoup(response.text, 'lxml')

		print soup

scrapp_colleges_startclass = scrapp_colleges_startclass()

scrapp_colleges_startclass.chec_the_json_answer()