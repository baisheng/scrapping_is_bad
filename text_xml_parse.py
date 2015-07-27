from lxml import html
import requests

page = requests.get('http://www.51offer.com/school/Newcastle.html')
tree = html.fromstring(page.text)
buyers = tree.xpath('//div[@class="summarize"]/text()')
print buyers