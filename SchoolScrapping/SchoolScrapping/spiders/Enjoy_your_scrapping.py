from scrapy.spider import BaseSpider
from scrapy.selector import Selector
import codecs

class Enjoy_your_scrapping(BaseSpider):
    name = "enjoy_your_scrapping"
    allowed_domains = ["51offer.org"]
    start_urls = [
        "http://www.51offer.com/school/Newcastle.html"
    ]

    def parse(self, response):
        # filename = response.url.split("/")[-2]
        # open(filename, 'wb').write(response.body)
        sel = Selector(response)
        sites = sel.xpath('//div[@class="summarize"]')
        for site in sites:
            desc = site.xpath('text()').extract()
            print desc[0].encode('utf8')