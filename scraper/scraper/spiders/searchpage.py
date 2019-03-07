import scrapy
import logging
import os
import re
import html2text
import mysql.connector
from scraper.modules.spiderjob import SpiderJob

log = logging.getLogger('SearchSpider')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

cursor = mysql.cursor()


def fetch_single(query):
    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        return data[0][0]
    else:
        return False


logs_dir = os.path.abspath(os.path.join(os.path.realpath(__file__), '../../../logs'))


class SearchpageSpider(scrapy.Spider):
    name = 'searchpage'

    def __init__(self, job=False, **kwargs):

        self.job = SpiderJob(job)
        self.keyword = self.job.param
        self.url = 'https://www.amazon.com/s/?field-keywords='
        super(SearchpageSpider, self).__init__(**kwargs)

    def start_requests(self):
        self.url = self.url + self.job.param.replace(' ', '%')
        # duplicate run check
        if self.job.status == 2:
            log.debug('Skipping duplicate')
        else:
            self.job.start()
            yield scrapy.Request(self.url, self.parse)

    def parse(self, response):
        h = html2text.HTML2Text()
        h.ignore_links = True
        # Getting asins from search pages currently there are two page types
        asins = response.css('li.s-result-item')

        if asins:
            for elem in asins:
                title = elem.css('h2::text').get()
                asin = elem.css('::attr(data-asin)').get()
                elem_string = h.handle(elem.get())
                prices = re.findall(r"(\$\d+\.?\d+)", elem_string)
                if prices:
                    price = '-'.join(prices)
                else:
                    price = ''
                reviews = elem.css('a.a-size-small.a-link-normal.a-text-normal')
                review = ''
                if reviews:
                    for div in reviews:
                        a = div.css('::attr(href)').get()
                        if 'eview' in a:
                            review = div.css('::text').get()
                image = elem.css('img::attr(src)').get()
                if not image:
                    image = ''
                self.save_asin(asin, title, price, review, image)
        else:
            asins = response.css('.s-result-list>div')
            if asins:
                for elem in asins:
                    asin = elem.css('::attr(data-asin)').get()
                    title = elem.css('h5 span::text').get()
                    elem_string = h.handle(elem.get())
                    prices = re.findall(r"(\$\d+\.?\d+)", elem_string)
                    if prices:
                        price = '-'.join(prices)
                    else:
                        price = ''
                    reviews = elem.css('a.a-link-normal')
                    review = ''
                    if reviews:
                        for div in reviews:
                            a = div.css('::attr(href)').get()
                            if 'eview' in a:
                                review = div.css('span::text').get()
                    image = elem.css('img::attr(src)').get()
                    if not image:
                        image = ''
                    self.save_asin(asin, title, price, review, image)

        # Getting Next Page Url
        next_page = response.css('a#pagnNextLink::attr(href)').get()
        if next_page is None:
            next_page = response.css('li.a-last a::attr(href)').get()

        if next_page:
            log.debug("Going To next Page")
            self.url = 'https://www.amazon.com%s' % next_page
            yield scrapy.Request(self.url, self.parse)
        else:
            self.job.finish()

    def save_asin(self, asin, title, price, review, image):

        if asin:
            if review:
                review = review.replace(',', '')

            cursor.execute("INSERT INTO `asin`(`asin`, `title`, `price`, `reviews`, `job` , `image`)"
                           " VALUES (%s,%s,%s,%s,%s,%s) "
                           "ON DUPLICATE KEY UPDATE "
                           "`title`= %s,`price`=%s,`reviews`=%s, image=%s, "
                           "`job` = CONCAT(`job`, ',' , %s)",
                           (asin, title, price, review, self.job.job, image,
                            title, price, review, image,
                            self.job.job)
                           )
            mysql.commit()
