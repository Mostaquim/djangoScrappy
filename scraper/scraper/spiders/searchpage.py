import scrapy
import logging
import os
import re
import html2text
import mysql.connector
import nltk

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


class ScraperConfig:

    def __init__(self):
        pass

    @staticmethod
    def search_url(keyword):
        search_url = "https://www.amazon.com/s/?field-keywords="
        return search_url + keyword.replace(' ', '%20')

    @staticmethod
    def asin_url(asin):
        return 'https://www.amazon.com/dp/' + asin

    @staticmethod
    def get_keyword():
        cursor.execute("SELECT word FROM `keywords` WHERE `scrapped` = 0 LIMIT 1")
        data = cursor.fetchall()
        if data:
            return data[0][0]
        else:
            return False


class SearchpageSpider(scrapy.Spider):
    name = 'searchpage'

    def __init__(self, **kwargs):
        self.keyword = fetch_single("SELECT keyword FROM `state` WHERE 1")
        self.url = fetch_single("SELECT url FROM `state` WHERE 1")
        self.process = fetch_single("SELECT process FROM `state` WHERE 1")
        self.asin = ''
        log.debug("(init) Scraper Spider Initiated with Keyword %s , URL %s   , Process %s"
                  % (self.keyword, self.url,  self.process))
        super(SearchpageSpider, self).__init__(**kwargs)

    def start_requests(self):
        self.keyword = ScraperConfig.get_keyword()
        self.url = ScraperConfig.search_url(self.keyword)
        self.save_instance()
        self.save_instance()
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

                self.save_asin(asin, title, price, review)
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
                    self.save_asin(asin, title, price, review)

        # Getting Next Page Url
        next_page = response.css('a#pagnNextLink::attr(href)').get()
        if next_page is None:
            next_page = response.css('li.a-last a::attr(href)').get()

        if next_page:
            log.debug("Going To next Page")
            self.url = 'https://www.amazon.com%s' % next_page
            self.save_instance()

            yield scrapy.Request(self.url, self.parse)

    def save_asin(self, asin, title, price, review):
        log.debug("SAVE ASIN CALLED")
        if asin:
            if review:
                review = review.replace(',', '')
            keywords = self.keyword.lower().split()
            p = nltk.PorterStemmer()
            keywords = [p.stem(word) for word in keywords]
            log.debug(keywords)
            log.debug("%s %s %s %s " % (asin, title, price, review))
            if all(key in title.lower() for key in keywords):
                cursor.execute("INSERT IGNORE INTO "
                               "`asin`(`id`, `title`, `price`, `reviews`, `keyword`)"
                               " VALUES (%s,%s,%s,%s,%s)",
                               (asin, title, price, review, self.keyword)
                               )
                mysql.commit()
                log.debug("Added %s" % title)
            else:
                log.debug("Ignored %s" % title)

    def save_instance(self):
        log.debug("SAVE INSTANCE CALLED")
        # TODO check if save works
        cursor.execute("UPDATE `state` SET `keyword`='%s',"
                       "`url`='%s',"
                       "`process`='%s' "
                       " WHERE id = 1 "
                       % (self.keyword, self.url, self.process))
        mysql.commit()
