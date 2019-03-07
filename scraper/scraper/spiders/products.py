# -*- coding: utf-8 -*-
import scrapy
import logging
import os
import re
import mysql.connector
from scraper.modules.spiderjob import SpiderJob

log = logging.getLogger('ProductSpider')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)
logs_dir = os.path.abspath(os.path.join(os.path.realpath(__file__), '../../../logs'))

cursor = mysql.cursor(buffered=True)


def fetch_single(query):
    cursor.execute(query)
    data = cursor.fetchall()
    if data:
        return data[0][0]
    else:
        return False


class ProductsSpider(scrapy.Spider):
    name = 'products'

    def __init__(self, job=0, **kwargs):
        self.job = SpiderJob(job)
        self.url = 'https://www.amazon.com/dp/'
        self.asin = ''
        self.sets = set()
        super(ProductsSpider, self).__init__(**kwargs)

    def start_requests(self):
        self.sets = self.job.get_asin()
        if self.job.status == 2:
            log.debug('Skipping duplicate')
        else:
            if len(self.sets):
                self.asin = self.sets.pop()[0]
                self.job.start()
                url = self.url + self.asin
                yield scrapy.Request(url, self.parse)
            else:
                self.job.finish()

    def parse(self, response):
        filename = logs_dir + '/%s.html' % self.asin
        with open(filename, 'wb') as f:
            f.write(response.body)

        log.debug("(parse_asin) %s" % response.url)
        # field name
        name = response.css('#productTitle::text').get()
        if name is not None:
            name = name.encode('UTF-8').strip()
        else:
            name = response.css('#ebooksProductTitle::text').get()
            if name is not None:
                name = name.encode('UTF-8').strip()
        # field price
        price = response.css('#price_inside_buybox::text').get()

        if price is not None:
            price = price.strip()
        else:
            price = response.css('#priceblock_ourprice::text').get()
            if price is not None:
                price = price.strip()

        # field manufacturer
        manufacturer = response.css('#bylineInfo::text').get()

        if manufacturer is not None:
            manufacturer = manufacturer.encode('UTF-8').strip()

        # field rating
        rating = response.css('div#averageCustomerReviews .a-icon-alt::text').get()
        if rating is not None:
            rating = rating.encode('UTF-8').strip().split()[0]

        reviews = response.css('#acrCustomerReviewText::text').get()
        if reviews is not None:
            reviews = reviews.encode('UTF-8').strip().split()[0]
        # breadcrumbs contain categories

        breadcrumbs = response.css('.a-subheader.a-breadcrumb ul li a')

        sub_cats = []
        main_cat = ""

        if breadcrumbs is not None:
            for li in breadcrumbs:
                text = li.css("::text").get().strip()
                href = li.css('::attr(href)').get().split('&')
                url = False
                for node in href:
                    if 'node' in node:
                        url = 'https://www.amazon.com/b/?' + node
                if url:
                    self.job.save_url(text, url)

                if main_cat == "":
                    main_cat = text
                else:
                    sub_cats.append(text)

        sub_cats = ';'.join(sub_cats)
        # number of sellers
        seller_number = response.css('#olp_feature_div a::text').get()

        if seller_number is not None:
            arr = seller_number.strip().encode("ascii", "ignore").split()
            for i in arr:
                if '(' in i:
                    seller_number = i.replace('(', '').replace(')', '')
                if '$' in i and price is None:
                    price = i
        else:
            seller_number = response.xpath('//*[@id="unqualified"]/div[1]').extract()
            if seller_number is not None:
                seller_number = response.xpath('//*[@id="unqualified"]/div[1]/a/text()').get()
                if seller_number:
                    seller_number = seller_number.split()[0]
        if price is None:
            price = response.xpath('//*[@id="unqualified"]/div[1]/span/text()').get()

        # number of shares on social media
        shares = response.css('.share_count_text::text').get()
        if shares is not None:
            shares = shares.encode('UTF-8').strip()

        # two types of table found on amazon's platform one with a table and the other with a list
        # it contains field such as dimension shipping weight rank

        dimension = ""
        shipping_weight = ""
        rank = ""

        table = response.css('table#productDetails_detailBullets_sections1 tr')

        for tr in table:
            table_name = tr.css('th::text').get().strip()
            table_value = tr.css('td::text').get().strip()
            if 'Dimensions' in table_name:
                dimension = table_value.encode('UTF-8').strip()
            if 'Shipping' in table_name:
                shipping_weight = table_value.encode('UTF-8').strip()
                try:
                    shipping_weight = " ".join(shipping_weight.split())
                except IndexError:
                    shipping_weight = shipping_weight
                    pass
            if 'Rank' in table_name:
                rank = tr.css('td span span::text').get().strip().encode('UTF-8')
                rank = rank.split()[0]

        table = response.css('#detail-bullets').get()
        if table is not None:
            table = table.replace(',', '')
            if rank == "":
                rank = re.findall(r"(#\d+\d)", table)
                if not rank:
                    rank = ""
                else:
                    try:
                        rank = rank[0]
                    except IndexError:
                        pass
            if dimension == "":
                dimension = re.findall(r"(\d\d x \d\d x \d\d [a-z]+)", table)
                if not dimension:
                    dimension = ""
                else:
                    try:
                        dimension = dimension[0]
                    except IndexError:
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) ounces)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    shipping_weight = shipping_weight[0][0]
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) lb)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except IndexError:
                        shipping_weight = "err"
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) kg)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except IndexError:
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) g)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except IndexError:
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) pound)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except IndexError:
                        pass

        insert = mysql.cursor(buffered=True)
        if rank:
            rank = rank.replace(',', '').replace('#', '').strip()
        insert.execute("UPDATE `asin` SET "
                       "`title`=%s,`price`=%s,"
                       "`manufacturer`=%s,`rating`=%s,`reviews`=%s,"
                       "`rank`=%s,`seller_count`=%s,`category`=%s,"
                       "`subcategory`=%s,`url`=%s,"
                       "`dimension`=%s,`weight`=%s,`shares`=%s, `crawljob`=%s "
                       "WHERE asin = %s"
                       , (name, price, manufacturer, rating, reviews,
                          rank, seller_number, main_cat,
                          sub_cats, response.url,
                          dimension, shipping_weight, shares, self.job.job,
                          self.asin))

        mysql.commit()

        if len(self.sets) != 0:
            self.asin = self.sets.pop()[0]
            url = self.url + self.asin
            yield scrapy.Request(url, self.parse, headers={'referer': None})
        else:
            self.job.finish()
