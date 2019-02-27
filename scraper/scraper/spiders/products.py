# -*- coding: utf-8 -*-
import scrapy
import logging
import os
import re
import mysql.connector
import nltk

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


def get_asin(key):
    key = "%" + key + "%"
    cursor.execute("SELECT id FROM `asin` WHERE `keyword` LIKE '%s' and crawled=0 LIMIT 1" % key)
    a = cursor.fetchone()
    if a:
        return a[0]
    return a


# SELECT id FROM `asin` WHERE `keyword` like '%exercise yoga balls%' and crawled=0 LIMIT 1

class ScraperConfig:

    def __init__(self):
        pass

    @staticmethod
    def asin_url(asin):
        if not asin:
            return False
        return 'https://www.amazon.com/dp/' + asin

    @staticmethod
    def get_keyword():
        cursor.execute("SELECT word FROM `keywords` WHERE `scrapped` = 0 LIMIT 1")
        data = cursor.fetchall()
        if data:
            return data[0][0]
        else:
            return False


class ProductsSpider(scrapy.Spider):
    name = 'products'
    allowed_domains = ['amazon.com']
    start_urls = ['http://amazon.com/']

    def __init__(self, **kwargs):
        self.keyword = fetch_single("SELECT keyword FROM `state` WHERE 1")
        self.asin = get_asin(self.keyword)
        self.url = ScraperConfig.asin_url(self.asin)
        super(ProductsSpider, self).__init__(**kwargs)

    def start_requests(self):
        self.keyword = ScraperConfig.get_keyword()
        self.url = ScraperConfig.asin_url(get_asin(self.keyword))
        self.save_instance()
        yield scrapy.Request(self.url, self.parse)

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

        breadcrumbs = response.css('.a-subheader.a-breadcrumb ul li a::text')

        sub_cats = []
        main_cat = ""

        if breadcrumbs is not None:
            for li in breadcrumbs:
                if main_cat == "":
                    main_cat = li.get().strip()
                else:
                    sub_cats.append(li.get().strip())
        sub_cats = ';'.join(sub_cats)

        # number of sellers
        seller_number = response.css('#olp_feature_div a::text').get()

        if seller_number is not None:
            arr = seller_number.strip().encode().split()
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
                    shipping_weight = shipping_weight.split()[0] + " " + shipping_weight.split()[1]
                except():
                    shipping_weight = shipping_weight
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
                    except():
                        pass
            if dimension == "":
                dimension = re.findall(r"(\d\d x \d\d x \d\d [a-z]+)", table)
                if not dimension:
                    dimension = ""
                else:
                    try:
                        dimension = dimension[0]
                    except():
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
                    except():
                        shipping_weight = "err"
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) kg)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except():
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) g)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except():
                        pass
            if shipping_weight == "":
                shipping_weight = re.findall(r"((\d+(\.\d+)?) pound)", table)
                if not shipping_weight:
                    shipping_weight = ""
                else:
                    try:
                        shipping_weight = shipping_weight[0][0]
                    except():
                        pass

        log.debug([name, price,
                   manufacturer, rating, reviews,
                   rank, seller_number, main_cat,
                   sub_cats, self.keyword, response.url,
                   dimension, shipping_weight, shares,
                   self.asin])
        insert = mysql.cursor()
        insert.execute("UPDATE `asin` SET "
                       "`title`=%s,`price`=%s,"
                       "`manufacturer`=%s,`rating`=%s,`reviews`=%s,"
                       "`rank`=%s,`seller_count`=%s,`category`=%s,"
                       "`subcategory`=%s,`keyword`=%s,`url`=%s,"
                       "`dimension`=%s,`weight`=%s,`shares`=%s, `crawled`=1 "
                       "WHERE id = %s"
                       , (name, price, manufacturer, rating, reviews,
                          rank, seller_number, main_cat,
                          sub_cats, self.keyword, response.url,
                          dimension, shipping_weight, shares,
                          self.asin))
        log.debug(insert.statement)
        mysql.commit()
        self.asin = get_asin(self.keyword)
        self.url = ScraperConfig.asin_url(self.asin)
        log.debug("For next step asin %s url %s" % (self.asin, self.url))
        self.save_instance()
        if self.url:
            yield scrapy.Request(self.url, self.parse, headers={'referer': None})

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
                               "`asin`(`id`, `title`, `price`, `reviews`)"
                               " VALUES (%s,%s,%s,%s)",
                               (asin, title, price, review)
                               )
                mysql.commit()
                log.debug("Added %s" % title)
            else:
                log.debug("Ignored %s" % title)

    def save_instance(self):
        log.debug("SAVE INSTANCE CALLED")
        cursor.execute("UPDATE `state` SET `keyword`='%s',"
                       "`url`='%s'"
                       " WHERE id = 1 "
                       % (self.keyword, self.url))
        mysql.commit()
