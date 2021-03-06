import logging
import mysql.connector
from scrapy.conf import settings
import datetime

logger = logging.getLogger('ProxyMiddleWareLogger')


class Mode:
    def __init__(self):
        pass

    RANDOMIZE_PROXY_EVERY_REQUESTS, RANDOMIZE_PROXY_ONCE, SET_CUSTOM_PROXY = range(3)


mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)


def getProxy():
    cursor = mysql.cursor(buffered=True)
    cursor.execute("SELECT * FROM `proxy` ORDER BY `failed` ASC, `last_failed` ASC, `last_crawled` ASC")
    data = cursor.fetchone()
    return data


def saveCrawled(key):
    cursor = mysql.cursor(buffered=True)
    cursor.execute("UPDATE `proxy` SET `crawled` = `crawled` + 1 , `last_crawled`=NOW() WHERE `proxy`.`id` = %s;"
                   , (key,))
    mysql.commit()


def saveFailed(key, reason):
    cursor = mysql.cursor(buffered=True)
    cursor.execute("UPDATE `proxy` SET `failed` = `failed` + 1 , `last_failed`=NOW() , failed_reason = '%s'"
                   " WHERE `proxy`.`id` = %s;"
                   , (reason, key))
    mysql.commit()


class ProxyMiddleware(object):

    def __init__(self):
        self.mode = settings.get('PROXY_MODE')
        self.proxy_list = settings.get('PROXY_LIST')
        self.chosen_proxy = getProxy()

    def process_request(self, request, spider):
        proxy_address = "http://" + self.chosen_proxy[1] + ":" + self.chosen_proxy[2]
        logger.info('PROXY CHOSEN %s' % proxy_address)
        request.meta['proxy'] = proxy_address
        saveCrawled(self.chosen_proxy[0])
        request.meta['proxy_id'] = self.chosen_proxy[0]
        self.chosen_proxy = getProxy()
