from random import choice
from scrapy import signals
from scrapy.exceptions import NotConfigured
import mysql.connector

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

cursor = mysql.cursor(buffered=True)


def get_useragent():
    cursor.execute("SELECT string from useragent ORDER by rand() LIMIT 1")
    return cursor.fetchone()[0]


class RotateUserAgentMiddleware(object):
    """Rotate user-agent for each request."""

    def __init__(self):
        self.enabled = True

    def process_request(self, request, spider):
        request.headers['user-agent'] = get_useragent()
