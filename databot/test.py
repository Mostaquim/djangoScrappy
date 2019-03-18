import mysql.connector
import logging
from time import sleep
import os

log = logging.getLogger('DataSorter')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)
cursor = mysql.cursor()


class DataBot:
    def __init__(self):
        self.all = set()
        for i in range(0, 10000000):
            self.all.add(i)

    def insert(self):
        for i in self.all:
            cursor.execute("INSERT INTO `test` ( `a`) VALUES (%s)", (i,))
        mysql.commit()


databot = DataBot()
databot.insert()
