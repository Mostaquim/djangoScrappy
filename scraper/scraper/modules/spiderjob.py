import mysql.connector
import logging
import requests
import json

log = logging.getLogger('SpiderJob')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

cursor = mysql.cursor(buffered=True)


def query(query_str, args):
    cursor.execute(query_str, args)
    return [dict(zip([column[0] for column in cursor.description], row))
            for row in cursor.fetchall()]


class SpiderJob(object):

    def __init__(self, job):
        self.job = job
        data = query("SELECT * FROM jobs WHERE id=%s", (self.job,))
        self.status = data[0]['status']
        self.spider = data[0]['spider']
        self.searchBy = data[0]['searchby'].strip()
        paramkey = data[0]['param']

        if self.searchBy == "keywords":
            data = query("SELECT word , product from keywords where id=%s", (paramkey,))
            self.param = data[0]['word']
            self.product = data[0]['product']

        elif self.searchBy == "urls":
            data = query("SELECT url from urls where id=%s", (paramkey,))
            self.param = data[0]['url']
        else:
            self.param = paramkey

    def get_asin(self, prev=''):
        if self.searchBy == 'job':
            sKey = '%' + str(self.param) + '%'
            log.debug(sKey)
            cursor.execute("SELECT asin FROM `asin` WHERE job like %s and crawljob=0",
                           (sKey,))
        elif self.searchBy == 'product':
            cursor.execute("SELECT asin FROM `asin` WHERE product=%s and asin!=%s and crawljob=0",
                           (self.param, prev,))
        return set(cursor.fetchall())

    def start(self):
        cursor.execute("UPDATE `jobs` SET `status`=1, `updated`=NOW() WHERE id=%s", (self.job,))
        mysql.commit()

    def finish(self):
        if self.spider == 'searchpage':
            if self.product != 0:
                cursor.execute("UPDATE `products` SET `keyword_search` = '2' WHERE `products`.`id` = %s;",
                               (self.product,))
                mysql.commit()

        elif self.spider == 'products':
            cursor.execute("SELECT id FROM urls WHERE job =%s ", (self.job,))
            data = cursor.fetchall()
            for i in data:
                self.start_spider(i[0], 'urls', 'urls')
        if self.searchBy == 'products':
            cursor.execute("UPDATE `products` SET `keyword_search` = '3' WHERE `products`.`id` = %s;",
                           (self.product,))
            mysql.commit()
        cursor.execute("UPDATE `jobs` SET `status`=2, `updated`=NOW() WHERE id=%s", (self.job,))
        mysql.commit()

    def save_url(self, name, url):
        log.debug("%s %s %s " % (name, url, self.job))
        cursor.execute("INSERT IGNORE INTO `urls` ( `name`, `url` ,`job`) VALUES (%s,%s, %s)", (name, url, self.job))
        mysql.commit()

    def start_spider(self, param, searchby, spider):
        cursor.execute("INSERT INTO `jobs` (`param`, `searchby`, `spider`) "
                       "VALUES ( %s, %s,%s)", (param, searchby, spider))
        mysql.commit()
        newjob = cursor.lastrowid

        r = requests.post("http://localhost:6800/schedule.json",
                          data={'project': 'default',
                                'spider': spider,
                                'job': newjob,
                                }
                          )
        if r.status_code == 200:
            response = json.loads(r.text)
            log.debug(response)
            cursor.execute("INSERT INTO `job_logs` (job_id, job_scrapy_id) "
                           "VALUES (%s , %s)", (newjob, response['jobid']))
