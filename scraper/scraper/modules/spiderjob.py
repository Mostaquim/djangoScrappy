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
        self.searchBy = data[0]['searchby']
        paramkey = data[0]['param']
        if self.searchBy == 'keywords':
            data = query("SELECT word from keywords where id=%s", (paramkey,))
            self.param = data[0]['word']
        else:
            self.param = paramkey

    # todo fix asin duplication issue
    def get_asin(self, prev=''):
        if self.searchBy == 'job':
            cursor.execute("SELECT asin FROM `asin` WHERE job=%s and asin!=%s and crawljob=0",
                           (self.param, prev,))
        elif self.searchBy == 'product':
            cursor.execute("SELECT asin FROM `asin` WHERE product=%s and asin!=%s and crawljob=0",
                           (self.param, prev,))
        return set(cursor.fetchall())

    def start(self):
        cursor.execute("UPDATE `jobs` SET `status`=1, `updated`=NOW() WHERE id=%s", (self.job,))
        mysql.commit()

    def finish(self):
        cursor.execute("SELECT COUNT(asin) FROM `asin` WHERE reviews > 600 and job = %s", (self.job,))
        result = cursor.fetchone()[0]
        log.debug(result)
        if result < 11:
            cursor.execute("INSERT INTO `jobs` (`param`, `searchby`, `spider`) "
                           "VALUES ( %s, 'job',  'products')", (self.job,))
            mysql.commit()
            newjob = cursor.lastrowid
            r = requests.post("http://localhost:6800/schedule.json",
                              data={'project': 'default',
                                    'spider': 'products',
                                    'job': self.job,
                                    }
                              )
            if r.status_code == 200:
                response = json.loads(r.text)
                cursor.execute("INSERT INTO `job_logs` (job_id, job_scrapy_id) "
                               "VALUES (%s , %s)", (newjob, response['jobid']))

        cursor.execute("UPDATE `jobs` SET `status`=2, `updated`=NOW() WHERE id=%s", (self.job,))
        mysql.commit()
