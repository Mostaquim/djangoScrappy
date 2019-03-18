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


def insert_word(asin_id, word):
    cursor.execute("SELECT id FROM word_dump WHERE word=%s", (word,))
    d = cursor.fetchone()
    if d:
        word_id = d[0]
    else:
        cursor.execute("INSERT INTO `word_dump`(`word`) VALUES (%s)", (word,))
        mysql.commit()
        word_id = cursor.lastrowid
    cursor.execute("INSERT INTO `asin_word_rel`"
                   "(`asin_id`, `word_id`) VALUES "
                   "(%s,%s)", (asin_id, word_id))
    mysql.commit()


def parse_single():
    cursor.execute("SELECT id, title FROM asin WHERE id NOT IN(SELECT asin_id as id FROM asin_word_rel) "
                   "and title is not null LIMIT 1")
    d = cursor.fetchone()
    print d
    z = True
    if d:
        z = False
        whitelist = set('abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        stopwords = {'on', 'the', 'a', 'for', 'with'}

        asin_id = d[0]
        title = d[1]
        if title:
            title = title.encode('ascii', 'ignore')
            title = ''.join(filter(whitelist.__contains__, title))

            title = title.split(' ')
            title = list(dict.fromkeys(title))
            for w in title:
                if w:
                    if w not in stopwords:
                        if len(w) > 2:
                            z = True
                            insert_word(asin_id, w.lower())
        try:
            parse_single()
        except RuntimeError:
            sleep(2)
            print('Run Time error')
            os.system('python datasorter.py')

parse_single()
