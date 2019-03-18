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
# related word_id query
# SELECT DISTINCT word_id from asin_word_rel WHERE EXISTS (SELECT asin_id from (SELECT DISTINCT asin_id FROM `asin_word_rel` WHERE word_id = 2435) as a WHERE a.asin_id = asin_word_rel.asin_id)

# SELECT DISTINCT asin_id from asin_word_rel WHERE word_id=2435 OR word_id=2436 GROUP BY asin_id HAVING COUNT(*) > 1
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
                   "and title is not null and noselect = 0 LIMIT 1")
    d = cursor.fetchone()
    z = False
    if d:
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
            if not z:
                print asin_id
                cursor.execute("UPDATE `asin` set noselect = 1 where id = %s", (asin_id,))
                mysql.commit()
            parse_single()
        except RuntimeError:
            sleep(2)
            print('Run Time error')
            os.system('python datasorter.py')


parse_single()
