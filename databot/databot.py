import mysql.connector
import logging
import time
import os

log = logging.getLogger('DataSorter')
start = time.time()
mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

selectCursor = mysql.cursor(buffered=True)


class DataBot:
    def __init__(self):
        self.cursor = mysql.cursor()
        selectCursor.execute("SELECT id, title FROM asin WHERE id NOT IN(SELECT asin_id as id FROM asin_word_rel) "
                             "and title is not null and noselect = 0 LIMIT 5000")
        self.data = selectCursor.fetchall()

    def insertWord(self, asin_id, word):
        selectCursor.execute("SELECT id FROM word_dump WHERE word=%s", (word,))
        d = selectCursor.fetchone()
        if d:
            word_id = d[0]
        else:
            self.cursor.execute("INSERT INTO `word_dump`(`word`) VALUES (%s)", (word,))

            word_id = self.cursor.lastrowid
        self.cursor.execute("INSERT INTO `asin_word_rel`"
                            "(`asin_id`, `word_id`) VALUES "
                            "(%s,%s)", (asin_id, word_id))

    def insert(self):
        whitelist = set('abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        stopwords = {'on', 'the', 'a', 'for', 'with'}
        for d in self.data:
            asin_id = d[0]
            title = d[1]
            if title:
                z = False
                title = title.encode('ascii', 'ignore')
                title = ''.join(filter(whitelist.__contains__, title))
                title = title.split(' ')
                title = list(dict.fromkeys(title))
                for w in title:
                    if w:
                        if w not in stopwords:
                            if len(w) > 2:
                                z = True
                                self.insertWord(asin_id, w.lower())
                if not z:
                    print asin_id
                    self.cursor.execute("UPDATE `asin` set noselect = 1 where id = %s", (asin_id,))
        try:
            mysql.commit()
        except mysql.IntegrityError:
            print 'ERROR'
        finally:
            end = time.time()
            print(end - start)
            DataBot().insert()


DataBot().insert()
