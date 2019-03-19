import mysql.connector
import logging
import time
import os

# SELECT DISTINCT word_id from asin_word_rel WHERE asin_id in (SELECT DISTINCT asin_id FROM `asin_word_rel` WHERE word_id = 2435)

log = logging.getLogger('DataSorter')
start = time.time()
mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

selectCursor = mysql.cursor(buffered=True)


class ReviewGen:
    def __init__(self):
        self.counter = 1
        self.cursor = mysql.cursor()
        self.id = 0

    def parse_one(self):
        selectCursor.execute("SELECT id,word from word_dump WHERE `wcount` > 9")
        words = selectCursor.fetchall()
        for word_id in words:
            print word_id[0]
            selectCursor.execute("SELECT COUNT(asin) FROM asin WHERE id in "
                                 "(SELECT DISTINCT asin_id as id from asin_word_rel WHERE word_id=%s)"
                                 " and reviews < 601 and reviews is not null and reviews != 0", (word_id[0],))
            d = selectCursor.fetchone()
            selectCursor.execute("SELECT COUNT(asin) FROM asin WHERE id in "
                                 "(SELECT DISTINCT asin_id as id from asin_word_rel WHERE word_id=%s)", (word_id[0],))
            n = selectCursor.fetchone()
            if d[0] == 0:
                if n[0] != 0:
                    print "found %s" % word_id[0]
                    self.cursor.execute("INSERT INTO `valid_keyword` "
                                        "(`keyword`, `word_set`, `wcount`) "
                                        "VALUES (%s, %s, %s)", (word_id[1], word_id[0], n[0]))
                self.cursor.execute("UPDATE `word_dump` SET `searchlevel` = 1 WHERE `id` = %s;", (word_id[0],))
                self.counter += 1

            if self.counter > 500:
                self.counter = 0
                mysql.commit()
        mysql.commit()

    def parse_two(self):
        selectCursor.execute("SELECT id,word from word_dump WHERE `wcount` > 9 and searchlevel = 0")
        words = selectCursor.fetchall()
        for word in words:
            selectCursor.execute("SELECT id, word from word_dump WHERE id in"
                                 " (SELECT DISTINCT word_id as id from asin_word_rel where asin_id in"
                                 " (SELECT DISTINCT asin_id from asin_word_rel where word_id = %s)) "
                                 "AND searchlevel = 0 AND wcount > 9", (word[0],))
            rel_words = selectCursor.fetchall()

            for sword in rel_words:
                if word[0] != sword[0]:
                    selectCursor.execute("SELECT COUNT(*) FROM asin WHERE id in "
                                         "( SELECT asin_id as id from asin_word_rel "
                                         "WHERE word_id =%s or word_id=%s GROUP BY asin_id HAVING COUNT(*) > 1 ) "
                                         "and reviews < 601 and reviews is not null and reviews != 0",
                                         (word[0], sword[0]))

                    d = selectCursor.fetchone()

                    keyword = "%s %s" % (word[1], sword[1])
                    wset = "%s,%s" % (word[0], sword[0])

                    selectCursor.execute("SELECT COUNT(*) FROM asin WHERE id in "
                                         "( SELECT asin_id as id from asin_word_rel "
                                         "WHERE word_id =%s or word_id=%s "
                                         "GROUP BY asin_id HAVING COUNT(*) > 1 ) ",
                                         (word[0], sword[0]))

                    n = selectCursor.fetchone()

                    if d[0] == 0 and n[0] != 0:
                        print "%s %s %s #%s" % (keyword, wset, n[0], self.counter)
                        self.cursor.execute("INSERT INTO `valid_keyword` "
                                            "(`keyword`, `word_set`, `wcount`) "
                                            "VALUES (%s, %s, %s)", (keyword, wset, n[0]))
                        self.counter += 1

                    if self.counter > 100:
                        self.counter = 0
                        mysql.commit()
        mysql.commit()

    def parse_three(self):
        selectCursor.execute("SELECT id,word from word_dump WHERE 1")
        words = selectCursor.fetchall()
        for word in words:
            selectCursor.execute("SELECT w.id, w.word FROM asin_word_rel as rel "
                                 "INNER JOIN word_dump w ON w.id = rel.word_id "
                                 "where rel.asin_id in "
                                 "(SELECT asin_id from asin_word_rel where word_id = %s)", (word[0],))
            rel_words = selectCursor.fetchall()

            for sword in rel_words:
                if word[0] != sword[0]:
                    for dword in rel_words:
                        if dword[0] != sword[0] and dword[0] != word[0]:
                            selectCursor.execute("SELECT COUNT(*) FROM asin WHERE id in "
                                                 "( SELECT asin_id as id from asin_word_rel "
                                                 "WHERE word_id =%s or word_id=%s or word_id=%s "
                                                 "GROUP BY asin_id HAVING COUNT(*) > 1 ) "
                                                 "and reviews < 601 and reviews is not null and reviews != 0",
                                                 (word[0], sword[0], dword[0])
                                                 )

                            d = selectCursor.fetchone()
                            keyword = "%s %s %s" % (word[1], sword[1], dword[1])
                            wset = "%s,%s,%s" % (word[0], sword[0], dword[0])
                            if d[0] == 0:
                                self.cursor.execute("INSERT INTO `valid_keyword` "
                                                    "(`keyword`, `word_set`) "
                                                    "VALUES (%s, %s)", (keyword, wset))
                                self.counter += 1

                            if self.counter > 2000:
                                self.counter = 0
                                mysql.commit()
            mysql.commit()


ReviewGen().parse_two()
