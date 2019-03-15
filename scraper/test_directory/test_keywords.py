import mysql.connector
import collections
mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)

cursor = mysql.cursor(buffered=True)
cursor.execute("SELECT `title` FROM `asin` WHERE 1")

data = cursor.fetchall()
stopwords = {'on', 'the', 'a', 'for', 'with'}
wordcount = {}
for x in data:
    for word in x[0].replace('-', " ").replace('/', " ").lower().split():
        word = word.replace(".", "")
        word = word.replace(",", "")
        word = word.replace(":", "")
        word = word.replace("\"", "")
        word = word.replace("!", "")
        word = word.replace("*", "")
        word = word.replace("&", "")
        word = word.replace("-", "")
        word = word.replace("(", "")
        word = word.replace(")", "")
        word = word.replace("\u", "")
        if word not in stopwords:
            if word not in wordcount:
                wordcount[word] = 1
            else:
                wordcount[word] += 1
word_counter = collections.Counter(wordcount)
for word, count in word_counter.most_common(50):
    print(word, ": ", count)