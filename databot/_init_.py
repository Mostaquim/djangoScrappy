#
# related word_id query
# SELECT DISTINCT word_id from asin_word_rel WHERE EXISTS (SELECT asin_id from (SELECT DISTINCT asin_id FROM `asin_word_rel` WHERE word_id = 2435) as a WHERE a.asin_id = asin_word_rel.asin_id)
#
# FIND common asin by two word_id
# SELECT * FROM asin WHERE EXISTS ( SELECT id from (SELECT DISTINCT asin_id as id from asin_word_rel WHERE word_id=2435 OR word_id=2436 GROUP BY asin_id HAVING COUNT(*) > 1) as a WHERE a.id = asin.id )
