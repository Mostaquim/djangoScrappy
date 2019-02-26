from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
import logging
import mysql.connector
import datetime

logger = logging.getLogger('CustomRetryMiddleware')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)


def saveFailed(key, reason):
    cursor = mysql.cursor(buffered=True)
    cursor.execute("UPDATE `proxy` SET `failed` = `failed` + 1 , `last_failed`='%s' , failed_reason = '%s'"
                   " WHERE `proxy`.`id` = %s;"
                   % (datetime.datetime.now(), reason, key))
    mysql.commit()


class CustomRetryMiddleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        logger.debug(response.status)
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            saveFailed(request.meta['proxy_id'], reason)
            return self._retry(request, reason, spider) or response

        # this is your check
        if response.status == 200:
            if 'Robot' in response.xpath('/html/head/title').get():
                saveFailed(request.meta['proxy_id'], "Captcha Problem")
                return self._retry(request, 'Captcha Problem', spider) or response
        return response

