from scrapy.downloadermiddlewares.retry import RetryMiddleware
import logging
import mysql.connector
import datetime
from scrapy.utils.response import response_status_message
from scrapy.utils.python import global_object_name

logger = logging.getLogger('CustomRetryMiddleware')

mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="metro2o33",
    database="web_scrapper"
)


def saveFailed(key, reason):
    cursor = mysql.cursor(buffered=True)

    cursor.execute("UPDATE `proxy` SET `failed` = `failed` + 1 , `last_failed`=NOW() , failed_reason = %s"
                   " WHERE `proxy`.`id` = %s;", (str(reason), key))
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

        # captcha check
        if response.status == 200:
            if 'Robot' in response.xpath('/html/head/title').get():
                logger.debug(response.xpath('/html/head/title').get())
                return self._retry(request, 'Captcha Problem', spider) or response
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, self.EXCEPTIONS_TO_RETRY) \
                and not request.meta.get('dont_retry', False):
            return self._retry(request, exception, spider)

    def _retry(self, request, reason, spider):
        retries = request.meta.get('retry_times', 0) + 1

        retry_times = self.max_retry_times

        if 'max_retry_times' in request.meta:
            retry_times = request.meta['max_retry_times']

        stats = spider.crawler.stats
        if retries <= retry_times:
            logger.debug("Retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
            saveFailed(request.meta['proxy_id'], reason)
            retryreq = request.copy()
            retryreq.meta['retry_times'] = retries
            retryreq.dont_filter = True
            retryreq.priority = request.priority + self.priority_adjust

            if isinstance(reason, Exception):
                reason = global_object_name(reason.__class__)

            stats.inc_value('retry/count')
            stats.inc_value('retry/reason_count/%s' % reason)
            return retryreq
        else:
            stats.inc_value('retry/max_reached')
            logger.debug("Gave up retrying %(request)s (failed %(retries)d times): %(reason)s",
                         {'request': request, 'retries': retries, 'reason': reason},
                         extra={'spider': spider})
