from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
import logging

logger = logging.getLogger('CustomRetryMiddleware')


class CustomRetryMiddleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        logger.debug("PROXY ID : %s" % request.meta['proxy_id'])

        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response

        # this is your check
        if response.status == 200:
            if 'Robot' in response.xpath('/html/head/title').get():
                return self._retry(request, 'Captcha Problem', spider) or response
        return response
