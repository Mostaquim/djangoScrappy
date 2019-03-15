from twisted.web import server, resource
from twisted.internet import reactor, endpoints
import os
import logging
import datasorter

log = logging.getLogger('bot')


class Counter(resource.Resource):
    isLeaf = True
    numberRequests = 0
    start = ''

    def render_GET(self, request):
        request.setHeader(b"content-type", b"text/plain")
        content = "Bot stance %s" % format(self.start)
        return content.encode("ascii")

    def render_POST(self, request):

        post = request.args.keys()
        if 'start' in post:
            start = request.args['start'][0]
            if start == 'GO':
                self.start = 'GO'
                content = 'GO'
            else:
                self.start = 'NO GO'
                content = "NO GO"
            return content.encode("ascii")
        else:
            return "invalid request"


endpoints.serverFromString(reactor, "tcp:8888").listen(server.Site(Counter()))
reactor.run()
