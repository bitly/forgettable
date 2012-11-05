import tornado.options
import tornado.web
import tornado.httpserver
import tornado.ioloop
from distribution import Distribution

class Application(tornado.web.Application):
    def __init__(self):

        app_settings = {
            'debug': True,
            "autoescape" : None,
        }

        handlers = [
            (r"/ping$", PingHandler),
            (r"/incr$", IncrHandler),
            (r"/get$", GetHandler),
        ]
        tornado.web.Application.__init__(self, handlers, **app_settings)

class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.finish('OK')
    def head(self):
        self.finish('OK')

class IncrHandler(tornado.web.RequestHandler):
    def get(self):
        key = self.get_argument('key')
        bin = self.get_argument('bin')
        Distribution(key).incr(bin)

class GetHandler(tornado.web.RequestHandler):
    def get(self):
        key = self.get_argument('key')
        dist = Distribution(key).get_dist()
        self.finish({
            "status_code":200,
            "data":[{
                "bin":key, 
                "probability":value
            } for key,value in dist.iteritems()]
        })

if __name__ == "__main__":
    tornado.options.define("port", default=8000, help="Listen on port", type=int)
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(request_callback=Application())
    http_server.listen(tornado.options.options.port, address="0.0.0.0")
    tornado.ioloop.IOLoop.instance().start()
