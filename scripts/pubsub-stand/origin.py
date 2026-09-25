from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
import time
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  time.sleep(.1)
  body=('origin:'+self.path).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args): pass
ThreadingHTTPServer(('0.0.0.0',8080),Handler).serve_forever()
