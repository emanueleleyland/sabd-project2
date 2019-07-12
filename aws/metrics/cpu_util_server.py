from http.server import BaseHTTPRequestHandler, HTTPServer
import psutil

# Creiamo la classe che riceverà e risponderà alla richieste HTTP
class testHTTPServer_RequestHandler(BaseHTTPRequestHandler):
  # Implementiamo il metodo che risponde alle richieste GET
  def do_GET(self):
        # Specifichiamo il codice di risposta
        self.send_response(200)
        # Specifichiamo uno o più header
        self.send_header('Content-type','text/html')
        self.end_headers()
        perc = psutil.cpu_percent()
        # Specifichiamo il messaggio che costituirà il corpo della risposta
        message = str(perc)
        self.wfile.write(bytes(message, "utf8"))
        return
def run():
  print('Avvio del server...')
  server_address = ('127.0.0.1', 9999)
  httpd = HTTPServer(server_address, testHTTPServer_RequestHandler)
  print('Server in esecuzione...')
  httpd.serve_forever()
run()