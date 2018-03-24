#!/usr/bin/env python3
import random
import time
import signal
import socketserver
import sys

GOODSID = 200
MSG_NUM = 30
BROWSE_NUM = 5
STAY_TIME = 10
COLLECTION = [0, 1]
BUY_NUM = [0, 1, 0, 2, 0, 0, 0, 1, 0]

class SendMessage(socketserver.BaseRequestHandler):
  def handle(self):
    while True:
      for i in range(random.randint(0,MSG_NUM)):
        msg = str(random.randint(0, GOODSID)) + "::" + str(random.randint(1, BROWSE_NUM)) + "::" + \
              '{0:.3f}'.format(random.random() * 10) + "::" + \
              str(COLLECTION[random.randint(0, len(COLLECTION))-1]) + "::" +  \
              str(BUY_NUM[random.randint(0, len(BUY_NUM))-1]) + "\n"
        try:
          self.request.sendall(bytes(msg, encoding="utf-8"))
        except BrokenPipeError as e:
          print ("{} closed connection".format(self.client_address), file=sys.stderr)
          return
    
      time.sleep(1)
    

def ExitOnSignal(signum, frame):
  print("bye bye")
  exit(0)

signal.signal(signal.SIGINT, ExitOnSignal)

if __name__ == '__main__':
  server = socketserver.ThreadingTCPServer(("0.0.0.0", 9999), SendMessage)
  server.serve_forever()
