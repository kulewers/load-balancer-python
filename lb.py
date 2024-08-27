#!/usr/bin/env python3

import threading
import socket
import time
import logging
import argparse
from urllib.parse import urlparse

backend_list = []
active_backends = []
threads = []

def health_check(event: threading.Event, interval: int = 10) -> None:
  global active_backends
  while True:
    for i, addr in enumerate(backend_list):
      backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      try:
        backend.connect(addr) 
        req = f'GET / HTTP/1.1\r\nHost: {addr[0]}:{addr[1]}\r\nAccept: */*\r\n\r\n'
        logging.debug(f'Sending health check request to "{addr[0]}:{addr[1]}":\n' + req)
        backend.send(req.encode('utf-8'))
        res = backend.recv(4096).decode('utf-8')
        logging.debug('Health check response status: ' + res.split('\r\n')[0])
        if res.split('\r\n')[0].split(' ')[1] == '200':
          active_backends[i] = True
        else:
          active_backends[i] = False
      except ConnectionRefusedError:
        logging.debug(f'Failed to establish connection to "{addr[0]}:{addr[1]}", marking server as inactive')
        active_backends[i] = False
    logging.debug("Health check pass complete, active backends: " + str(active_backends))
    event.set()
    time.sleep(interval)

def handle_client(conn: socket, addr: tuple[str, int], target: tuple[str, int]) -> None:
  req = conn.recv(4096).decode('utf-8')
  logging.info(f'Received request from "{addr[0]}":\n' + req)
  backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  backend.connect(target)
  backend.send(req.encode('utf-8'))
  res_header = backend.recv(4096).decode('utf-8')
  body_len = 0
  for header in res_header.split('\r\n')[1:]:
    if header.startswith("Content-Length:"):
      body_len = int(header.split(":")[1])
  if body_len:
    res_body = ''
    while body_len != len(res_body):
      res_body += backend.recv(4096).decode('utf-8')

    res = res_header+res_body
  else:
    res = res_header
  logging.info("Backend responded with:\n" + res)
  conn.send((res).encode('utf-8'))
  conn.close()

def main() -> None:
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
  )

  parser = argparse.ArgumentParser(
    prog="Load Balancer",
    description="Redistribute incoming http requests to a set of servers"
  )
  parser.add_argument('-b','--backends', required=True, action='extend', nargs='+')
  parser.add_argument('-H','--host', required=False, default='127.0.0.1')
  parser.add_argument('-p','--port', required=False, default=80)
  args = parser.parse_args()
  for backend in args.backends:
    if not backend.startswith('http://'):
      backend = 'http://' + backend
    b = urlparse(backend)
    if not b.port:
      logging.error(f'URL port not specified for backend "{backend}", ignorring option')      
      continue
    backend_list.append((b.hostname, b.port))
  logging.info(f'Initialized backend server options: {backend_list}')
  global active_backends
  active_backends = [False for _ in range(len(backend_list))]
  host = args.host
  port = int(args.port)

  server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  server.bind((host, port))
  server.listen()
  logging.info(f'Bound server to {host}:{port}')

  init_check = threading.Event()
  t = threading.Thread(target=health_check, args=(init_check,), daemon=True)
  t.start()

  logging.debug('Waiting for initial health check')
  init_check.wait()
  logging.debug('Initial health check complete')

  try:
    current_index = 0
    while True:
      logging.info("Server listening for incoming connections")
      conn, addr = server.accept()

      logging.debug("Healthy server count: " + str(active_backends.count(True)))
      if active_backends.count(True) < 1:
        raise Exception("No active servers, shutting down service")
      while active_backends[current_index] is False:
        current_index = (current_index + 1) % len(backend_list)
      target = backend_list[current_index]
      current_index = (current_index + 1) % len(backend_list)

      t = threading.Thread(target=handle_client, args=(conn, addr, target))
      t.start()

      threads.append(t)
  except KeyboardInterrupt:
    print("\nKeyboard interrupt received, exiting")
  except Exception as e:
    logging.critical(e)
  finally:
    if server:
      server.close()
    for thread in threads:
      thread.join()


if __name__ == '__main__':
  main()