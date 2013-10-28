# 1 get hostname and defineif it host node or slave node
# 2 If node  is slave node start mdns service which will provide info about node to master node and other slave nodes
# 2 For both master and slave nodes start MDNS browser that will mantain list of avaliabale nodes
# 3

import BaseHTTPServer
import sys
import getopt
import threading
import signal
import socket
import httplib
import random
import string
# we use zeroconf to discover other nodes
from Zeroconf import *
from collections import namedtuple
# represent a node
Node = namedtuple("Node", "hostname address port")

MAX_CONTENT_LENGHT = 1024  # Maximum length of the content of the http request (1 kilobyte)
MAX_STORAGE_SIZE = 104857600  # Maximum total storage allowed (100 megabytes)
FRONTEND_NAME = "rocks.cs.uit.no"  # name of frontend (master node)
SERVICE_TYPE = "_http._tcp.local."
# SERVICE_NAME_SLAVE = "BackendNode" #use hostname instead of
APPLICATION_PORT = 23667

storageBackendNodes = []
httpdServeRequests = True

"""

"""


class StorageServerFrontend:
    def __init__(self):
        hostname = socket.gethostname()
        if hostname == FRONTEND_NAME:
            print "Master node start"
            r = Zeroconf()
            browser = ServiceBrowser(r, type, listener);
        else:
            print "Slave node " + hostname + " start"
            r = Zeroconf()
            ip = socket.gethostbyname(socket.gethostname())
            desc = {'desc': 'backend_node'}
            info = ServiceInfo(SERVICE_TYPE, hostname + "." + SERVICE_TYPE, socket.inet_aton(ip), APPLICATION_PORT, 0,
                               0, desc, hostname)
            print "mDNS service..."
            r.registerService(info)
            print "started OK"


    def sendGET(self, key):
        #node = random.choice(storageBackendNodes)

        # 	TODO:
        # 	Send a GET request to the node for the give key
        # 	return the data

        # (Remove this) Returns the value given the key
        return None

    def sendPUT(self, key, value, size):
        self.size = self.size + size
        node = random.choice(storageBackendNodes)

        # 	TODO:
        # 	Send a PUT request to the node with the key/value pair

        # (Remove this) Stores the key/value pair
        self.map[key] = value


"""

"""


class HttpRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    global frontend
    frontend = StorageServerFrontend()

    # Returns the
    def do_GET(self):
        key = self.path
        value = frontend.sendGET(key)
        print self.path

        if value is None:
            self.sendErrorResponse(404, "Key not found")
            return

        # Write header
        self.send_response(200)
        self.send_header("Content-type", "application/octet-stream")
        self.end_headers()

        # Write Body
        self.wfile.write(value)

    def do_PUT(self):
        contentLength = int(self.headers['Content-Length'])

        if contentLength <= 0 or contentLength > MAX_CONTENT_LENGHT:
            self.sendErrorResponse(400, "Content body to large")
            return

        frontend.size += contentLength
        if frontend.size > MAX_STORAGE_SIZE:
            self.sendErrorResponse(400, "Storage server(s) exhausted")
            return

        # Forward the request to the backend servers
        frontend.sendPUT(self.path, self.rfile.read(contentLength), contentLength)

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def sendErrorResponse(self, code, msg):
        self.send_response(code)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(msg)


"""
    WEB SERVER
"""


class FrontendHTTPServer(BaseHTTPServer.HTTPServer):
    def server_bind(self):
        BaseHTTPServer.HTTPServer.server_bind(self)
        self.socket.settimeout(1)
        self.run = True

    def get_request(self):
        while self.run == True:
            try:
                sock, addr = self.socket.accept()
                sock.settimeout(None)
                return (sock, addr)
            except socket.timeout:
                if not self.run:
                    raise socket.error

    def stop(self):
        self.run = False

    def serve(self):
        while self.run == True:
            self.handle_request()

            #

            # <p>Mdns Browser

            # <p>Used discover and present other nodes


class StorageServerDiscovery(object):
    def __init__(self):
        self.r = Zeroconf()
        pass

    def removeService(self, zeroconf, type, name):
        for member in storageBackendNodes:
            if member.hostname == name:
                storageBackendNodes.remove(member)
                return
        print "\tService", name, "removed"

    def addService(self, zeroconf, type, name):
        for member in storageBackendNodes:
            if member.hostname == name:
                return # already exist
        info = self.r.getServiceInfo(type, name)
        node = Node(name, socket.inet_ntoa(info.getAddress()), info.getPort())
        storageBackendNodes.append(node)
        print "\tNode", name, "added"


"""
Main function
"""
if __name__ == '__main__':
    httpserver_port = APPLICATION_PORT
    # Start the webserver which handles incomming requests
    try:
        httpd = FrontendHTTPServer(("", httpserver_port), HttpRequestHandler)
        server_thread = threading.Thread(target=httpd.serve)
        server_thread.daemon = True
        server_thread.start()

        def handler(signum, frame):
            print "Stopping http server..."
            httpd.stop()

        signal.signal(signal.SIGINT, handler)

    except:
        print "Error: unable to http server thread"

    # Start mdns browser to discover all oter nodes
    r = Zeroconf()
    listener = StorageServerDiscovery()
    browser = ServiceBrowser(r, SERVICE_TYPE, listener)

    print "mDNS browser OK"
    # Wait for server thread to exit
server_thread.join(100)




