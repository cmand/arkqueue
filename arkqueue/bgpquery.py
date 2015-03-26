#!/usr/bin/env python
##########################################################################
# Program: $Id: bgpquery.py 433 2011-03-19 00:18:16Z rbeverly $
# Author:  Rob Beverly <rbeverly@lcs.mit.edu>
# Date:    $Date: 2011-03-18 17:18:16 -0700 (Fri, 18 Mar 2011) $
#
# Purpose: Routines to query BGP daemon
###########################################################################

from socket import *
import sys

class BGPquery:
    def __init__(self, host, port, debug=False):
        self.server = (host, port)
        self.debug = debug

    def connect(self):
        if self.debug:
            print "Connecting to:", self.server
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect(self.server)
   
    def query(self, q):
        if self.debug:
            print "Issuing query:", q
        q = q + "\n"
        self.sock.send(q)

    def lookup(self, ip):
        self.query("s " + ip)
        return self.read()

    def walk(self):
        self.query("w ")
        return self.read()

    def rand(self):
        self.query("r ")
        return self.read()

    def read(self):
        response = self.sock.recv(1024).rstrip()
        (ip, mask, asn) = (0, 0, 0)
        if (response.find("not found") == -1) and (response.find("end") == -1):
            if response.find(",") != -1:
                (prefix, asn) = response.split(",")
            else:
                prefix = response
            (ip, mask) = prefix.split("/")
        return (ip, mask, asn)
