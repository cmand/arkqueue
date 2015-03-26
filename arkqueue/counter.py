#!/usr/bin/env python
#
# Class:        $Id: counter.py 1515 2015-02-05 18:23:19Z jprohrer $
# Author:       Justin P. Rohrer <jprohrer@nps.edu>
# Description:  Thread-safe counter class

import threading

class Counter(object):
    def __init__(self, initval=0):
        self.val = initval
        self.lock = threading.Lock()

    def increment(self, delta=1):
        with self.lock:
            self.val += delta
            return self.val
	
	def decrement(self, delta=1):
	    with self.lock:
			self.val -= delta
			return self.val

    def value(self):
        with self.lock:
            return self.val