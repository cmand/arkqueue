#!/usr/bin/env python
#
# Program:      $Id: arkvp.py 1475 2014-12-04 23:51:42Z jprohrer $
# Author:       Justin P. Rohrer <jprohrer@nps.edu>
# Description:  Class for managing probes to be executed from a particular ARK vantage point

import sys, time, subprocess, select
import Queue
import logging
from threading import Thread
from threading import Event
#from threading import Timer
#import tod
from counter import Counter


class ArkVP(Thread):
    def __init__(self, vpName, counter, result_queue, sessionIdBase=None, concurrency=100, timeout=600, reanimate=True, window_max=10080, loggingLevel=logging.WARNING):
        Thread.__init__(self)
        self.vpName = vpName
        self.sessionId = sessionIdBase + ':' + vpName
        self.probenum = counter
        self.results = result_queue
        self.concurrency = concurrency
        self.timeout = timeout
        self.reanimate = reanimate
        self.max_rtt_hist = window_max
        self.lastActTime = time.time()
        self.probesWaiting = Queue.PriorityQueue()
        self.probesOutstanding = dict()
        self.timestamps = dict()
        self.RTTs = list()
        self.totalRequests = 0
        self.completedRequests = 0
        self.exitEvent = Event()
        #self.tracenum = 1
        self.responsive = True
        #signal.signal(signal.SIGINT, self.signal_handler)	# Signal only works in main thread
        
        # create logger
        self.logger = logging.getLogger('[' + self.__class__.__name__ + ':' + self.vpName + ']')
        if loggingLevel:
            self.logger.setLevel(loggingLevel)
        # create console handler and set level to debug
        ch = logging.StreamHandler()
        if loggingLevel:
            ch.setLevel(loggingLevel)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s:%(levelname)s: %(message)s")
        # add formatter to ch
        ch.setFormatter(formatter)
        # add ch to logger
        self.logger.addHandler(ch)
        
        self.todClient = subprocess.Popen(['./tod-client', '--session-id='+self.sessionId,'--concurrency='+str(self.concurrency)],shell=False,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
        
    #def signal_handler(self, signal, frame):
    #    self.exit()
    
    def addProbe(self, target, priority):
        self.probesWaiting.put([priority,target])
        self.totalRequests += 1
    
    def getWaiting(self):
        return self.probesWaiting.qsize()
    
    def getOutstanding(self):
        return len(self.probesOutstanding)
    
    def getLastAct(self):
        return self.lastActTime
    
    def getTotal(self):
        return self.totalRequests
    
    def getComplete(self):
        return self.completedRequests

    def getIncomplete(self):
        return self.totalRequests - self.completedRequests
    
    def getRTT(self, window=1):
        window = max(min(window, len(self.RTTs)), 1)
        sum_rtt = 0
        last_rtt = len(self.RTTs) - 1
        if last_rtt >= 0 and last_rtt >= window-1:
            for i in range(window):
                sum_rtt += self.RTTs[last_rtt-i]
        
        return sum_rtt / window
    
    def isActive(self):
        return (time.time() - self.lastActTime) < self.timeout
    
    def isResponding(self):
        #self.logger.debug('isActive: ' + str(self.isActive()) + ' RTT: ' + str(self.getRTT()) + ' Outstanding: ' + str(len(self.probesOutstanding)))
        if self.isActive() and (self.getRTT() < self.timeout):     # If VP active and responding within timeout, it is responsive
            if self.reanimate:
                self.responsive = True
        elif (len(self.probesOutstanding) > 0):     # If VP not active while probes are outstanding, it is not responsive
            if self.responsive:     # If first call since VP stopped responding
                self.logger.warning("Vantage point is not responding.")
            self.responsive = False
        
        return self.responsive
    
    def printActive(self):
        if self.isActive():
            sys.stdout.write('!')
        else:
            sys.stdout.write('.')
    
    def printUp(self):
        if self.isResponding():
            sys.stdout.write('!')
        else:
            sys.stdout.write('.')
    
    def printSummary(self):
        print "Ark vantage point name:", self.vpName
        print "Probes submitted:", self.getTotal()
        print "Probes completed:", self.getComplete()
        print "Probes incomplete:", self.getIncomplete(), "\n"
    
    def clearTod(self):
        todDebug = subprocess.Popen(['./tod-debug', '--session-id='+self.sessionId,'--clear-requests'],shell=False,stdout=subprocess.PIPE)
        #todDebug = subprocess.Popen(['./tod-debug', '--session-id='+self.sessionId,'--clear-requests'])
        try:
            fdready = select.select([todDebug.stdout], [], [], 30)
        except select.error as ex:
            self.logger.warning("Select error: " + str(ex))
        if len(fdready[0]) > 0:
            for line in todDebug.stdout:
                self.logger.debug(line.strip())
        self.probesOutstanding.clear()
    
    #def clearTod(self):
    #    todDebug = subprocess.Popen(['./tod-debug', '--session-id='+self.sessionId,'--clear-requests'],shell=False,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
    #    for line in iter(todDebug.stdout.readline, b''):
    #        self.logger.debug(line.strip())
    #    todDebug.stdout.close()
    #    self.probesOutstanding.clear()
        
    def stop(self):
        if self.is_alive():
            self.logger.debug("Thread stopping.")
            self.probesWaiting = Queue.PriorityQueue()
            self.rt.join()
            self.todClient.terminate()
            #todDebug = subprocess.Popen(['./tod-debug', '--session-id='+self.sessionId,'--clear-requests'],shell=False,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
            #try:
            #    fdready = select.select([todDebug.stdout], [], [], 60)
            #except select.error as ex:
            #    self.logger.warning("Select error: " + str(ex))
                #if ex[0] == 4:
                #    continue
                #else:
                #    raise
            #if len(fdready[0]) > 0:
            #    for line in todDebug.stdout:
            #        self.logger.info(line.strip())
            #todDebug.terminate()
            #self.probesOutstanding.clear()
            self.clearTod()
    
    def receive_thread(self):
        while not self.exitEvent.isSet():
            #while len(self.probesOutstanding) > 0 and self.isActive():
                # wait for data from tod-client (1 sec timeout)
                try:
                    fdready = select.select([self.todClient.stdout], [], [], 10)
                except select.error  as ex:
                    self.logger.error("Select error: " + str(ex))
                    if ex[0] == 4:
                        continue
                    else:
                        raise
                if len(fdready[0]) > 0:
                    out = self.todClient.stdout.readline()
                    if len(out.strip().split()) > 0:
                        reqid = int(out.strip().split()[0])
                        if reqid in self.probesOutstanding:
                            self.lastActTime = time.time()
                            self.timestamps[reqid][1] = self.lastActTime
                            rtt = self.timestamps[reqid][1] - self.timestamps[reqid][0]
                            self.logger.debug("Probe # " + str(reqid) + " took " + str(rtt) + " s")
                            self.RTTs.append(rtt)
                            del self.timestamps[reqid]
                            while len(self.RTTs) > self.max_rtt_hist:
                                self.timestamps.pop(0)
                            self.completedRequests += 1
                            #self.finish_hook(out, [self.vpName, self.probesOutstanding[reqid]])
                            self.results.put([3,[out, [self.vpName, self.probesOutstanding[reqid]]]])
                            del self.probesOutstanding[reqid]
                        else:
                            self.logger.warning("Received unexpected request ID: " + str(reqid))
            
            #time.sleep(60)
    
    def run(self):
        self.rt = Thread(target=self.receive_thread)
        self.rt.daemon = True
        self.rt.start()
        while not self.exitEvent.isSet():
            self.logger.debug("Probes active: " + str(len(self.probesOutstanding)) + " Targets remaining: " + str(self.probesWaiting.qsize()) + " Time since activity: " + str(time.time() - self.lastActTime))
            while len(self.probesOutstanding) < self.concurrency:
                try:
                    [priority,probe] = self.probesWaiting.get(timeout=10)
                except Queue.Empty:
                    break
                else:
                    probenum = self.probenum.increment()
                    todstring = str(probenum) + ' ' + self.vpName + ' trace ' + probe
                    try:
                        self.todClient.stdin.write(todstring + "\n")
                    except IOError as ex:
                        self.logger.error("IO Error: " + str(ex))
                        #if ex.errno == errno.EPIPE:
                        #    continue
                    else:
                        self.lastActTime = time.time()
                        self.probesOutstanding[probenum] = probe
                        self.timestamps[probenum] = [self.lastActTime, None]
                        #self.tracenum += 1
                    self.probesWaiting.task_done()
                    self.results.put([2, [self.vpName, probe]])
            
            time.sleep(10)
        
        self.stop()
            
    def exit(self):
        if self.exitEvent.isSet():
            return
        self.logger.debug("Thread asked to exit.")
        self.exitEvent.set()
        #self.stop()
        #self.logger.info("Thread waiting to exit.")
        #self.join()    # Cannot join current thread
        #self.logger.info("Thread exiting.")
