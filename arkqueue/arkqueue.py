#!/usr/bin/env python
#
# Program:      $Id: arkqueue.py 1637 2015-03-10 22:59:15Z jprohrer $
# Author:       Justin P. Rohrer <jprohrer@nps.edu>, based in part on arkmonitor.py by Robert Beverly <rbeverly@nps.edu>
# Description:  General class for holding useful Ark stuff, with advanced options and functionality

import struct
import socket
import random
import subprocess
import select
import datetime
import sys
import time
import Queue
import logging
import numpy as np
from threading import Thread
from threading import Event

#import tod
from counter import Counter
from arkvp import ArkVP


class ArkQueue(Thread):
    def __init__(self, useBad=False, monitorfile=None, sessionid=None, yaml=True, verbose=False, submit_hook=None, finish_hook=None, idle_hook=None, concurrency=25, timeout=600, monitor_blacklist=None, window_max=10080, loggingLevel=logging.INFO):
        Thread.__init__(self)
        self.verbose = verbose
        self.sessionid = sessionid
        self.concurrency = concurrency
        self.timeout = timeout
        self.submit_hook = submit_hook
        self.finish_hook = finish_hook
        self.idle_hook = idle_hook
        self.useBadMonitors = useBad
        self.max_rtt_hist = window_max
        self.logging_level = loggingLevel
        
        # create logger
        self.logger = logging.getLogger('[' + self.__class__.__name__ + ']')
        if loggingLevel:
            self.logger.setLevel(self.logging_level)
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
        
        self.targets = Queue.PriorityQueue()
        self.results = Queue.PriorityQueue()
        self.vpsUsed = 0
        self.monitor_flag = False
        self.exitEvent = Event()
        #signal.signal(signal.SIGINT, self.signal_handler)

        self.monitors = dict()
        #self.monitor_status = dict()    # Pending, Active, Unresponsive, 
        self.team = dict()
        self.vps = dict()
        self.probenum = Counter(0)
        self.maxtimeouts = 10
        self.requests_outstanding = dict()
        self.callbacks_t = Thread(target=self.callback_thread, args=(self.results,))
        self.callbacks_t.daemon = True
        
        if monitor_blacklist:
            self.blacklist = blacklist
        else:
            self.blacklist = ['nap-it', 'sea-us', 'nce-fr', 'bed-us', 'muc-de', 'ord-us', 'sin2-sg', 'nrt2-jp', 'gig-br', 'dkr-sn', 'mry-us']
        # process a CAIDA monitors.yaml file
        if monitorfile and yaml:
          self.readMonitorsYaml(monitorfile)
        if monitorfile and not yaml:
          self.readMonitorsTxt(monitorfile)
        for team in self.team:
          self.logger.debug("Updating with " + str(len(self.team[team])) + " team " + str(team) + " monitors")
          self.logger.debug(str(self.team[team]))
          self.monitors.update(self.team[team])
        self.logger.debug("Total " + str(len(self.monitors)) + " monitors: " + str(self.monitors))
        self.update_monitor_list(useBad=self.useBadMonitors)
        #if useBad == False:
        #  for down in self.blacklist:
        #    if down in self.monitors:
        #      del self.monitors[down]
        #self.monitors_by_ip = dict((v,k) for k, v in self.monitors.iteritems())
        #self.monitor_list = self.monitors.keys()
        #self.last_monitor = len(self.monitor_list) - 1
    
    def update_monitor_list(self, useBad=False):
        down_monitors = self.blacklist + self.vps_not_responding_list()
        temp_monitors = self.monitors
        if not useBad:
            for down in down_monitors:
                if down in temp_monitors:
                    del temp_monitors[down]
        self.monitors_by_ip = dict((v,k) for k, v in temp_monitors.iteritems())
        self.monitor_list = temp_monitors.keys()
        self.last_monitor = len(self.monitor_list) - 1
    
    #def signal_handler(self, signal, frame):
    #    self.exit()
    
    def stop(self):
        self.logger.info("Cleaning Up!")
        for vp in self.vps.keys():
            #sys.stdout.write('.')
            self.vps[vp].exit()
        #sys.stdout.write('\n')
        self.logger.info("Waiting for VP threads to exit.")
        for vp in self.vps.keys():
            sys.stdout.write('.')
            sys.stdout.flush()
            self.vps[vp].join()
        sys.stdout.write('\n')
        self.logger.info("Waiting for callbacks to finish.")
        self.callbacks_t.join()
        #self.results.join()
        self.logger.info("Callbacks finished.")
    
    def readMonitorsTxt(self, monitorfile):
      f = open(monitorfile, 'r')  
      self.team[1] = dict()
      for line in f:
        (mon, ip) = map(lambda x: x.strip(), line.strip().split(':'))
        self.team[1][mon] = ip

    def readMonitorsYaml(self, monitorfile):
      f = open(monitorfile, 'r')
      (mon, ip, team) = (None, None, None)
      for line in f:
        fields = line.strip().split()
        if len(fields) == 2:
          if fields[0] == '.monitor:':
            (mon, ip, team) = (None, None, None)
            mon = fields[1]
          if fields[0] == 'ip_address:':
            ip = fields[1]
          if fields[0] == 'team:': 
            team = fields[1]
          if not( (mon is None) or (ip is None) ):
            self.logger.debug("Monitor: "+ str(mon) +", IP: "+ str(ip) +", Team:"+ str(team))
            if (team not in self.team):
              self.team[team] = dict()
            self.team[team][mon] = ip
            (mon, ip, team) = (None, None, None)

    def numMonitors(self):
        return len(self.monitor_list)

    def getMonitors(self):
        return self.monitor_list

    def getNextMonitor(self):
        self.last_monitor = (self.last_monitor + 1) % len(self.monitor_list)
        return self.monitor_list[self.last_monitor]

    def getRandMonitor(self):
        monitor_index = random.randint(0,len(self.monitor_list)-1)
        return self.monitor_list[monitor_index]

    def existsMonitor(self, mon):
        if self.monitors.has_key(mon):
            return True
        return False

    def getMonitorByIP(self, ip):
        return self.monitors_by_ip[ip]

    def get(self, rand=False):
        if rand:
            return self.getRandMonitor()
        else:
            return self.getNextMonitor()

    @staticmethod
    def dottedQuadToNum(ip):
        "convert decimal dotted quad string to long integer"
        return struct.unpack('!I',socket.inet_aton(ip))[0]

    @staticmethod
    def numToDottedQuad(n):
        "convert long int to dotted quad string"
        return socket.inet_ntoa(struct.pack('!I',n))
    
    @staticmethod    
    def NetworkAddress(ip, bits):
        ipaddr = ArkMonitor.dottedQuadToNum(ip)
        mask = (0xffffffff << (32 - int(bits))) & 0xffffffff
        n = ipaddr & mask
        netaddr = ArkMonitor.numToDottedQuad(n)
        return netaddr
    
    def probes_submitted(self):
        probes_submitted = 0
        for vp in self.vps.keys():
            probes_submitted += self.vps[vp].getTotal()
        return probes_submitted
    
    def probes_waiting(self):
        probes_waiting = 0
        for vp in self.vps.keys():
            probes_waiting += self.vps[vp].getWaiting()
        return probes_waiting
    
    def probes_active(self):
        probes_active = 0
        for vp in self.vps.keys():
            probes_active += self.vps[vp].getOutstanding()
        return probes_active
        
    def probes_complete(self):
        probes_complete = 0
        for vp in self.vps.keys():
            probes_complete += self.vps[vp].getComplete()
        return probes_complete

    def targets_remaining(self):
        targets_remaining = 0
        for vp in self.vps.keys():
            targets_remaining += self.vps[vp].getIncomplete()
        return targets_remaining
    
    def vps_alive(self):
        vps_alive = 0
        for vp in self.vps.keys():
            if self.vps[vp].is_alive():
                vps_alive += 1
        return vps_alive

    def vps_active(self):
        vps_active = 0
        for vp in self.vps.keys():
            if self.vps[vp].isActive():
                vps_active += 1
        return vps_active
    
    def vps_responding(self):
        vps_resp = 0
        for vp in self.vps.keys():
            if self.vps[vp].isResponding():
                vps_resp += 1
        return vps_resp
    
    def vps_responding_list(self):
        vps_resp = list()
        for vp in self.vps.keys():
            if self.vps[vp].isResponding():
                vps_resp.append(vp)
        return vps_resp
    
    def vps_not_responding(self):
        vps_stopped = 0
        for vp in self.vps.keys():
            if not self.vps[vp].isResponding():
                vps_stopped += 1
        return vps_stopped
    
    def vps_not_responding_list(self):
        vps_stopped = list()
        for vp in self.vps.keys():
            if not self.vps[vp].isResponding():
                vps_stopped.append(vp)
        return vps_stopped
    
    def vps_stopped(self):
        vps_stopped = 0
        for vp in self.vps.keys():
            if not self.vps[vp].is_alive():
                vps_stopped += 1
        return vps_stopped
    
    def vps_stopped_list(self):
        vps_stopped = list()
        for vp in self.vps.keys():
            if not self.vps[vp].is_alive():
                vps_stopped.append(vp)
        return vps_stopped
    
    def vps_rtt_dict(self, window=1):
        vps_rtt = dict()
        if window > self.max_rtt_hist:
            self.logger.warning("RTT window requested is larger than max_rtt_hist: " + str(self.max_rtt_hist))
            for vp in self.vps.keys():
                vps_rtt[vp] = 0
        else:
            for vp in self.vps.keys():
                if self.vps[vp].isResponding():
                    vps_rtt[vp] = self.vps[vp].getRTT(window=window)
        
        return vps_rtt
    
    def avg_rtt(self, window=1):
        return np.mean(self.vps_rtt_dict(window=window).values())
        
    def clear_tod(self, vp=None, clear_responding=False):
        if vp and vp in self.vps.keys():
            self.vps[vp].clearTod()
        else:
            for vp in self.vps.keys():
                if (not self.vps.isResponding()) or clear_responding:
                    self.vps[vp].clearTod()
    
    def print_active(self):
        for vp in self.vps.keys():
            self.vps[vp].printActive()
        
        sys.stdout.write("\n")
        sys.stdout.flush()
    
    def print_up(self):
        for vp in self.vps.keys():
            self.vps[vp].printUp()
        
        sys.stdout.write("\n")
        sys.stdout.flush()
    
    def print_status(self):
        print "Probes active:", self.probes_active(), "Probes complete:", self.probes_complete(), "Probes waiting:", self.probes_waiting()
        print "Vantage points active:", self.vps_active(), "Vantage points stopped:", self.vps_stopped()
        print "Vantage points stopped:", self.vps_stopped_list()
    
    def print_vp_summary(self):
        for vp in self.vps.keys():
            self.vps[vp].printSummary()
    
    def print_summary(self):
        print self.__class__.__name__, "Probing Summary:"
        print "Total number of probes submitted:", self.probes_submitted()
        print "Number of probes completed:", self.probes_complete()
        print "Average probe completion time:", self.avg_rtt(window=self.max_rtt_hist), "s"
        print "Number of probes not completed:", self.targets_remaining()
        print "Number of Ark vantage points used:", self.vpsUsed
        print "Number of Ark vantage points not responding:", self.vps_not_responding()
        print "List of Ark vantage points not responding:", self.vps_not_responding_list()
    
    def is_active(self):
        return ((not self.targets.empty()) or (self.probes_waiting() > 0) or (self.probes_active() > 0)) and (self.vps_active() > 0)
    
    def is_responding(self):
        return self.vps_responding() > 0
    
    def monitor_til_done(self):
        self.monitor_flag = True
        #print 'Targets:', self.targets.qsize(), 'VP Targets:', self.targets_remaining(), 'Active probes:', self.probes_active(), 'VPs Alive:', self.vps_alive()
        while self.monitor_flag and self.is_active() and self.is_responding():
            self.print_status()
            time.sleep(10)
    
    def monitor_stop(self):
        self.monitor_flag = False
        
    def addProbe(self, targets, priority=3):
        while len(targets) > 0:
            [vp, trg] = targets.pop(0).split()
            self.targets.put([priority, vp, trg])
    
    def callback_thread(self, q):
        while not self.exitEvent.isSet():
            try:
                [priority, data] = q.get(timeout=10)
            except Queue.Empty:
                pass
            else:
                #print 'Result received, priority =', priority
                if priority == 2:               # probe request submitted to tod
                    [vp, trg] = data
                    #print 'VP =', vp, 'Target =', trg
                    if self.submit_hook:
                        self.submit_hook([vp, trg])
                
                elif priority == 3:             # probe request finished
                    [out, request] = data
                    #print 'Output =', out, 'Request =', request
                    if self.finish_hook:
                        self.finish_hook(out, request)
                q.task_done()
    
    # Emulate arkmonitor.py API and behavior
    def probe(self, submit_hook, finish_hook, targets, idle_hook=None, traces_in_flight=None, timeout=None):
        self.submit_hook = submit_hook
        self.finish_hook = finish_hook
        vpSet = set([i.split()[0] for i in targets])
        if idle_hook:
            self.idle_hook = idle_hook
        if traces_in_flight:
            self.concurrency = traces_in_flight // len(vpSet)
            if self.concurrency < 1:
                self.concurrency = 1
        if timeout:
            self.timeout = timeout
        
        self.addProbe(targets=targets)
        self.logger.info("Interacting with Ark ToD, will maintain " + str(self.concurrency) + " traces in flight per vantage point.")
        
        if not self.is_alive():
            self.start()
        
        time.sleep(1)	# Allow child threads to start so monitoring won't immediately exit
        self.monitor_til_done()
        self.exit(waitForExit=True)
        print '------------'
        self.print_summary()
    
    def run(self):
        #callbacks_t = Thread(target=callback_thread, args=(self.results,))
        self.callbacks_t.start()
        while not self.exitEvent.isSet():
            try:
                [priority, vp, trg] = self.targets.get(timeout=10)
            except Queue.Empty:
                pass
            else:
                if vp not in self.vps.keys():
                    self.vps[vp] = ArkVP(vpName=vp,sessionIdBase=self.sessionid,counter=self.probenum,result_queue=self.results,concurrency=self.concurrency,timeout=self.timeout,window_max=self.max_rtt_hist,loggingLevel=self.logging_level)
                    self.vps[vp].daemon = True    # thread dies with the program
                    self.vps[vp].start()
                    self.vpsUsed += 1
            
                self.vps[vp].addProbe(trg, priority)
                self.targets.task_done()
            
            self.update_monitor_list()
            
            if self.idle_hook:
                self.idle_hook()
        
        self.stop()
                    
    def exit(self, waitForExit=True):
        if self.exitEvent.isSet():
            return
        self.logger.info("Thread asked to exit.")
        self.exitEvent.set()
        #self.stop()
        if waitForExit:
            self.logger.info("Thread waiting to exit.")
            self.join()
            
        #sys.exit(0)
