#!/usr/bin/env python
#
# Program:      $Id: arkqueue_sample.py 1491 2014-12-17 22:07:32Z jprohrer $
# Author:       Justin P. Rohrer <jprohrer@nps.edu>
# Description:  Demonstrate use of ArkQueue class

import getopt
import sys
import os
import time
import signal
from threading import Event
from arkqueue import arkqueue as aq
from arkqueue import tod

prog = os.path.basename(__file__)
exitEvent = Event()

hopNrtt = []

def exit():
    if exitEvent.isSet():
        return

    exitEvent.set()
    return

def signal_handler(signal, frame):
    exit()

signal.signal(signal.SIGINT, signal_handler)

def submit(x):
    sys.stdout.write('+')

def finish(out, request):
    sys.stdout.write('^')

def usage(prog):
    print "Usage:", prog, "[-hv]"
    sys.exit(-1)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hf:",  ["help", "flight="])
    except getopt.GetoptError, err:
        usage(sys.argv[0])

    for o, a in opts:
        if o in ("-f", "--flight"):
            traces_in_flight = int(a)
        elif o in ("-h", "--help"):
            usage(sys.argv[0])
        else:
            assert False, "unhandled option"


    print '[', prog, "]Interacting with Ark ToD, will maintain 1 traces in flight per vantage point."
    ark = aq.ArkQueue(monitorfile="monitors.yaml", sessionid="ArkQueueSample", yaml=True, verbose=False, submit_hook=submit, finish_hook=finish, idle_hook=None, concurrency=10, timeout=60, monitor_blacklist=list())
    vps = ark.getMonitors()
    targets = ['128.61.2.1', '130.207.244.244', '2607:f8b0:4005:802::1000', '2001:470:1f06:ee1::2']
    ark.start()
    for i in range(0, 100):
        vp = vps[i % len(vps)]
        target = targets[i % len(targets)]
        #print vp + ' ' + target
        ark.addProbe(targets=[vp + ' ' + target], priority=3)
    
    time.sleep(1)   # Allow child threads to start so monitoring won't immediately exit
    #ark.monitor_til_done()
    while (not exitEvent.isSet()) and ark.is_active():
        #sys.stdout.write('*')
        sys.stdout.write("\n")
        ark.print_up()
        #sys.stdout.write("\n")
        #sys.stdout.write("\n")
        #ark.print_status()
        time.sleep(10)
    
    print 'Exiting Ark'
    ark.exit()
    print '------------'
    ark.print_summary()

if __name__ == "__main__":
    main()
