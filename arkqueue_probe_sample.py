#!/usr/bin/env python
#
# Program:      $Id: arkqueue_probe_sample.py 1579 2015-02-26 19:41:31Z jprohrer $
# Author:       Justin P. Rohrer <jprohrer@nps.edu>, based extensively on sample.py by Robert Beverly <rbeverly@nps.edu>
# Description:  Demonstrate use of ArkQueue class with "probe mode" and submit() and finish() hooks 

import getopt
import sys
import os
import signal
from threading import Event
from arkqueue import arkqueue as aq
from arkqueue import tod

prog = os.path.basename(__file__)
exitEvent = Event()

def submit(x):
  print '[', prog, "] Executing request:\t", x

def finish(out, request):
  #hopNrtt = []
  print '[', prog, "] Raw output:", out, "for request:", request
  t = tod.ToD(out)
  print '[', prog, "] Destination:", t.getDst()
  #t.show()

def usage(prog):
  print "Usage:", prog, "[-hv]"
  sys.exit(-1)

ark = aq.ArkQueue(monitorfile="monitors.yaml", sessionid="ArkQueueProbeSample", yaml=True, verbose=False)

def exit():
    if exitEvent.isSet():
        return

    exitEvent.set()
    ark.exit()
    return

def signal_handler(signal, frame):
    exit()

signal.signal(signal.SIGINT, signal_handler)

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

  targets = ['san-us 128.61.2.1', 'san-us 130.207.244.244']
  targets.append('san-us 2607:f8b0:4005:802::1000')
  targets.append('ams-nl 2001:470:1f06:ee1::2')
  #targets =['ams-nl 2001:470:1f06:ee1::2']
  #targets =['san-us 2607:f8b0:4005:802::1000']
  ark.probe(submit, finish, targets, traces_in_flight=10)

if __name__ == "__main__":
  main()
