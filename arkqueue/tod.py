#!/usr/bin/env python
#
# Program:      $Id: tod.py 1420 2014-09-06 21:30:08Z gcai $
# Author:       Robert Beverly <rbeverly@nps.edu>
# Description:  General class for processing topology on demand traces

import bgpquery

class ToD:
    def __init__(self, line):
        self.line = line
        self.range = [0,0]
        self.used = False
        self.reached_prefix = False
        #if len(line.strip().split(None)) < 17:
        #    print line
        try:
            (self.reqid, tmp, tmp, tmp, self.src, self.dst,
         tmp, tmp, self.ts, self.dstreached, self.rtt, self.ttl, self.rttl,
         self.haltreason, tmp, self.status, self.path) = line.strip().split(None,16)
        except:
            (self.reqid, tmp, tmp, tmp, self.src, self.dst,
         tmp, tmp, self.ts, self.dstreached, self.rtt, self.ttl, self.rttl,
         self.haltreason, tmp, self.status) = line.strip().split(None,16)
            self.path = ''
        self.ASN = 0
        self.ASNhops = []
        self.destASNhops = []   # Gathers interfaces that belong to
                                # the destination ASN.
        self.hops = []
        self.rtts = []
        self.edges = set()
        for hop in self.path.split():
            if hop[0] == 'q':
                self.hops.append('0.0.0.0')
                continue
            # XXX - when do we get this behavior?
            if hop.find(";") > -1:
                hop = hop.split(";")[0]
            (ip, rtt, tries) = hop.split(",")
            #print "[tod.py] ", ip, "\trtt: ", rtt, "\ttries: ", tries
            if len(self.hops) > 0:
                if self.hops[-1] != '0.0.0.0':
                    self.edges.add((self.hops[-1],ip))
            self.hops.append(ip)
            self.rtts.append(rtt)

        self.vertices = set([x for x in self.hops if x != '0.0.0.0'])
        self.vertices.add(self.src)


    def printSummary(self):
        print "[tod.py] Trace:", self.src, "->", self.dst, "[", self.range[0], ":", self.range[1], "]"

    def coversize(self):
        if self.range[0] == 0 and self.range[1] == 0:
            return 0
        return self.range[1] - self.range[0] + 1

    def addcoverhop(self, hop_num):
        if self.range[0] == 0:
            self.range[0] = hop_num
        self.range[1] = hop_num

    def reset(self):
        self.range = [0,0]

    def writeout(self, file):
        file.write(self.line)

    def show(self, start=1, end=9999):
        print "[tod.py] ReqId ", self.reqid, ":    ", self.src, "->", self.dst, ", ASN:", self.ASN, 
        if start != 1 or end != 9999:
            print "range:", start, "->", end,
        if end > len(self.hops):
            end = len(self.hops)
        print 
        for i in range(start, end + 1):
            print("\t(Hop " + str(i) +") " + self.hops[i-1] + "\tRTT:" + self.rtts[i-1])
        #prints RTT to destination for completeness
        print "\t(Dest) ", self.dst, "\tRTT:", self.rtt

    def showCSV(self, start=1, end=9999):
        #print "ReqID, Source, Destination"
        #print(self.reqid + "," + self.src + "," + self.dst)
        if start != 1 or end != 9999:
            print "range:", start, "->", end,
        if end > len(self.hops):
            end = len(self.hops)
        #print "ReqID, Hop#, Hop, RTT"
        for i in range(start, end + 1):
            print(self.reqid + "," + str(i) + "," + self.hops[i-1] + "," + self.rtts[i-1])
        #prints RTT to destination for completeness
        print(str(end+1) + "," + self.dst + "," + self.rtt)
            
    def store(self, start=1, end=9999):
        text =  "[tod.py] Trace:" + str(self.src) + "->" + str(self.dst) + \
                "ASN:" + str(self.ASN) + "\n"
        if start != 1 or end != 9999:
            text = text + "range:" + str(start) + "->" + str(end) + "\n"
        if end > len(self.hops):
            end = len(self.hops)
        for i in range(start, end + 1):
            text = text + "\t" + str(i) + "Hop:" + str(self.hops[i-1])
        return text

    #returns if dstReached status
    def getDstReached(self):
        return self.dstreached

    #returns list of tuples
    def getHopRTT(self, start=1, end=9999):
        results=[]
        if start != 1 or end != 9999:
            print "range:", start, "->", end,
        if end > len(self.hops):
            end = len(self.hops)

        for i in range(start, end + 1):
            oneHopRTT=self.reqid + "," + str(i) + "," + self.hops[i-1] + "," + self.rtts[i-1]
            results.append(oneHopRTT)
        #RTT to destination for completeness
        oneHopRTT = self.reqid + "," + str(end+1) + "," + self.dst + "," + self.rtt
        results.append(oneHopRTT)
        return results
        
    #returns the rtt to the final destination
    def getRTT(self):
        return float(self.rtt)
            
    def getDst(self):
        return self.dst

    def getHops(self):
        return self.hops

    def getRTTs(self):
        return self.rtts

    def getNumHops(self):
        return len(self.hops)

    def add(self, interface_dict):
        for hop in self.hops:
            if interface_dict.has_key(hop):
                interface_dict[hop]+=1
            else:
                interface_dict[hop]=1

    def differAt(self, trace):
        s = self.hops
        t = trace.hops
        diff = []
        l = len(s)
        if (len(t) < l):
            l = len(t)
        for i in range(l):
            if s[i] == '0.0.0.0' or t[i] == '0.0.0.0':
                continue
            if s[i] != t[i]:
                diff.append(i)
        return diff

    def completeStatus(self, prefix):
        for hop in self.hops:
            if prefix.contains(hop):
                self.reached_prefix = True
                return True
        return False

    def EDbyASN(self, t2):
        s = []
        for i in range(len(self.hops)):
            if self.ASNhops[i] == self.ASN:
                s.append(self.hops[i])
        t = []
        for i in range(len(t2.hops)):
            if t2.ASNhops[i] == t2.ASN:
                t.append(t2.hops[i])
        return self.ED(s,t)

    def ED(self, s, t):
#    def ED(self, t):
#        s = self.hops
        m = len(s) + 1
        n = len(t) + 1
        d = []
 
        for i in range(m):
            d.append([])
            for j in range(n):
                d[i].append(0)
        
        for i in range(m):
            d[i][0] = i
        for j in range(n):
            d[0][j] = j

        for j in range(1,n):
            for i in range(1,m):
                if s[i-1] == t[j-1]:
                    d[i][j] = d[i-1][j-1]
                else:
                    r = [ d[i-1][j] + 1, d[i][j-1] + 1, d[i-1][j-1] + 1 ]
                    d[i][j] = min(r)

        #for i in range(m):
        #    for j in range(n):
        #        print d[i][j],
        #    print ""

        #print "Dist:", d[m-1][n-1]
        return d[m-1][n-1]
    
    # conservative ED measure
    def ED2(self, t):
        s = self.hops

        if (len(s) > len(t)):
            tmp = s
            s = t
            t = tmp
        m = len(s) + 1
        n = len(t) + 1
        d = []

        ll = len(s)
        if (len(t) < len(s)):
            ll = len(t)

        for i in range(m):
            d.append([])
            for j in range(n):
                d[i].append(0)
        
        for i in range(m):
            d[i][0] = i
        for j in range(n):
            d[0][j] = j

        mm = 99999
        for j in range(1,n):
            for i in range(1,m):
                if s[i-1] == t[j-1]:
                    d[i][j] = 0
                else:
                    r = [ d[i-1][j] + 1, d[i][j-1] + 1, d[i-1][j-1] + 1 ]
                    d[i][j] = min(r)
            if d[m-1][j] < mm:
                mm = d[m-1][j]

        return mm
    
    def hopASN(self):
        min_asn_hops = 0
        dASNhops = []
        last_hop = ''
        ingress = ''
        b = bgpquery.BGPquery("localhost", 2000)
        b.connect()
        (ip, mask, asn) = b.lookup(self.dst)
        self.ASN = asn
        #print asn
        for hop in self.hops:
            (ip, mask, asn) = b.lookup(hop)
            self.ASNhops.append(asn)
        #    print hop, asn
            if asn == self.ASN:   # checks whether ip belongs to destination ASN
                dASNhops.append(hop)
                if ingress == '' and '*' not in last_hop:
                    ingress = last_hop
            #else:
            last_hop = hop # holds last hop outside the dest. AS.
        l = len(dASNhops)
        if l < min_asn_hops: # at least 1 hop in dASNhops
            xhops = self.hops[-(min_asn_hops+1)-l:-l-1]
            dASNhops = xhops + dASNhops
        del b
        #print self.hops
        #print dASNhops
        return dASNhops, ingress
