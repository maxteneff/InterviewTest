#!/usr/bin/python

import time
import datetime
import random
import socket

ips=["123.221.14.56", "16.180.70.237", "10.182.189.79", "218.193.16.244", "198.122.118.164", "114.214.178.92", "233.192.62.103", "244.157.45.12", "81.73.150.239", "237.43.24.118"]
hosts=["host-1", "host-2", "host-3", "host-4", "host-5", "host-6", "host-7", "host-8"]
uris=["/handle-bars", "/stems", "/wheelsets", "/forks", "/seatposts", "/saddles", "/shifters", "/store"]
codes = [100, 101, 102, 200, 201, 202, 300, 301, 302, 400, 404, 500, 502]
otime = datetime.datetime(2013, 10, 10)

while True:
	increment = datetime.timedelta(seconds=random.randint(30, 300))
	otime += increment
	uri = random.choice(uris)
	host = random.choice(hosts)
	ip = random.choice(ips)
	code = random.choice(codes)
	timestamp = otime.strftime('%Y-%m-%dT%H:%M:%S.000')
	str = '%s %s %s "%s" %s' % (host, ip, timestamp, uri, code)
	print (str)
	time.sleep (1)