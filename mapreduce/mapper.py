#! /usr/bin/env python

import sys
from random import random

id_list = []
for line in sys.stdin:
    line.strip()
    id_list.append((random(), line))

for id_cur in sorted(id_list):
    print '%s' % (id_cur[1])
