#! /usr/bin/env python

from __future__ import print_function
import sys
from random import randrange

st = ''
for line in sys.stdin:
    line = line.strip()
    st = st + line + ','

st = st[:-1]
id_list = st.split(',')

while len(id_list) > 0:
    r = randrange(start=1, stop=6, step=1)
    st = ''
    counter = 0
    for id_cur in id_list:
        st = st + "%s," % id_cur
        id_list.remove(id_cur)
        counter += 1
        if counter == r:
            break
    st = st[:-1]
    print(st)
