#!/usr/bin/env python

import math

def classification(n):
  base = math.floor(math.log(n)/math.log(10)) - 1
  relative = n / math.pow(10, base)
  order = min(2, math.floor(relative/25))
  if base < 0:
    base = 0
  return base * 3 + order

for n in range(24,1001):
  print n, classification(n)
