#!/usr/bin/env python
import sys
from collections import defaultdict

# Initialize a dictionary to hold counts
counts = defaultdict(lambda: defaultdict(lambda: {"three_attempt": 0, "three_made": 0, "two_attempt": 0, "two_made": 0}))

for line in sys.stdin:
  line = line.strip()
  season, position, stype = line.split('\t')
  if stype == "three_attempt":
    counts[season][position]["three_attempt"] += 1
  elif stype == "three_made":
    counts[season][position]["three_made"] += 1
  elif stype == "two_attempt":
    counts[season][position]["two_attempt"] += 1
  elif stype == "two_made":
    counts[season][position]["two_made"] += 1

# Print header
print("Season\tPosition\tThree Attempts\tThree Made\tTwo Attempts\tTwo Made")

for season in sorted(counts.keys()):
  for position in sorted(counts[season].keys()):
    data = counts[season][position]
    print "{}\t{}\t{}\t{}\t{}\t{}".format(season, position, data["three_attempt"], data["three_made"], data["two_attempt"], data["two_made"])
