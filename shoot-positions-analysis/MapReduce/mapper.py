#!/usr/bin/env python
import sys
import csv

for line in sys.stdin:
  line = line.strip()
  try:
    data = next(csv.reader([line]))
    season = data[0]  # season is the first column (2004-2024)
    shot_type = data[15]  # shot type is the 15th column (2PT/ 3PT)
    shot_made = data[13]  # SHOT_MADE is the 13th column (TRUE/FALSE)
    position = data[6]  #position is the 6th column (C,F,G,NA)

    # Check for 3PT shot made
    if shot_type == '3PT Field Goal':
      #3pt shot attempt
      print("{}\t{}\t{}".format(season, position, "three_attempt"))

      if shot_made == 'TRUE':
        print("{}\t{}\t{}".format(season, position, "three_made"))
    
    elif shot_type == '2PT Field Goal':
      #2pt shot attempt
      print("{}\t{}\t{}".format(season,position, "two_attempt"))

      if shot_made == 'TRUE':
        print("{}\t{}\t{}".format(season,position, "two_made"))

  except Exception as e:
    continue
