import time
import pandas as pd

# Specify the input file name
input_file = 'NBA_2004_2024_Shots.csv'  # Change this to your actual file name

# Read the CSV file into a DataFrame
df = pd.read_csv(input_file)

# Counting attempts and makes
map_results = []
start_map_time = time.time()

for index, row in df.iterrows():
    season = row[0]  # season is the first column (2004-2024)
    shot_type = row[15]  # shot type is the 15th column (2PT/ 3PT)
    shot_made = row[13]  # SHOT_MADE is the 13th column (TRUE/FALSE)
    position = row[6]  # position is the 6th column (C,F,G,NA)
    if shot_type == '3PT Field Goal':
        # Count three-point attempts
        map_results.append((season, position, 'three', shot_made))
    elif shot_type == '2PT Field Goal':
        # Count two-point attempts
        map_results.append((season, position, 'two', shot_made))
end_map_time = time.time()

#Reduce
reduce_results = {}
start_reduce_time = time.time()
for season, position, shot_type, is_made in map_results:
  key = (season, position)
  if key not in reduce_results:
    reduce_results[key] = {
      'Three Attempts': 0,
      'Three Made': 0,
      'Two Attempts': 0,
      'Two Made': 0
    }
  if shot_type == 'three':
    reduce_results[key]['Three Attempts'] += 1
    if is_made:
      reduce_results[key]['Three Made'] += 1
  elif shot_type == 'two':
    reduce_results[key]['Two Attempts'] += 1
    if is_made:
      reduce_results[key]['Two Made'] += 1
end_reduce_time = time.time()

# Convert the results into a DataFrame
result = pd.DataFrame.from_dict(reduce_results, orient='index')
result = result.astype(int)
# Sort by season and position
result.index = pd.MultiIndex.from_tuples(result.index, names=['Season', 'Position'])  # Create a MultiIndex
result = result.sort_index()

# Set pandas option to display all rows
pd.set_option('display.max_rows', None)

map_time = end_map_time - start_map_time
reduce_time = end_reduce_time - start_reduce_time
print("Map Time: {:.2f} sec".format(map_time))
print("Reduce Time: {:.2f} sec".format(reduce_time))

# Output the result
print("Season\tPosition\tThree Attempts\tThree Made\tTwo Attempts\tTwo Made")
for idx, row in result.iterrows():
    season, position = idx
    # Format the output with specified spacing
    print "{}\t{}\t{}\t{}\t{}\t{}".format(season, position, row["Three Attempts"], row["Three Made"], row["Two Attempts"], row["Two Made"])
