import os
import argparse
import csv

YEARS = ["2011", "2012"]

def main(sessions_list_folder, raw_data_folder, output_data_folder):
  if not os.path.isdir(sessions_list_folder):
    print "Invalid sessions list folder."
    return

  if not os.path.isdir(output_data_folder):
    print "Invalid output folder."
    return

  if not os.path.isdir(raw_data_folder):
    print "Invalid raw data folder."
    return

  for y in YEARS:
    y_path = os.path.join(sessions_list_folder, y)

    if not os.path.isdir(y_path):
      print "Year folder does not exist."
      return

    for f_path in os.listdir(y_path):
      raw_file_name = f_path[1:]

      session_full_path = os.path.join(y_path, f_path)
      raw_file_full_path = os.path.join(raw_data_folder, raw_file_name)
      output_file_full_path = os.path.join(output_data_folder, raw_file_name)
      print "Processing file " + raw_file_full_path +  " (sessions source: " + session_full_path + ", output: "+ output_file_full_path + ")"

      lookup = set()
      print type(lookup)
      with open(session_full_path, 'rb') as csvfile:
        session_reader = csv.reader(csvfile, delimiter='\t')
        for row in session_reader:
          if not row[0] in lookup:
            lookup.add(row[0])

      with open(session_full_path, 'rb') as csvfile:
        raw_reader = csv.reader(csvfile, delimiter='\t')
        with open(output_file_full_path, 'wb') as csvfile:
          filtered_writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
          for row in raw_reader:
            if row[0] in lookup:
              filtered_writer.writerow(row)
            else:
              print "Skipping row[0]"

      print len(lookup)
 
if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Filter raw data and keep only sessions from the sessions list.')
  parser.add_argument('--debug', default=False, help='a boolean denoting the debug mode')
  parser.add_argument('sessions', default=False, help='path to the sessions list folder')
  parser.add_argument('raw', default=False, help='path to the raw files folder')
  parser.add_argument('output', default=False, help='path to the output files folder')
  args = parser.parse_args()
  main(args.sessions, args.raw, args.output)
