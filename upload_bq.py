"""
   Upload to Google BigQuery Script

   Sharded process by T_ID.  Allows multiple upload_bq scripts to work on the 
   same directory of files, necessary since the BigQuery's commandline is kind
   of slow and I don't have all day.
"""

import config
import sys
import os
import time
import subprocess

import argparse, sys
parser = argparse.ArgumentParser()
parser.add_argument("--date", help="YYYYMMDD syntax or enter 'all' to process all the directories", required=True)
parser.add_argument("--shard", type=int, required=True)
parsed_arguments = parser.parse_args(sys.argv[1:])

MAX_SHARD = 5 

def main():
    main_directory = "data_files/"

    if parsed_arguments.date == "all": 

        subdirs = os.listdir(main_directory)
        for subdir in subdirs:
            _subdir = os.path.join(main_directory, subdir)
            if os.path.isdir(_subdir):
                iterate_thru_files(_subdir)
    else:
        _subdir = os.path.join(main_directory, parsed_arguments.date)
        iterate_thru_files(_subdir)

def iterate_thru_files(subdir):
    csv_files = os.listdir(subdir)
    for csv_file in csv_files:
        _csv_file = os.path.join(subdir, csv_file)
        table_name = csv_file.replace(".csv", "")
        try:
            # upload this file to google big query
            if os.path.isfile(_csv_file):
                t_id = csv_file[0:csv_file.index('_')]
                if int(t_id) % MAX_SHARD == parsed_arguments.shard:
                    begin_time = int(time.time())       
                    upload_csv_file(table_name, _csv_file)
                    end_time = int(time.time())

                    time_processed = end_time - begin_time
                    print " Processed", csv_file, "in", str(time_processed), "seconds"
        except Exception, e:
            print "Failure to upload to Google Big Query, reason:", e

def upload_csv_file(table_name, file_path):
    bq_commandline = "bq load nydev.%s %s time:INTEGER,msg_num:INTEGER,severity:INTEGER,message:STRING,dyn_headers:STRING" % (table_name, file_path)
    print bq_commandline
    subprocess.call(bq_commandline, shell=True)

if __name__ == "__main__":
    print "#" * 30
    print "Initialized Import to Google BigQuery Script"
    print "#" * 30
    
    try:
        main()
    except Exception, e:
        print "   **** General Exception hit, reason:", e

    print "#" * 30
    print "Exiting..."
    print "#" * 30
    sys.exit(0)
