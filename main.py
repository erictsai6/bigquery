import config
import sys
from extract_solr import ExtractSolr

import argparse, sys
parser = argparse.ArgumentParser()
parser.add_argument("--begin", type=int, help='Begin Time in seconds', required=True)
parser.add_argument("--end", type=int, help='End Time in seconds', required=True)
parsed_arguments = parser.parse_args(sys.argv[1:])


def main():
    extractor = ExtractSolr(config, parsed_arguments)
    try:
        extractor.execute()
    except Exception, e:
        print " " * 10, "Failure inside execute loop", e
        sys.exit(1)

if __name__ == "__main__":
    print "#" * 30
    print "Initialized Extract Solr JSON Script"
    print "#" * 30

    main()

    print "#" * 30
    print "Exiting..."
    print "#" * 30
    sys.exit(0)
