import calendar
import datetime
import urllib, urllib2
import time
import os
import dateutil.parse

import simplejson as json


class ExtractSolr(object):
    def __init__(self, config, parsed_arguments):
        self.config = config
        self.parsed_arguments = parsed_arguments
        self.log_id_dict = {}

    def execute(self):
        req = self.rest_api_setup()
       
        start_time = int(time.time())
        json_file = self.retrieve_json(req)
        end_time = int(time.time())

        time_processed = end_time - start_time
        print " --- time processed approximately", str(time_processed), "ms"

        if not json_file:
            raise Exception("No json file found")
       
        self.parse_json(json_file)
   
    def rest_api_setup(self):
        url = "http://{0}:{1}/solr/select/"
        data = {}
        data['wt'] = "json"
        data['rows'] = 10
        data['fl'] = "*,score"
        data['start'] = 0
        data['version'] = '2.2'
        data['indent'] = 'on'
        data['q'] = 'time:[{0} TO {1}]'

        _data = urllib.urlencode(data)

        _url = url + "?" + _data

        req = urllib2.Request(url=url, data=data)
        return req

    def retrieve_json(self, req):
        try:
            res = urllib2.urlopen(req)
        except urllib2.HTTPError, e:
            print self._error_msg_format("URLError occurred, reason: %s" % e.read()) 
            return None 
        except urllib2.URLError, e:
            print self._error_msg_format("URLError occurred, reason: %s" % e.reason)
            return None
        except Exception, e:
            print self._error_msg_format("General Exception occurred, reason: %s" % e)
            return None

    def parse_json(self, json_file):
        raw_msgs = json_file['response']['docs']
        data_directory = "data_files/"
        for raw_msg in raw_msgs:
            ts_dt = dateutil.parser.parse(raw_msg['time'])
            ts = int(time.mktime(ts_dt.timetuple()))
            encoded_ts = self._encoded_timestamp(ts)
            filename = "{0}_{1}_{2}_{3}_{4}.csv".format(raw_msg['tenant_id'],
                                raw_msg['host_name'], raw_msg['host_id'],
                                raw_msg['log_id'], encoded_ts)
            file_path = data_directory + encoded_ts + "/" filename

            # Checks if the directory exists, if not then create it..
            if not os.path.exists(data_directory + encoded_ts):
                os.makedirs(data_directory + encoded_ts)
           
            # Checks if the file exists
            if not os.path.isfile(file_path):
                ff = open(file_path, "w")
                ff.write('time, msg_num, severity, message, dyn_headers')
            else:
                ff = open(file_path, "a")
            
            row_line = "{0}, {1}, {2}, \"{3}\", \"{4}\"".format(
                        raw_msg.time, raw_msg.msg_num, raw_msg.severity, 
                        raw_msg.message.replace("\"", "\"\""), "")
            ff.write(row_line)

            ff.close()
   
    def _encoded_timestamp(self, timestamp):
        
        return None 

    def _error_msg_format(self, msg):
        return " " * 5, "*" * 5, msg
