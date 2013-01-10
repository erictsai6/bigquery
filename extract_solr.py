import calendar
import datetime
import urllib, urllib2
import time
import os
import dateutil.parser
import re
import sys

import json

REQUEST_LIMIT = 250000

class ExtractSolr(object):
    def __init__(self, config, parsed_arguments):
        self.config = config
        self.parsed_arguments = parsed_arguments
        self.log_id_set = set() 

    def execute(self):
        # total_req
        total_req_num = 1000000 
        
        f_metrics = open("metrics.txt", "w")

        for request_start in range(0, total_req_num, REQUEST_LIMIT):
            delta_start = total_req_num - request_start
            if delta_start > REQUEST_LIMIT:
                delta_start = REQUEST_LIMIT 
            req = self.rest_api_setup(request_start, delta_start)

            start_time = int(time.time())
            json_file = self.retrieve_json(req)
            end_time = int(time.time())

            time_processed = end_time - start_time
            print " --- time processed approximately", str(time_processed), "s"

            if not json_file:
                raise Exception("No json file found")
             
            f_metrics.write("request_start-%s, file_size-%s bytes, time_processed-%s s\n" %
                        (request_start, sys.getsizeof(json_file), time_processed))
           
            self.parse_json(json_file)
        
        f_metrics.close()
   
    def rest_api_setup(self, start, num_request):

        begin_dt = self._isoformat_timestamp(self.parsed_arguments.begin)
        end_dt = self._isoformat_timestamp(self.parsed_arguments.end)

        url = "http://{0}:{1}/solr/select/".format(self.config.SOLR_SERVER_HOST, self.config.SOLR_SERVER_PORT)
        
        data = {}
        data['wt'] = "json"
        data['rows'] = num_request 
        data['fl'] = "*,score"
        data['start'] = start 
        data['version'] = '2.2'
        data['indent'] = 'on'
        data['q'] = 'time:[{0} TO {1}]'.format(begin_dt, end_dt)

        _data = urllib.urlencode(data)

        _url = url + "?" + _data
        print _url

        req = urllib2.Request(url=_url)
        return req

    def retrieve_json(self, req):
        try:
            res = urllib2.urlopen(req)
            return json.loads(res.read())
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
        cnt = 0
        for raw_msg in raw_msgs:
            ts_dt = dateutil.parser.parse(raw_msg['time'])
            ts = int(time.mktime(ts_dt.timetuple()))
            encoded_ts = self._encoded_timestamp(ts_dt)
            filename = "{0}_{1}_{2}_{3}_{4}.csv".format(raw_msg['tenant_id'],
                                raw_msg['host_name'], raw_msg['host_id'],
                                raw_msg['log_id'], encoded_ts)

            file_path = data_directory + encoded_ts + "/" + filename

            # Checks if the directory exists, if not then create it..
            if not os.path.exists(data_directory + encoded_ts):
                os.makedirs(data_directory + encoded_ts)
           
            # Checks if the file exists
            if not os.path.isfile(file_path):
                ff = open(file_path, "w")
                #ff.write('time, msg_num, severity, message, dyn_headers')
            else:
                ff = open(file_path, "a+")
           
            if filename not in self.log_id_set:
                print filename, "successfully added"
                self.log_id_set.add(filename)
           
            try:
                row_line = "{0}, {1}, {2}, \"{3}\", \"{4}\"\n".format(
                        ts, raw_msg['msg_num'], raw_msg['severity'], 
                        self._modify_log_msg(raw_msg['message']), "")
            except Exception, e:
                print self._error_msg_format("exception parsing the log message, reason: %s" % e)
                ff.close()
                continue
            ff.write(row_line)

            ff.close()
            cnt += 1
            if cnt % 1000 == 0:
                print "  ", cnt, "log_msg processed"
   
    def _encoded_timestamp(self, ts_dt):
        return "%04d%02d%02d" % (ts_dt.year, ts_dt.month, ts_dt.day)

    def _isoformat_timestamp(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        dt_str = dt.isoformat()
        print dt_str
        if '.' in dt_str:
            dt_str = dt_str[0:dt_str.index('.')]
        dt_str += "Z"
        return dt_str

    def _modify_log_msg(self, log_msg):
        _log_msg = log_msg.replace("\"", "\"\"")
        _log_msg = _log_msg.replace("\n", "")
        return _log_msg

    def _error_msg_format(self, msg):
        return "%s %s %s" % (" " * 5, "*" * 5, msg)
