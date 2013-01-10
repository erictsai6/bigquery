import calendar
import datetime
import urllib, urllib2
import time

import json


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

        begin_dt = self._isoformat_timestamp(self.parsed_arguments.begin)
        end_dt = self._isoformat_timestamp(self.parsed_arguments.end)

        url = "http://{0}:{1}/solr/select/".format(self.config.SOLR_SERVER_HOST, self.config.SOLR_SERVER_PORT)
        data = {}
        data['wt'] = "json"
        data['rows'] = 10
        data['fl'] = "*,score"
        data['start'] = 0
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
        print "ASDF"
        for raw_msg in raw_msgs:
            print raw_msg
            encoded_ts = self._encoded_timestamp(raw_msg['time'])
            filename = "{0}_{1}_{2}_{3}_{4}.csv".format(raw_msg['tenant_id'],
                                raw_msg['host_name'], raw_msg['host_id'],
                                raw_msg['log_id'], raw_msg['time'])

            pass
   
    def _encoded_timestamp(self, timestamp):
        
        return None 

    def _isoformat_timestamp(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        dt_str = dt.isoformat()
        print dt_str
        if '.' in dt_str:
            dt_str = dt_str[0:dt_str.index('.')]
        dt_str += "Z"
        return dt_str

    def _error_msg_format(self, msg):
        return " " * 5, "*" * 5, msg
