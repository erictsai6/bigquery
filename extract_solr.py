import calendar
import datetime
import urllib, urllib2
import time

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
        for raw_msg in raw_msgs:
            encoded_ts = self._encoded_timestamp(raw_msg['time'])
            filename = "{0}_{1}_{2}_{3}_{4}.csv".format(raw_msg['tenant_id'],
                                raw_msg['host_name'], raw_msg['host_id'],
                                raw_msg['log_id'], raw_msg['time'])

            pass
   
    def _encoded_timestamp(self, timestamp):
        
        return None 

    def _error_msg_format(self, msg):
        return " " * 5, "*" * 5, msg
