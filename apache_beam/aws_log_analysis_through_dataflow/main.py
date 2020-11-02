from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

class AwsLogParser:

    def __init__(self):

        self.table_spec = bigquery.TableReference(
            projectId='justlikethat-294122',
            datasetId='s3_logs',
            tableId='access_report'
        )

        self.schema = 'bucket:string,date:string,remote_ip:string,operation:string,\
                      key:string,request_uri:string,http_status:string,error_code:string,\
                      bytes_sent:string,object_size:string,total_time:string,turn_aroundtime:string,\
                      referrer:string'

        self.log = {
            # 'bucket_owner': '',
            'bucket': '',
            'date': '',
            #'time_offset': '',
            'remote_ip': '',
            #'request_arn': '',
            #'request_id': '',
            'operation': '',
            'key': '',
            'request_uri': '',
            'http_status': '',
            'error_code': '',
            'bytes_sent': '',
            'object_size': '',
            'total_time': '',
            'turn_aroundtime': '',
            'referrer': '',
            #'user_agent': '',
            #'version_id': '',
            #'host_id': '',
            #'signature_version': '',
            #'cipher_suite': '',
            #'authentication_type': '',
            #'host_header': '',
            #'tls_version': ''
        }


    def parse(self, line):
        data = line.split(' ')

        #self.log['bucket_owner']         = data[0]
        self.log['bucket']               = data[1]
        self.log['date']                 = data[2].split('[')[-1]
        #self.log['time_offset']          = data[3]
        self.log['remote_ip']            = data[4]
        #self.log['request_arn']          = data[5]
        #self.log['request_id']           = data[6]
        self.log['operation']            = data[7]
        self.log['key']                  = data[8]
        self.log['request_uri']          = data[9] + ' ' + data[10] + ' ' + data[11]
        self.log['http_status']          = data[12]
        self.log['error_code']           = data[13]
        self.log['bytes_sent']           = data[14]
        self.log['object_size']          = data[15]
        self.log['total_time']           = data[16]
        self.log['turn_aroundtime']      = data[17]
        self.log['referrer']             = data[18]
        """
        index = 19
        ua = ""
        count = 0
        while(count < 2):
            print(index, '--->', data[index])
            if '"' in data[index]:
                count += 1
            ua += data[index] + " "
            index += 1
        self.log['user_agent'] = ua
        """
        """
        self.log['user_agent']           = data[19]
        self.log['version_id']           = data[20]
        self.log['host_id']              = data[21]
        self.log['signature_version']    = data[22]
        self.log['cipher_suite']         = data[23]
        self.log['authentication_type']  = data[24]
        self.log['host_header']          = data[25]
        self.log['tls_version']          = data[26]
        """
        return self.log



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://justlikethat-294122/aws_logs/2020*',
        #default='/home/shravan/Desktop/gcp_files/sample.txt',
        help='Input file to process.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        obj = AwsLogParser()
        quotes = (
                p
                | 'Read' >> ReadFromText(known_args.input)
                | 'Parse Log' >> beam.Map(lambda line: obj.parse(line))
        )

        #quotes | 'Write' >> WriteToText('output')
        quotes | beam.io.gcp.bigquery.WriteToBigQuery(
            obj.table_spec,
            schema=obj.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()



