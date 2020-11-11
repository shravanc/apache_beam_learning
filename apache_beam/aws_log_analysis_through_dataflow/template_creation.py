from __future__ import absolute_import

import argparse
import logging
from datetime import datetime

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
            datasetId='medium_dataset',
            tableId='acces'
        )

        self.schema = 'request_id:string,bucket:string,date:datetime,datetime:datetime,remote_ip:string,\
                      operation:string,key:string,request_uri:string,http_status:string,error_code:string,\
                      bytes_sent:string,object_size:string,total_time:string,turn_aroundtime:string,\
                      referrer:string,category:string,html_name:string,file_name:string'

        self.log = {
            'request_id': '',
            'bucket': '',
            'date': '',
            'remote_ip': '',
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
            'category': '',
            'html_name': '',
            'file_name': ''
        }


    def parse(self, line):
        from datetime import datetime

        data = line.split(' ')
        self.log['request_id']           = data[6]
        self.log['bucket']               = data[1]
        self.log['date']                 = datetime.strptime(data[2].split(':')[0], "[%d/%b/%Y")
        self.log['datetime']             = datetime.strptime(data[2], "[%d/%b/%Y:%H:%M:%S")
        self.log['remote_ip']            = data[4]
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
        if 'html' in data[10]:
            url_ele = data[10].split('/')
            self.log['category'] = url_ele[-2]
            self.log['html_name'] = url_ele[-1].split('?')[0]
            self.log['file_name'] = self.log['html_name'].split('.html')[0]
        
        return self.log

class LogParserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input',
          default='gs://justlikethat-294122/aws_logs/2020-10-19-17-27-28-AB0B7526FB2BE77A',
        #   default='/home/shravan/Desktop/gcp_files/sample_1.txt',
          help='Path of the file to read from')


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_topic',
        dest='input_topic',
        default='let_me_know',
        help='Input file to process.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    parser_args = pipeline_options.view_as(LogParserOptions) #.save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        print("Input------->", parser_args.input)
        obj = AwsLogParser()
        quotes = (
                p
                | 'Read' >> ReadFromText(parser_args.input)
                | 'Parse Log' >> beam.Map(lambda line: obj.parse(line))
        )

        quotes | beam.io.gcp.bigquery.WriteToBigQuery(
            obj.table_spec,
            schema=obj.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

 



