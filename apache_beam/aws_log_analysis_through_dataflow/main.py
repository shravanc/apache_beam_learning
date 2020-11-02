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

        self.schema = 'bucket:string,operation:string,key:string,request_uri:string\
                      ,http_status:string,error_code:string,bytes_sent:string,object_size:string,\
                      total_time:string,turnaround_time:string,referrer:string,user_agent:string,\
                      request_header:string'

    def parse(self, x):
        x = x.replace('"', '')
        data = x.split(' ')
        date = data[2].split('[')[-1]
        offset = data[3].split(']')[0]
        valid_data = [
            data[1], # bucket_name
            #f"{date} {offset}",
            data[7],    # operation
            data[8],    # Key
            data[9],    # request_uri
            data[10],   # http status
            data[11],   # error_code
            data[12],   # bytes_sent
            data[13],   # object_size
            data[14],   # total_time
            data[15],   # turn_aroundtime
            data[16],   # referrer
            data[17],   # user_agent
            data[26],   # request_header
        ]

        row = dict(zip(('bucket', 'operation', 'key', 'request_uri', 'http_status', \
                    'error_code', 'bytes_sent', 'object_size', 'total_time', 'turnaround_time',\
                    'referrer', 'user_agent', 'request_header'), valid_data))
        return row


class Utility:


    def print_me(self, data):
        print("Printing it here ---->", data)

    def parse(self, line):
        return {'source':'Shravan C','quote':'my quote'}


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://justlikethat-294122/sample.txt',
        help='Input file to process.')


    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        utility = AwsLogParser()
        quotes = (
                p
                | 'Read' >> ReadFromText(known_args.input)
                | 'Parse Log' >> beam.Map(lambda line: utility.parse(line))
        )

        quotes | beam.io.gcp.bigquery.WriteToBigQuery(
            utility.table_spec,
            schema=utility.schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()



