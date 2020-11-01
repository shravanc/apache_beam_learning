from __future__ import absolute_import
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

"""
class data_ingestion:

    def parse_method(self, x):
        x = x.replace('"', '')
        data = x.split(' ')
        date = data[2].split('[')[-1]
        offset = data[3].split(']')[0]
        valid_data =[
            data[1], # bucket_name
            f"{date} {offset}",
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

        row = dict(
                zip(('bucket_name', 'date', 'operation', 'key', 'request_uri', 'http_status', 'error_code', 'bytes_sent', 'object_size', 'total_time', 'turnaround_time', 'referrer', 'user_agent', 'request_header' ), valid_data))
        return row
"""

class CustomPipelineOptions(PipelineOptions):
    """
    Runtime Parameters given during template execution
    path and organization parameters are necessary for execution of pipeline
    campaign is optional for committing to bigquery
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--path',
            type=str,
            default='gs://dataflow_learning/tmp/2020-10-02-11-34-19-EA6C5E314B70B157',
            help='Path of the file to read from')
        parser.add_value_provider_argument(
            '--output',
            type=str,
            default='evident-quasar-268120:dataflow_exp.file_access',
            help='Output file if needed')

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    global cloud_options
    global custom_options

    SCHEMA = 'bucket:string,date:datetime,operation:string,key:string,request_uri:string,http_status:string,error_code:string,bytes_sent:string,total_time:string,turnaround_time:string,referrer:string,user_agent:string,request_header:string'


    pipeline_options = PipelineOptions(pipeline_args)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)
    def parse_method(x):
        x = x.replace('"', '')
        data = x.split(' ')
        date = data[2].split('[')[-1]
        offset = data[3].split(']')[0]
        valid_data =[
            data[1], # bucket_name
            f"{date} {offset}",
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

        row = dict(
                zip(('bucket_name', 'date', 'operation', 'key', 'request_uri', 'http_status', 'error_code', 'bytes_sent', 'object_size', 'total_time', 'turnaround_time', 'referrer', 'user_agent', 'request_header' ), valid_data))
        return row

    init_data = (p
                        | 'Read from a file' >> beam.io.ReadFromText(custom_options.path)
                        | 'String To BigQuery Row' >> beam.Map(lambda s: parse_method(s))
                        | 'Write To BigQuery' >> beam.io.Write(
                            beam.io.BigQuerySink(
                            'dataflow_exp.file_access',
                            schema=SCHEMA,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                            ))
                )


    result = p.run()
    # result.wait_until_finish

if __name__ == '__main__':
    run()


# python template.py --runner DataflowRunner --project $PROJECT --staging_location gs://$BUCKET/staging --temp_location gs://$BUCKET/temp --
# template_location gs://$BUCKET/templates/$TemplateName
