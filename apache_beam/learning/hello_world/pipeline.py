from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://dataflow_learning/tmp/2020-10-02-11-34-19-EA6C5E314B70B157',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='gs://dataflow_learning/output.txt',
      help='Output file to write results to.')
  parser.add_argument('--project', dest='project', required=False, help='Project name', default='evident-quasar-268120',
                    action="store")
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
        '--runner=Dataflow',
        '--region=europe-west1',
        '--staging_location=gs://dataflow_learning/tmp',
        '--temp_location=gs://dataflow_learning/tmp',
        '--job_name=wordcount'
  ])

  args = parser.parse_args()
  pipeline_options = {
    'project': args.project,
    'staging_location': args.staging_location,
    'runner': args.runner,
    'job_name': args.job_name,
    'region': args.region,
    'output': args.output,
    'input': args.input,
    'temp_location': args.temp_location,
    'template_location': 'gs://' + args.bucket_name + '/templates/' + args.template_name}
  print(pipeline_options)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    def split_me(x):
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
        return valid_data#x.split(' ')

    splits = (lines | 'Split' >> beam.Map(split_me))

    SCHEMA = 'bucket:string,date:datetime,operation:string,key:string,request_uri:string,http_status:string,error_code:string,bytes_sent:string,total_time:string,turnaround_time:string,referrer:string,user_agent:string,request_header:string'
    table_spec = 'evident-quasar-268120:dataflow_exp.file_access'
    splits | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           table_spec,
           schema=SCHEMA)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()


