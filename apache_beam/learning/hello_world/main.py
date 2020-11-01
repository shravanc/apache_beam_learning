from __future__ import absolute_import

import argparse
import logging
import re

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
      default='/tmp/logs/2020-10-02-11-34-19-EA6C5E314B70B157',
      #default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      # CHANGE 1/6: The Google Cloud Storage path is required
      # for outputting the results.
      default='/tmp/logs/output.txt',
      #default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      # CHANGE 2/6: (OPTIONAL) Change this to DataflowRunner to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--runner=DirectRunner',
      # CHANGE 3/6: (OPTIONAL) Your project ID is required in order to
      # run your pipeline on the Google Cloud Dataflow Service.
      '--project=SET_YOUR_PROJECT_ID_HERE',
      # CHANGE 4/6: (OPTIONAL) The Google Cloud region (e.g. us-central1)
      # is required in order to run your pipeline on the Google Cloud
      # Dataflow Service.
      '--region=SET_REGION_HERE',
      # CHANGE 5/6: Your Google Cloud Storage path is required for staging local
      # files.
      '--staging_location=/tmp/logs/2020-10-02-11-34-19-EA6C5E314B70B157', #gs://YOUR_BUCKET_NAME/AND_STAGING_DIRECTORY',
      # CHANGE 6/6: Your Google Cloud Storage path is required for temporary
      # files.
      '--temp_location=/tmp', #gs://YOUR_BUCKET_NAME/AND_TEMP_DIRECTORY',
      '--job_name=your-wordcount-job',
  ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the mtext filettern] into a PCollection.
    lines = p | ReadFromText(known_args.input)

    # Count the occurrences of each word.

    def split_me(x):
        print("--->", x)
        x = x.replace('"', '')
        data = x.split(' ')
        print("--->", data)
        print("***", data[2])
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

    splits = (
        lines | 'Split' >> beam.Map(split_me)
    )

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    #output = counts | 'Format' >> beam.Map(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    splits | WriteToText(known_args.output)
    #output | WriteToText(known_args.output)

import os
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
  os.system("cat ./output*")


