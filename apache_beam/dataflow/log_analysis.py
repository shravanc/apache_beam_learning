#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

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
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.io.gcp.internal.clients import bigquery


class AwsLogParser(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, x):
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
      return [row]


def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='/home/shravan/Desktop/gcp_files/2020-10-02-11-34-19-EA6C5E314B70B157',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=False,
      default='output',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    bq_details = "justlikethat-294122:log_analysis.aws_log"
    SCHEMA = "bucket:string,date:datetime,operation:string,key:string,request_uri:string,http_status:string,error_code:string,bytes_sent:string,total_time:string,turnaround_time:string,referrer:string,user_agent:string,request_header:string"
    log_parser = AwsLogParser()
    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(known_args.input)

    data = (
        lines
        | 'Parse AWS Log File' >> beam.Map(lambda log: log_parser.process(log))
    )


    table_spec = bigquery.TableReference(
    projectId='justlikethat-294122',
    datasetId='log_analysis',
    tableId='aws_log_test_1')


    data | beam.io.WriteToBigQuery(
    table_spec,
    schema=SCHEMA,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    #data | 'Write' >> WriteToText(known_args.output)
    """
    counts = (
        lines
        | 'Split' >>
        (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
        | 'Handle lower case' >> beam.Map(lambda word: str.lower(word))
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))


    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(known_args.output)
    """


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
