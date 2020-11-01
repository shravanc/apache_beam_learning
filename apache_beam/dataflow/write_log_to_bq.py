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



class Utility:

    def __init__(self):

        self.table_spec = bigquery.TableReference(
            projectId='justlikethat-294122',
            datasetId='log_analysis',
            tableId='quotes'
        )

        self.schema = 'source:STRING,  quote:STRING'

    def print_me(self, data):
        print("Printing it here ---->", data)

    def parse(self, line):
        return {'source': 'Shravan C', 'quote':'my quote'}
        #print("*******LINE********", line)
        """
        return beam.Create([
            {
                'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
            }
        ])

        return [
            {'source': 'Shravan C', 'quote': 'Give life, a life'},
            {'source': 'Pooran C', 'quote': 'E preeti yake bhoomi melide'}
        ]
        """

    def convert(self, data):
        #print("JSON---->", data)
        return beam.Create(data)




def run(argv=None, save_main_session=True):
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

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    utility = Utility()
    #"""
    quotes = (
        p
        | 'Read' >> ReadFromText(known_args.input)
        | 'Parse Log' >> beam.Map(lambda line: utility.parse(line))
    )

    #data | "Output" >> WriteToText(known_args.output)
    """
    quotes = p | beam.Create([
      {
        'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'
      },
      {
        'source': 'Mahatma Gandhi1', 'quote': 'My life is my message.1'
      }
    ])
    """

    table_spec = bigquery.TableReference(
    projectId='justlikethat-294122',
    datasetId='log_analysis',
    tableId='quotes')

    """
    table_schema = 'source:STRING,  quote:STRING'
    quotes | beam.io.WriteToBigQuery(
        table_spec,
        table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
    """
    quotes | "Output" >> WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()

