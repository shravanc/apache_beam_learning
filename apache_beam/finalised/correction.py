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

        utility = Utility()
        quotes = (
                p
                | 'Read' >> ReadFromText(known_args.input)
                | 'Parse Log' >> beam.Map(lambda line: utility.parse(line))
        )
        table_spec = bigquery.TableReference(
            projectId='justlikethat-294122',
            datasetId='mydataset',
            tableId='quotes'
        )
        table_schema='source:STRING,quote:STRING'

        quotes | beam.io.gcp.bigquery.WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()



