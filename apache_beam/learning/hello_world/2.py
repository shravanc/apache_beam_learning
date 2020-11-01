from __future__ import absolute_import
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, SetupOptions

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
            default='gs://dataflow_learning/output.txt',
            help='Output file if needed')

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    global cloud_options
    global custom_options

    pipeline_options = PipelineOptions(pipeline_args)
    cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)

    init_data = (p
                        | 'Hello World' >> beam.Create(['Hello World'])
                        | 'Writing to gs' >> beam.io.WriteToText(custom_options.output)
                 )

    result = p.run()
    # result.wait_until_finish

if __name__ == '__main__':
    run()


# python template.py --runner DataflowRunner --project $PROJECT --staging_location gs://$BUCKET/staging --temp_location gs://$BUCKET/temp --
# template_location gs://$BUCKET/templates/$TemplateName