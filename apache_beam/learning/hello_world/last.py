import argparse
import logging
import re
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

class dataingestion:

    def parse_method(self, string_input):
        return {'source': 'Shravan C', 'quote':'my quote'}


def run():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',dest='input',
            required=False,
            help='Input file to read. This can be a local file or '
                 'a file in a Google Storage Bucket.',
            default='/home/shravan/Desktop/gcp_files/2020-10-02-11-34-19-EA6C5E314B70B157')
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='log_analysis.quotes')
    parser.add_argument('--project', dest='project', required=False, help='Project name', default='justlikethat-294122',
                        action="store")
    parser.add_argument('--bucket_name', dest='bucket_name', required=False, help='bucket name',
                        default='justlikethat-294122')
    parser.add_argument('--runner', dest='runner', required=False, help='Runner Name', default='DataflowRunner',
                        action="store")
    parser.add_argument('--jobname', dest='job_name', required=False, help='jobName', default='dataflowtest',
                        action="store")
    parser.add_argument('--staging_location', dest='staging_location', required=False, help='staging_location',
                        default='gs://justlikethat-294122/staging')
    parser.add_argument('--region', dest='region', required=False, help='Region', default='europe-west1',
                        action="store")
    parser.add_argument('--temp_location', dest='temp_location', required=False, help='temp location',
                        default='gs://justlikethat-294122/temp/')
    parser.add_argument('--template_name', dest='template_name', required=False, help='template name',
                        default='example-template')


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
    pipeline_options_val = PipelineOptions.from_dictionary(pipeline_options)
    p = beam.Pipeline(options=pipeline_options_val)
    data_ingestion = dataingestion()

    (p | 'Read from a File' >> beam.io.ReadFromText(pipeline_options["input"], skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s)) |
     'Write to BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    pipeline_options["output"],
                    schema='source:STRING,  quote:STRING',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    #p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
