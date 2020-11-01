import argparse
import logging
import re
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam

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


def run():

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',dest='input',
                        required=False,
                        help='Input file to read. This can be a local file or GCP',
                        default='gs://dataflow_learning/tmp/2020-10-02-11-34-19-EA6C5E314B70B157')
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='dataflow_exp.file_access')

    parser.add_argument('--project', dest='project', required=False, help='Project name', default='evident-quasar-268120',
                        action="store")

    parser.add_argument('--bucket_name', dest='bucket_name', required=False, help='bucket name',
                        default='dataflow_learning')

    parser.add_argument('--runner', dest='runner', required=False, help='Runner Name', default='DataflowRunner',
                        action="store")

    parser.add_argument('--jobname', dest='job_name', required=False, help='jobName', default='dataflowtest',
                        action="store")

    parser.add_argument('--staging_location', dest='staging_location', required=False, help='staging_location',
                        default='gs://dataflow_learning/staging')

    parser.add_argument('--region', dest='region', required=False, help='Region', default='europe-west1',
                        action="store")

    parser.add_argument('--temp_location', dest='temp_location', required=False, help='temp location',
                        default='gs://dataflow_learning/temp/')

    parser.add_argument('--template_name', dest='template_name', required=False, help='template name',
                        default='example-template')


    args = parser.parse_args()
    SCHEMA = 'bucket:string,date:datetime,operation:string,key:string,request_uri:string,http_status:string,error_code:string,bytes_sent:string,total_time:string,turnaround_time:string,referrer:string,user_agent:string,request_header:string'
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
    di = data_ingestion()

    (p | 'Read from a File' >> beam.io.ReadFromText(pipeline_options["input"], skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: di.parse_method(s)) |
     'Write to BigQuery' >> beam.io.Write(
                beam.io.BigQuerySink(
                    pipeline_options["output"],
                    schema=SCHEMA,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run()
    #.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
