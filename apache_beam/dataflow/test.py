import apache_beam as beam
import sys

PROJECT='PROJECT_ID'
BUCKET='BUCKET_NAME'
schema = 'id:INTEGER,region:STRING'

class Split:

    def process(self, element):
        id, region = element.split(",")

        return [{
            'id': int(id),
            'region': region,
        }]

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)

   (p
      | 'ReadFromGCS' >> beam.io.textio.ReadFromText('gs://{0}/staging/dummy.csv'.format(BUCKET))
      | 'ParseCSV' >> beam.ParDo(Split())
      | 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:test.dummy'.format(PROJECT), schema=schema)
   )

   p.run()
