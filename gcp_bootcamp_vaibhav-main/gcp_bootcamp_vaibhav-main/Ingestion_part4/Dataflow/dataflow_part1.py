#Create a dataflow batch pipeline using Python to load data from file “f100k.csv” in GCS bucket to BigQuery Table “t100k”. Use VPC network created at the start of the bootcamp and machine type as n1-standard-2 and not more than 3 instances while launching pipeline.

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
SCHEMA = 'Order_Quantity:INT64,PinCode:INT64,Regular_Customer:BOOL,bs:STRING,city:STRING,id:INT64,name:STRING,randomdata:INT64,spend:INT64,state:STRING,id10:INT64,id2:INT64,id3:INT64,id4:INT64,id5:INT64,id6:INT64,id7:INT64,id8:INT64,id9:INT64,id_1:INT64,id11:INT64,id12:INT64,id13:INT64,id14:INT64,id15:INT64,id16:INT64,id17:INT64,id18:INT64,id19:INT64,id20:INT64,id21:INT64,id22:INT64,id23:INT64,id24:INT64,id25:INT64,id26:INT64,id27:INT64,id28:INT64,id29:INT64,id30:INT64,id31:INT64,id32:INT64,id33:INT64,id34:INT64,id35:INT64,id36:INT64,id37:INT64,id38:INT64,id39:INT64,id40:INT64'

p = beam.Pipeline(options=PipelineOptions())

(p | 'ReadData' >> beam.io.ReadFromText('gs://vaibhav-gupta-dfpart/f100k.csv', skip_header_lines =1)
       | 'Split' >> beam.Map(lambda x: x.split(','))
       | 'format to dict' >> beam.Map(lambda x: {"Order_Quantity": x[0],"PinCode": x[1],"Regular_Customer": x[2],"bs": x[3],"city": x[4],"id": x[5],"name": x[6],"randomdata": x[7],"spend": x[8],"state": x[9],"id10": x[10],"id2": x[11],"id3": x[12],"id4": x[13],"id5": x[14],"id6": x[15],"id7": x[16],"id8": x[17],"id9": x[18],"id_1": x[19],"id11": x[20],"id12": x[21],"id13": x[22],"id14": x[23],"id15": x[24],"id16": x[25],"id17": x[26],"id18": x[27],"id19": x[28],"id20": x[29],"id21": x[30],"id22": x[31],"id23": x[32],"id24": x[33],"id25": x[34],"id26": x[35],"id27": x[36],"id28": x[37],"id29": x[38],"id30": x[39],"id31": x[40],"id32": x[41],"id33": x[42],"id34": x[43],"id35": x[44],"id36": x[45],"id37": x[46],"id38": x[47],"id39": x[48],"id40": x[49]})
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:dfbatch.t100k'.format('vaibhav-gupta-bootcamp'),
            schema=SCHEMA,

write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
result = p.run()
