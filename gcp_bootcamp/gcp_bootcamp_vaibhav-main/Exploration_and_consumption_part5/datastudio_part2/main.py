def csv_loader(data, context):
        import os
        from google.cloud import bigquery
        client = bigquery.Client()
        dataset_id = os.environ['DATASET']
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = [
                bigquery.SchemaField('orderid', 'INTEGER'),
                bigquery.SchemaField('customerid', 'INTEGER'),
                bigquery.SchemaField('orderplaceddatetime', 'DATETIME'),
                bigquery.SchemaField('Ordercompletiondatetime', 'DATETIME'),
                bigquery.SchemaField('orderstatus', 'STRING')
                ]
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV
# get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://' + 'mydukan_order_details/' + data['name']
        if(data['name'] == 'order_details.csv'):
# lets do this
            load_job = client.load_table_from_uri(
                    uri,
                    dataset_ref.table('order_details'),
                    job_config=job_config)
            print('Starting job {}'.format(load_job.job_id))
            print('Function=csv_loader, Version=' + os.environ['VERSION'])
            print('File: {}'.format(data['name']))
            load_job.result()  # wait for table load to complete.
            print('Job finished.')
            destination_table = client.get_table(dataset_ref.table('order_details'))
            print('Loaded {} rows.'.format(destination_table.num_rows))

        elif(data['name'] == 'order_quantity.csv'):
                job_config.schema = [
                    bigquery.SchemaField('orderid', 'INTEGER'),
                    bigquery.SchemaField('productid', 'INTEGER'),
                    bigquery.SchemaField('quantity', 'INTEGER'),

                ]
                job_config.skip_leading_rows = 1
                job_config.source_format = bigquery.SourceFormat.CSV

                load_job = client.load_table_from_uri(
                    uri,
                    dataset_ref.table('order_quantity'),
                    job_config=job_config)
                print('Starting job {}'.format(load_job.job_id))
                print('Function=csv_loader, Version=' + os.environ['VERSION'])
                print('File: {}'.format(data['name']))
                load_job.result()  # wait for table load to complete.
                print('Job finished.')
                destination_table = client.get_table(dataset_ref.table('order_quantity'))
                print('Loaded {} rows.'.format(destination_table.num_rows))

