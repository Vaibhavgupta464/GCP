#Create a view with required fields which we can import to datastudio

from google.cloud import bigquery
client = bigquery.Client()
view_id = "vaibhav-gupta-bootcamp.mydukan_part2.studio"
view = bigquery.Table(view_id)
view.view_query = f"""select order_details.orderid,orderplaceddatetime,Ordercompletiondatetime,orderstatus,quantity from mydukan_part2.order_details left outer join mydukan_part2.order_quantity
on order_details.orderid= order_quantity.orderid"""


view = client.create_table(view)
print(f"Created {view.table_type}: {str(view.reference)}")
