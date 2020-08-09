import time
start = time.time()

from google.cloud import bigquery
client = bigquery.Client()

from datetime import date
data_atual = date.today()
 
project = "sanar-241611"

databases_id = []
databases_id.append("adwords_sanar")
databases_id.append("app_sanarflix")
databases_id.append("cognito")
databases_id.append("dataforseo")
databases_id.append("facebook_ads")
databases_id.append("mysql_payments")
databases_id.append("new_plataform_rm_prod")
databases_id.append("new_plataforma_rm")
databases_id.append("pagamento_sanarflix_com_br")
databases_id.append("payments")
databases_id.append("residencia_app")
databases_id.append("residencia_plataforma")
databases_id.append("sanar")
databases_id.append("sanarflix")
databases_id.append("sanarflix_platform")
databases_id.append("sanarmed")
databases_id.append("sanarmed_amp")
databases_id.append("sanarmed_com_br_residenciameia")
databases_id.append("sanarmed_dev")
databases_id.append("sanarmed_prod")
databases_id.append("sendgrid_emails")

for dataset_id in databases_id:
    tables = client.list_tables(dataset_id)
    tables_id = []
    
    for table in tables:
        tables_id.append(table.table_id)
    
    config_job = bigquery.ExtractJobConfig(compression = "SNAPPY", destination_format = "AVRO", use_avro_logical_types = "TRUE")

    for table_id in tables_id:
        if (("_view" not in table_id and "vw_" not in table_id) or ("viewed" in table_id and "viewed_view" not in table_id)):
            bucket_name = 'migracao_bigquery/'+str(data_atual)+'/'+dataset_id+'/'+table_id
            destination_uri = "gs://{}/{}".format(bucket_name, table_id+"-*.avro")
            dataset_ref = client.dataset(dataset_id, project=project)
            table_ref = dataset_ref.table(table_id)
            extract_job = client.extract_table(
                table_ref,
                destination_uri,
                # Location must match that of the source table.
                location = "US",
                job_config = config_job,
            )  # API request
            extract_job.result()  # Waits for job to complete.
            print(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
            )
            
end = time.time()
print("Tempo de Execução: {:.2f} min".format((end - start)/60))
            
#Código para ThinkFic
        
from google.cloud import bigquery
client = bigquery.Client()

from datetime import date
data_atual = date.today()

project = "sanar-241611"
dataset_id = "thinkific"

tables = client.list_tables(dataset_id)

tables_id = []

for table in tables:
    tables_id.append(table.table_id)
    
#for table_id in tables_id:
#    if (("_view" not in table_id and "vw_" not in table_id)):
#        print(table_id)

config_job = bigquery.ExtractJobConfig(compression = "SNAPPY", destination_format = "AVRO", use_avro_logical_types = "TRUE")

for table_id in tables_id:
    if (("_view" not in table_id and "vw_" not in table_id)):
        bucket_name = 'migracao_bigquery/'+str(data_atual)+'/'+dataset_id+'/'+table_id
        destination_uri = "gs://{}/{}".format(bucket_name, table_id+"-*.avro")
        dataset_ref = client.dataset(dataset_id, project=project)
        table_ref = dataset_ref.table(table_id)
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location = "US",
            job_config = config_job,
        )  # API request
        extract_job.result()  # Waits for job to complete.
        print(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
        )