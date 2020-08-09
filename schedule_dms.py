import boto3  
import os  
import time

client = boto3.client('dms',  
                      region_name = 'us-east-1',                   
                      aws_access_key_id = os.environ.get('AWS_DMS_KEY'),                      
                      aws_secret_access_key = os.environ.get('AWS_DMS_PASSWD'))

a = client.describe_replication_tasks()  
arn_dic = {}

for e in a['ReplicationTasks']: 
    if "parquet-staging-zone-prod" in e['ReplicationTaskIdentifier']:
        arn_dic[e['ReplicationTaskIdentifier']] = e['ReplicationTaskArn']
        
import time
start = time.time()

for k, v in arn_dic.items():  
    print("Task launched:{}".format(k))
    response = client.start_replication_task(
        ReplicationTaskArn=v,
        StartReplicationTaskType='reload-target'
    )
    print("DMS Response: %s" % response)
    
end = time.time()
print("Tempo de Execução: {:.2f} min".format((end - start)/60))
