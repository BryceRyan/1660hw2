import boto3
import csv

s3 = boto3.resource('s3', aws_access_key_id='AKIAJLPDRDFX65JR5KEQ', aws_secret_access_key='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')

try:
    s3.create_bucket(Bucket='bar68hw2', CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
except:
    print("bucket already made")

bucket=s3.Bucket("bar68hw2")
bucket.Acl().put(ACL='public-read')

body=open('exp1.csv', 'rb')
o=s3.Object('bar68hw2', 'test').put(Body=body)
s3.Object('bar68hw2', 'test').Acl().put(ACL='public-read')

dyndb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id='AKIAJLPDRDFX65JR5KEQ', aws_secret_access_key='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')

try:
    table=dyndb.create_table(TableName='DataTable',
    KeySchema=[
        {
            'AttributeName':'PartitionKey',
            'KeyType':'HASH'
        },
        {
            'AttributeName':'RowKey',
            'KeyType':'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName':'PartitionKey',
            'AttributeType':'S'
        },
        {
            'AttributeName':'RowKey',
            'AttributeType':'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits':5,
        'WriteCapacityUnits':5
    })
except:
    table=dyndb.Table('DataTable')

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

with open('experiments.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',')
    next(csvf)
    for item in csvf:
        body=open(item[2], 'rb')
        s3.Object('bar68hw2', item[2]).put(Body=body)
        md=s3.Object('bar68hw2', item[2]).Acl().put(ACL='public-read')
        url="https://s3-us-west-2.amazonaws.com/bar68hw2/"+item[2]
        metadata_item={'PartitionKey':item[0],'RowKey':item[1], 'description':item[4], 'date':item[3], 'url':url}

        try:
            table.put_item(Item=metadata_item)
        except:
            print("Item may already be there or another failure")

response=table.get_item(Key={'PartitionKey':'experiment1', 'RowKey':'data1'})
print(response["Item"])