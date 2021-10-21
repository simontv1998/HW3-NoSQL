from os import environ
import boto3
import csv

# read in access key and secret access key from env vars
ACCESS_KEY_ID = environ.get('ACCESS_KEY_ID')
SECRET_ACCESS_KEY_ID = environ.get('SECRET_ACCESS_KEY_ID')

# Upload your data as blobs to the S3 bucket
s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY_ID, aws_secret_access_key=SECRET_ACCESS_KEY_ID)
try:
    s3.create_bucket(Bucket='14848hw3nosqltest', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except Exception as e:
    print(e)

# we make this bucket pubclicly readable
bucket = s3.Bucket("14848hw3nosqltest")
bucket.Acl().put(ACL='public-read')

# # upload a file into the bucket
body = open('/mnt/c/Simon/CMU/Fall2021/14-848/HW/HW3/data/exp1.csv','rb')
o = s3.Object('14848hw3nosqltest', 'exp1').put(Body=body)
s3.Object('14848hw3nosqltest','exp1').Acl().put(ACL='public-read')

# create a DynamoDB table
dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY_ID
)
try:
    table = dyndb.create_table(
        TableName = 'DataTable',
        KeySchema = [
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print(e)
    # If there is an exception, the table may already exists. If so...
    table = dyndb.Table("DataTable")

# wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

print(table.item_count)
# 0


# Reading the csv file, uploading the blobs and creating the table
with open('c:\Simon\CMU\Fall2021\14-848\HW\HW3\data\experiments.csv', 'rb') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('c:\Simon\CMU\Fall2021\14-848\HW\HW3\data\\'+item[4], 'rb')
        s3.Object('datacont-name', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-2.amazonaws.com/datacont-name/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                        'description': item[4], 'date': item[2], 'url':url}

        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# search for an item
response = table.get_item(
    key = {
        'PartitionKey': 'experiment3',
        'RowKey': '4'
    }
)
item = response['Item']
print(item)