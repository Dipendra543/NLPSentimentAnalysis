import boto3
from botocore.exceptions import ClientError
import os
from os import path
import pickle


def connect(method):
    session = boto3.Session(
        aws_access_key_id=os.getenv("aws_token_key"),
        aws_secret_access_key=os.getenv("aws_secret_key"),
    )
    if method == 'client':
        s3_client = session.client('s3')
        # response = s3_client.list_buckets()
        # print(response)
        return s3_client
    elif method == 'resource':
        s3_resource = session.resource('s3')
        return s3_resource


def get_buckets_details(connector):
    response = connector.list_buckets()['Buckets']
    return response


def create_bucket(connector, bucket_name):
    try:
        connector.create_bucket(Bucket=bucket_name)
    except ClientError as ex:
        print("some error occured", ex)

    except Exception as e:
        print("Some Unknown Error occured", e)


def connect_to_bucket(connector):
    pass


def get_bucket_contents(connector, bucket_name):
    bucket = connector.Bucket(bucket_name)
    for each_obj in bucket.objects.all():
        print("KEY", each_obj.key)
        body = each_obj.get()['Body'].read()
        print("BODY", str(body))


def write_file(connector, bucket_name, filename, text):
    """

    :param connector: Specify the resource connector, NOT the client connector
    implemented like this for easiness
    :param bucket_name: name of the bucket to write files to
    :param filename: name of the file to write to, DONT INCLUDE EXTENSION, files are always uploaded as .txt
    :param text: the actual text to be written in "filename.txt"
    :return: None
    """

    full_name = "files/"+filename+".txt"
    if not path.exists(full_name):
        my_text = text
        with open(full_name, "w") as f:
            f.write(my_text)

        s3_object = connector.Object(bucket_name=bucket_name, key=full_name)
        s3_object.upload_file(full_name)


if __name__ == '__main__':
    # my_connector = connect('client')
    # print(my_connector)
    # create_bucket(my_connector, 'mydkbucketfornlpalice')
    my_client = connect('client')
    my_buckets_response = get_buckets_details(my_client)
    print(my_buckets_response)
    # create_bucket(my_client, "dipendratryingnlpsentiment")
    my_resource = connect('resource')
    # print(type(my_resource), type(my_client))
    # write_file(my_resource, 'nlp-sentiment-analysis', 'testfile', "This is a Wonderful World!!")
    get_bucket_contents(my_resource, 'nlp-sentiment-analysis')




