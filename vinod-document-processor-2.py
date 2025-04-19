import json
import boto3

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event, context):

    print('The event is: ', event)
    
    # hard-coded initially
    # input_bucket = 'vinod-aws-ai-output'

    output_bucket = 'vinod-aws-ai-output-2'
    
    records = event['Records']
    for record in records:
        if 'body' in record:
            body = record['body']
            print('The body is: ', body)
            json_body = json.loads(body)
            print('The json body is: ', json_body)
            records = json_body['Records']
            for record in records:
                if 's3' in record:
                    s3 = record['s3']
                    print('The s3 is: ', s3)
                    bucket = s3['bucket']
                    print('The bucket is: ', bucket)
                    input_bucket = bucket['name']
                    print('The input bucket name is: ', input_bucket)

                    response = s3_client.list_objects_v2(Bucket=input_bucket)
                    print('The response from list objects in input bucket is: ', response)

                    for obj in response['Contents']:

                        input_file = obj['Key']
                        print('The input file is: ', input_file)

                        if ('.' in input_file):
                            copy_source = {
                                'Bucket': input_bucket,
                                'Key': input_file
                            }
                            s3_resource.meta.client.copy(copy_source, output_bucket, input_file)
                            
                        else:

                            s3_get_object_response = s3_client.get_object(Bucket=input_bucket, Key=input_file)
                            # print('The get object from vinod-aws-ai-output response is: ', s3_get_object_response)
                            
                            input_data = json.loads(s3_get_object_response['Body'].read())
                            # print('The data is: ', input_data)
                            
                            iter = 0           
                            sze = 0
                            text = []
                            text_str = ''

                            for block in input_data["Blocks"]:

                                if (block["BlockType"] == "LINE"):
                                    # print('The block text now is: ', block["Text"])
                                    text.append(block["Text"])
                                    text_str = ' '.join(text)
                                    # print('The text_str now is: ', text_str)
                                    sze = len(text_str)
                                    # print('The size now, not yet ready for file put, is: ', input_file, ' ', sze)
                                    if (sze >= 4500):
                                        input_key = input_file + '_' + str(iter)
                                        # print('The size now, ready for file put, is: ', input_key, ' ', sze)
                                        output_data = json.dumps(text_str)
                                        s3_put_object_response_oversize = s3_client.put_object(Bucket=output_bucket, Key=input_key, Body=output_data)
                                        # print('The put object into vinod-aws-ai-output-2 response oversize is: ', s3_put_object_response_oversize)
                                        sze = 0
                                        iter = iter + 1
                                        text = []
                                        text_str = ''
                                        continue
                                    else:
                                        continue

                            output_data = json.dumps(text_str)
                            s3_put_object_response_normal = s3_client.put_object(Bucket=output_bucket, Key=input_file, Body=output_data)
                            # print('The put object into vinod-aws-ai-output-2 response normal is: ', s3_put_object_response_normal)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }