import json
import boto3

textract = boto3.client('textract')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    
    print('The event is: ', event)

    # used hard-coded bucket name / file name initially
    # input_bucket = 'vinod-aws-ai'
    # input_file = 'VINOD_March1980.pdf'
    
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
                    input_file = s3['object']['key']
                    print('The input file is: ', input_file)
   
                    copy_source = {
                        'Bucket': input_bucket,
                        'Key': input_file
                    }
                    
                    output_bucket = 'vinod-aws-ai-output'
                    s3_resource.meta.client.copy(copy_source, output_bucket, input_file)
                    
                    response = textract.start_document_text_detection(
                        DocumentLocation={
                            'S3Object': {
                                'Bucket': input_bucket,
                                'Name': input_file
                            }
                        },
                        NotificationChannel={
                            'SNSTopicArn': 'arn:aws:sns:us-east-1:100163808729:MyNotification',
                            'RoleArn': 'arn:aws:iam::100163808729:role/service-role/vinod-document-processor-role-b11hbn4e'
                        },
                        OutputConfig={
                            'S3Bucket': output_bucket
                        }
                    )
                    
                    print('The response is: ', response)
                    
                    job_id = response['JobId']
                    print('The job id is: ', job_id)
                        
                    return {
                        'statusCode': 200,
                        'body': json.dumps('Hello from Lambda!')
                    }


    # response = textract.detect_document_text(
    #     Document={
    #         'S3Object': {
    #             'Bucket': input_bucket,
    #             'Name': input_file
    #         }
    #     }
    # )
    # 
    # print('The response is: ', response)
    # detected_text = ''
    # for item in response['Blocks']:
    #     if item['BlockType'] == 'LINE':
    #         detected_text += item['Text'] + '\n'
    
    # print('The detected text is: ', detected_text)
    # s3.put_object(Bucket=output_bucket, Key=input_file + '.txt', Body=detected_text)