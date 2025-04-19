import json
import boto3
from decimal import Decimal

s3_client = boto3.client('s3')
comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')

ai_table = dynamodb.Table('vinod-ai-output')

def lambda_handler(event, context):

    print('The event in the lambda handler is: ', event)
        
    # hard-coded initially
    # input_bucket = 'vinod-aws-ai-output-2'
    # output_bucket = 'vinod-aws-ai-output-3' # not needed if we are loading dynamodb table instead of S3 bucket

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

                    input_file_list = []
                                       
                    response = s3_client.list_objects_v2(Bucket=input_bucket)
                    print('The response from list objects in input bucket is: ', response)
                    
                    main_input_file = ''
                    for obj in response['Contents']:
                        input_file = obj['Key']
                        if ('.pdf' in input_file):
                            main_input_file = input_file
                    
                    for obj in response['Contents']:
                        input_file = obj['Key']
                        
                        if ('.' not in input_file):
                            input_file_list.append(input_file)
                            print('The input file list is: ', input_file_list)
                            print('The length of the input file list is: ', len(input_file_list))

                            # For S3 load

                            # if len(input_file_list) < 24:
                            #     continue
                            # else:
                            #     #  below chunk of code will detect sentiment for a batch of 25 files
                            #     batch_detect_sentiment_response = comprehend.batch_detect_sentiment(
                            #     TextList=input_file_list,
                            #     LanguageCode='en'
                            #    )  
                            #     print('The batch detect sentiment response is: ', batch_detect_sentiment_response['ResultList'])
                            #     comprehend_output_1 = json.dumps(batch_detect_sentiment_response['ResultList'])
                            #     s3_put_object_comprehend_25_response = s3_client.put_object(Bucket=output_bucket, Key=input_file, Body=comprehend_output_1)
                            #     print('The s3 put object comprehend 25 response is: ', s3_put_object_comprehend_25_response)
                            #     input_file_list = []
                            #     continue

                            # For DynamoDB load

                            if len(input_file_list) < 24:
                                continue

                            else:

                                #  below chunk of code will detect sentiment for a batch of 25 files
                                batch_detect_sentiment_response = comprehend.batch_detect_sentiment(
                                TextList=input_file_list,
                                LanguageCode='en'
                            )  
                                print('The batch detect sentiment response is: ', batch_detect_sentiment_response['ResultList'])
                                
                                items_to_add = []
                                idx = 0
                                for result in batch_detect_sentiment_response['ResultList']:

                                    file_name = main_input_file
                                    # idx = result['Index'] this can repeat when there are multiple batches of 25 files
                                    idx = idx + 1
                                    sentiment = result['Sentiment']
                                    positive_sentiment_score = result['SentimentScore']['Positive']
                                    negative_sentiment_score = result['SentimentScore']['Negative']
                                    neutral_sentiment_score = result['SentimentScore']['Neutral']
                                    mixed_sentiment_score = result['SentimentScore']['Mixed']
                                    
                                    # Decimal(str)) to overcome the dynamodb error: Float types are not supported. Use Decimal types instead
                                    item = {
                                        'file_name': file_name,
                                        'idx': idx,
                                        'sentiment': sentiment,
                                        'positive_sentiment_score': Decimal(str(positive_sentiment_score)),
                                        'negative_sentiment_score': Decimal(str(negative_sentiment_score)),
                                        'neutral_sentiment_score': Decimal(str(neutral_sentiment_score)),
                                        'mixed_sentiment_score': Decimal(str(mixed_sentiment_score))
                                    }

                                    items_to_add.append(item)
                                    
                                try:
                                    with ai_table.batch_writer() as batch:
                                        for item in items_to_add:
                                            batch.put_item(Item=item)
                                except Exception as e:
                                    print('The exception is: ', e)

                            input_file_list = []
                            continue
                    
                    # below chunk of code will detect sentiment for the remaining files
                    # so, suppose we have 36 files to be processed, the else part in above block will capture the sentiment of first 25 files
                    # the below block will capture the sentiment of remaining 11 files

                    # For S3 load

                    # batch_detect_sentiment_response = comprehend.batch_detect_sentiment(
                    #         TextList=input_file_list,
                    #         LanguageCode='en'
                    #        )
                    # print('The batch detect sentiment response is: ', batch_detect_sentiment_response['ResultList'])
                    # comprehend_output_2 = json.dumps(batch_detect_sentiment_response['ResultList'])
                    # s3_put_object_comprehend_remaining_response = s3_client.put_object(Bucket=output_bucket, Key=input_file, Body=comprehend_output_2)
                    # print('The s3 put object comprehend remaining response is: ', s3_put_object_comprehend_remaining_response)

                    # For DynamoDB load

                    batch_detect_sentiment_response = comprehend.batch_detect_sentiment(
                            TextList=input_file_list,
                            LanguageCode='en'
                        )
                    print('The batch detect sentiment response is: ', batch_detect_sentiment_response['ResultList'])

                    items_to_add = []
                    for result in batch_detect_sentiment_response['ResultList']:

                        file_name = main_input_file
                        # idx = result['Index'] this can repeat when there are multiple batches of 25 files
                        idx = idx + 1
                        sentiment = result['Sentiment']
                        positive_sentiment_score = result['SentimentScore']['Positive']
                        negative_sentiment_score = result['SentimentScore']['Negative']
                        neutral_sentiment_score = result['SentimentScore']['Neutral']
                        mixed_sentiment_score = result['SentimentScore']['Mixed']
                                    
                        item = {
                                'file_name': file_name,
                                'idx': idx,
                                'sentiment': sentiment,
                                'positive_sentiment_score': Decimal(str(positive_sentiment_score)),
                                'negative_sentiment_score': Decimal(str(negative_sentiment_score)),
                                'neutral_sentiment_score': Decimal(str(neutral_sentiment_score)),
                                'mixed_sentiment_score': Decimal(str(mixed_sentiment_score))
                                }
                        items_to_add.append(item)
                        
                    try:
                        with ai_table.batch_writer() as batch:
                            for item in items_to_add:
                                batch.put_item(Item=item)
                    except Exception as e:
                        print('The exception is: ', e)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

    # start_sentiment_detection_job_response = comprehend.start_sentiment_detection_job(
    # InputDataConfig={
    #     'S3Uri': 's3://vinod-aws-ai-output-2/'
    # },
    # OutputDataConfig={
    #     'S3Uri': 's3://vinod-aws-ai-output-3/'
    # },
    # DataAccessRoleArn='arn:aws:iam::100163808729:role/service-role/AmazonComprehendServiceRole-ai',
    # LanguageCode='en'
    # )

    # print('The response from the comprehend start sentiment detection job is: ', start_sentiment_detection_job_response)

        # while (len(input_file_list) < 24):
    #     for obj in response['Contents']:
    #         input_file = obj['Key']
    #         input_file_list.append(input_file)
    #         print('The input file list is: ', input_file_list)
    #         print('The length of the input file list is: ', len(input_file_list))
    
    # for obj in response['Contents']:
    #     input_file = obj['Key']
    #     input_file_list.append(input_file)

