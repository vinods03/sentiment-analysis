# sentiment-analysis

Here is an example of processing unstructured data using AWS Generative AI services.
When a PDF document is uploaded into an S3 bucket, we make use of Amazon Textract and Amazon Comprehend to analyze the sentiment of the document (positive, negative, neutral)
We are making use of SQS queue and lambda for the orchestration.
