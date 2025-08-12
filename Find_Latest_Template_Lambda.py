import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Finds the most recently modified object in the S3 prefix for DE templates
    and returns its bucket and key.
    """
    processing_bucket = "de-processing-bucket"
    template_prefix = "DE_Templates/"
    
    try:
        # List all objects in the specified prefix
        response = s3_client.list_objects_v2(
            Bucket=processing_bucket,
            Prefix=template_prefix
        )
        
        if 'Contents' not in response or not response['Contents']:
            raise ValueError(f"No templates found in s3://{processing_bucket}/{template_prefix}")

        # Filter out any "folder" objects and sort by last modified time, descending
        all_files = [obj for obj in response['Contents'] if obj['Key'] != template_prefix]
        latest_template = sorted(all_files, key=lambda obj: obj['LastModified'], reverse=True)[0]
        
        latest_key = latest_template['Key']
        logger.info(f"Found latest template: {latest_key}")
        
        # Return the location of the latest template file
        return {
            "s3_bucket": processing_bucket,
            "s3_key": latest_key
        }

    except Exception as e:
        logger.error(f"Error finding latest template: {str(e)}")
        raise e
