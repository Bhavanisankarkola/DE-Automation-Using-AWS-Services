import boto3
import json
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
BUCKET_NAME = os.environ.get('UPLOAD_BUCKET', 'incoming-sop')
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        body = json.loads(event.get('body', '{}'))
        filename = body.get('filename')
        content_type = body.get('contentType')
        file_category = body.get('fileCategory', 'sop')  # Default to 'sop' for backward compatibility
        
        if not filename or not content_type:
            raise ValueError("Missing 'filename' or 'contentType' in request body")
        
        # Validate file category and content type
        if file_category == 'template':
            allowed_types = [
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # .xlsx
                'application/vnd.ms-excel'  # .xls
            ]
            if content_type not in allowed_types:
                raise ValueError(f"Invalid content type for template file: {content_type}. Must be Excel file (.xlsx or .xls)")
            
            folder = "DE_Templates"  # Updated to match your S3 path
            logger.info(f"Processing template file: {filename}")
            
        elif file_category == 'sop':
            # Validate SOP file types (existing logic)
            allowed_sop_types = [
                'application/pdf',
                'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # .docx
                'application/msword'  # .doc
            ]
            if content_type not in allowed_sop_types:
                raise ValueError(f"Invalid content type for SOP file: {content_type}. Must be PDF or Word document")
            
            folder = "SOP"  # Matches your S3 path
            logger.info(f"Processing SOP file: {filename}")
            
        else:
            raise ValueError(f"Invalid file category: {file_category}. Must be 'sop' or 'template'")
        
        # Sanitize filename to prevent directory traversal
        filename = os.path.basename(filename)
        s3_key = f"{folder}/{filename}"
        # Generate presigned URL with content type validation
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME, 
                'Key': s3_key,
                'ContentType': content_type
            },
            ExpiresIn=300,  # 5 minutes
            HttpMethod='PUT'
        )
        
        logger.info(f"Generated pre-signed URL for {s3_key} with Content-Type {content_type}")
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            },
            'body': json.dumps({
                'uploadURL': presigned_url, 
                'key': s3_key,
                'fileCategory': file_category
            })
        }
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            },
            'body': json.dumps({'error': str(ve)})
        }
    
    except Exception as e:
        logger.error(f"Error generating pre-signed URL: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            },
            'body': json.dumps({'error': 'Internal server error'})
        }
