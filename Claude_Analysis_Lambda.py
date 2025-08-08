import json
import boto3
import logging
from datetime import datetime
import time
import random
from botocore.exceptions import ClientError
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def invoke_model_with_retry(bedrock_client, model_id, body, max_retries=5):
    # ... (this function remains the same)
    """Invoke model with exponential backoff retry logic"""
    for attempt in range(max_retries):
        try:
            response = bedrock_client.invoke_model(
                modelId=model_id,
                body=json.dumps(body)
            )
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == 'ThrottlingException':
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    logger.info(f"Throttled on attempt {attempt + 1}. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error("Max retries reached for throttling")
            raise e

def lambda_handler(event, context):
    """Main Lambda handler with full context analysis."""
    
    s3_client = boto3.client('s3')
    bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
    bucket_name = 'de-processing-bucket'
    
    try:
        # Get required file keys from the event
        try:
            sop_file_key = event['sop_file_key']
            de_template_key = event['de_template_key']
        except KeyError as e:
            error_message = f"Missing required input key in the event: {str(e)}"
            logger.error(error_message)
            return {'statusCode': 400, 'body': json.dumps({'error': error_message})}

        # ... (filename processing logic remains the same)
        if sop_file_key.startswith(f's3://{bucket_name}/'):
            sop_file_key = sop_file_key.replace(f's3://{bucket_name}/', '')
        sop_base_filename = os.path.splitext(os.path.basename(sop_file_key))[0]
        if sop_base_filename.endswith('_processed'):
            sop_base_filename = sop_base_filename[:-10]
        output_filename = f"{sop_base_filename}_claude_analysis.json"
        output_key = f"analysis_results/{output_filename}"

        # Retrieve files
        sop_response = s3_client.get_object(Bucket=bucket_name, Key=sop_file_key)
        sop_data = json.loads(sop_response['Body'].read().decode('utf-8'))
        
        template_response = s3_client.get_object(Bucket=bucket_name, Key=de_template_key)
        de_template = json.loads(template_response['Body'].read().decode('utf-8'))
        
        if isinstance(de_template, dict):
            de_template = [de_template]
        
        analysis_results = []
        
        # Process each attribute
        for i, template_item in enumerate(de_template):
            try:
                if i > 0:
                    time.sleep(3)
                
                attribute = template_item.get('Attribute', 'Unknown')
                required_question = template_item.get('Required Questions', '')
                considerations = template_item.get('Considerations', '')
                
                # --- MODIFIED SECTION START ---
                # Use the full SOP data without truncation
                sop_text = json.dumps(sop_data, indent=2)
                
                # Log the size of the text being sent to the model
                logger.info(f"Analyzing attribute '{attribute}'. Sending {len(sop_text)} characters to the model.")
                # --- MODIFIED SECTION END ---
                
                prompt = f"""Analyze this SOP for "{attribute}".

SOP: {sop_text}

Question: {required_question}
Considerations: {considerations}

Respond in JSON:
{{
    "required_answer": "answer",
    "consideration_answers": ["answer1", "answer2"],
    "evidence": [{{"section": "name", "relevance": "why"}}],
    "comment": "summary"
}}"""
                
                body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 5000,  # Increased output token limit for more detailed answers
                    "messages": [{"role": "user", "content": prompt}]
                }
                
                response = invoke_model_with_retry(
                    bedrock_client, 
                    "anthropic.claude-3-sonnet-20240229-v1:0", 
                    body
                )
                
                # ... (rest of the processing and error handling logic)
                response_body = json.loads(response['body'].read())
                claude_text = response_body['content'][0]['text']
                
                try:
                    start = claude_text.find('{')
                    end = claude_text.rfind('}') + 1
                    if start != -1 and end != -1:
                        claude_json = json.loads(claude_text[start:end])
                    else:
                        raise ValueError("No JSON object found")
                except Exception:
                    claude_json = { "comment": "Could not parse valid JSON from model response." }
                
                result = {
                    "Attribute": attribute,
                    "Required Question": required_question,
                    "Considerations": considerations.split('\n') if considerations else [],
                    "Answers": claude_json.get('required_answer', ''),
                    "Evidence": claude_json.get('evidence', []),
                    "Comment": claude_json.get('comment', '')
                }
                
                analysis_results.append(result)
                logger.info(f"Successfully processed attribute: {attribute}")
                
            except Exception as e:
                logger.error(f"Error processing attribute '{attribute}': {str(e)}")
                analysis_results.append({
                    "Attribute": template_item.get('Attribute', 'Unknown'),
                    "Answers": f"Error: {str(e)}",
                    "Comment": f"Processing failed for this attribute: {str(e)}"
                })

        # ... (final result saving and return logic)
        output_data = {
            "source_sop_file": sop_file_key,
            "analysis_timestamp": datetime.now().isoformat(),
            "results": analysis_results
        }
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'output_location': f"s3://{bucket_name}/{output_key}",
        }
        
    except Exception as e:
        logger.error(f"A critical error occurred in the Lambda handler: {str(e)}", exc_info=True)
        raise e
