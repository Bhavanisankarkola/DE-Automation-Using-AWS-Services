import json
import boto3
import logging
from datetime import datetime
import time
import random
from botocore.exceptions import ClientError
import os
import traceback

# --- Standard Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = boto3.client('s3')
bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')


# --- NO CHANGES TO YOUR CORE LOGIC FUNCTIONS BELOW ---

def invoke_model_with_retry(bedrock_client, model_id, body, max_retries=5):
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

# --- UPDATED LAMBDA HANDLER SECTION ---
# This handler is designed to work correctly within the Step Function.

def lambda_handler(event, context):
    """
    Receives S3 locations for the structured SOP and the DE template,
    runs analysis using Claude, saves the result, and returns the S3 location of that result.
    """
    output_bucket_name = 'de-processing-bucket'
    
    try:
        # Step 1: Get input file locations from the Step Function event.
        # THIS IS THE UPDATED SECTION that correctly parses the input from the Parallel step.
        # Instead of 'sop_file_key', it now looks for 'structured_sop_input'.
        sop_input = event['structured_sop_input']
        template_input = event['de_template_input']
        
        sop_bucket = sop_input['s3_bucket']
        sop_key = sop_input['s3_key']
        
        template_bucket = template_input['s3_bucket']
        template_key = template_input['s3_key']

        logger.info(f"Processing SOP: s3://{sop_bucket}/{sop_key}")
        logger.info(f"Using Template: s3://{template_bucket}/{template_key}")

        # Step 2: Define the output file location. This logic is adapted from your original code.
        sop_base_filename = os.path.splitext(os.path.basename(sop_key))[0]
        if sop_base_filename.endswith('_processed'):
            sop_base_filename = sop_base_filename.replace('_processed', '')
        
        output_filename = f"{sop_base_filename}_claude_analysis.json"
        output_key = f"analysis_results/{output_filename}"

        # Step 3: Retrieve and load the input files from S3.
        sop_response = s3_client.get_object(Bucket=sop_bucket, Key=sop_key)
        sop_data = json.loads(sop_response['Body'].read().decode('utf-8'))
        
        template_response = s3_client.get_object(Bucket=template_bucket, Key=template_key)
        de_template = json.loads(template_response['Body'].read().decode('utf-8'))
        
        # --- Your core analysis logic begins here. No changes have been made to this section. ---
        if isinstance(de_template, dict):
            de_template = [de_template]
            
        analysis_results = []
        sop_text_for_prompt = json.dumps(sop_data, indent=2)

        for i, template_item in enumerate(de_template):
            try:
                if i > 0:
                    time.sleep(3) # Your original delay
                
                attribute = template_item.get('Attribute', 'Unknown')
                required_question = template_item.get('Required Questions', '')
                considerations = template_item.get('Considerations', '')
                
                logger.info(f"Analyzing attribute '{attribute}'. Sending {len(sop_text_for_prompt)} characters to the model.")
                
                prompt = f"""Analyze this SOP for "{attribute}".

SOP: {sop_text_for_prompt}

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
                    "max_tokens": 5000,
                    "messages": [{"role": "user", "content": prompt}]
                }
                
                response = invoke_model_with_retry(
                    bedrock_client, 
                    "anthropic.claude-3-sonnet-20240229-v1:0", 
                    body
                )
                
                response_body = json.loads(response['body'].read())
                claude_text = response_body['content'][0]['text']
                
                try:
                    start = claude_text.find('{')
                    end = claude_text.rfind('}') + 1
                    claude_json = json.loads(claude_text[start:end]) if start != -1 and end != -1 else {"comment": "No JSON object found in model response."}
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
        
        # --- End of your core analysis logic ---

        # Step 4: Save the final results to S3.
        # This section is adapted from your original code.
        output_data = {
            "source_sop_file": f"s3://{sop_bucket}/{sop_key}",
            "analysis_timestamp": datetime.now().isoformat(),
            "results": analysis_results
        }
        
        s3_client.put_object(
            Bucket=output_bucket_name,
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Successfully saved analysis results to s3://{output_bucket_name}/{output_key}")
        
        # Step 5: Return ONLY the location of the new file for the final step.
        return {
            "s3_bucket": output_bucket_name,
            "s3_key": output_key
        }

    except Exception as e:
        logger.error(f"A critical error occurred in the Lambda handler: {str(e)}", exc_info=True)
        # Re-raise the exception to make the Step Function task fail correctly.
        raise e


"""
Lambda Test Event
{
  "structured_sop_input": {
    "s3_bucket": "de-processing-bucket",
    "s3_key": "processed-sop/TEST SoP MR_processed.json"
  },
  "de_template_input": {
    "s3_bucket": "de-processing-bucket",
    "s3_key": "DE_Templates/Control_Testing_Template.json"
  }
}
"""
