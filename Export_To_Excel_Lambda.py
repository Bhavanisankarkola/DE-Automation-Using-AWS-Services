import json
import boto3
import pandas as pd
import io
import os

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the target bucket for the output Excel file
# It's better to get this from an environment variable for flexibility
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'sop-output-bucket')

def format_list_column(data):
    """
    Helper function to format columns that contain lists (like Evidence or Considerations)
    into a readable, multi-line string for a single Excel cell.
    """
    if not isinstance(data, list):
        return data

    # Handle the 'Evidence' list of dictionaries
    if data and isinstance(data[0], dict):
        formatted_items = []
        for item in data:
            # Assuming 'section' and 'relevance' keys exist
            section = item.get('section', 'N/A')
            relevance = item.get('relevance', 'N/A')
            formatted_items.append(f"Section: {section}\nRelevance: {relevance}")
        return "\n---\n".join(formatted_items)
    
    # Handle a simple list of strings (like 'Considerations')
    elif data and isinstance(data[0], str):
        return "\n".join(f"- {item}" for item in data)
        
    return ""

def lambda_handler(event, context):
    """
    Lambda function to read a JSON analysis file from S3, convert it to Excel,
    and upload the Excel file to another S3 bucket.

    The event object is expected to contain the source S3 bucket and key.
    Example event for Step Function integration:
    {
      "source_bucket": "de-processing-bucket",
      "source_key": "analysis_results/TEST SoP MR_claude_analysis.json"
    }
    """
    print(f"Received event: {json.dumps(event)}")

    # 1. Get the source bucket and key from the event object
    try:
        source_bucket = event['source_bucket']
        source_key = event['source_key']
    except KeyError:
        print("ERROR: Event object must contain 'source_bucket' and 'source_key'.")
        return {
            'statusCode': 400,
            'body': json.dumps('Error: Missing source_bucket or source_key in the event payload.')
        }

    try:
        # 2. Read the JSON file from the source S3 bucket
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        json_content = response['Body'].read().decode('utf-8')
        data = json.loads(json_content)

        # 3. Extract the 'results' array and convert it to a Pandas DataFrame
        results_data = data.get('results', [])
        if not results_data:
            raise ValueError("JSON data does not contain a 'results' key or the key is empty.")
            
        df = pd.DataFrame(results_data)
        
        # Format list-based columns for better readability in Excel
        if 'Evidence' in df.columns:
            df['Evidence'] = df['Evidence'].apply(format_list_column)
        if 'Considerations' in df.columns:
            df['Considerations'] = df['Considerations'].apply(format_list_column)


        # 4. Determine the output filename based on the 'source_sop_file'
        source_sop_path = data.get('source_sop_file', 'unknown_sop')
        # Extract the base name, e.g., "TEST SoP MR" from "processed-sop/TEST SoP MR_processed.json"
        base_name = os.path.basename(source_sop_path)
        sop_name = base_name.replace('_processed.json', '')
        output_filename = f"{sop_name} Final Output.xlsx"
        output_key = f"excel_outputs/{output_filename}" # Store in a sub-folder

        # 5. Create the Excel file in-memory
        with io.BytesIO() as output_buffer:
            # Use a writer to set column widths for better readability
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Analysis Results')
                # Auto-adjust column widths
                worksheet = writer.sheets['Analysis Results']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = (max_length + 2)
                    worksheet.column_dimensions[column_letter].width = min(adjusted_width, 70) # Cap width at 70

            excel_data = output_buffer.getvalue()

        # 6. Upload the Excel file to the target S3 bucket
        s3_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=output_key,
            Body=excel_data,
            ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

        print(f"Successfully created '{output_key}' and uploaded to bucket '{TARGET_BUCKET}'.")

        # 7. Return a success response with the output file location
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Excel file created and uploaded successfully.',
                'output_bucket': TARGET_BUCKET,
                'output_key': output_key
            })
        }

    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: The file '{source_key}' does not exist in bucket '{source_bucket}'.")
        return {
            'statusCode': 404,
            'body': json.dumps('Error: Source file not found in S3.')
        }
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'An internal error occurred: {str(e)}')
        }
