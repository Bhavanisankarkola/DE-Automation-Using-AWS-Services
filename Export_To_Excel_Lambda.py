import json
import boto3
import pandas as pd
import io
import os
import traceback

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the target bucket for the output Excel file
TARGET_BUCKET = os.environ.get('TARGET_BUCKET', 'sop-output-bucket')


# --- NO CHANGES TO YOUR CORE LOGIC FUNCTIONS BELOW ---

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


# --- UPDATED LAMBDA HANDLER SECTION ---

def lambda_handler(event, context):
    """
    Reads the S3 location of the analysis JSON, converts it to Excel,
    and uploads the final report. This handler is updated for Step Function integration.
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # Step 1: Get the source bucket and key from the event object.
        # THIS IS THE UPDATED SECTION: It now looks for 's3_bucket' and 's3_key'.
        source_bucket = event['s3_bucket']
        source_key = event['s3_key']

    except KeyError as e:
        error_msg = f"Error: Missing required key in the event payload: {e}. This function expects 's3_bucket' and 's3_key'."
        print(error_msg)
        raise ValueError(error_msg)

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

        # 4. Determine the output filename based on the source file key
        # (This logic from your original code is slightly improved)
        base_name = os.path.basename(source_key) # e.g., "TEST SoP MR_claude_analysis.json"
        sop_name = base_name.replace('_claude_analysis.json', '') # e.g., "TEST SoP MR"
        output_filename = f"{sop_name}_Final_Analysis.xlsx"
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
                            if cell.value:
                                max_length = max(len(str(cell.value)), max_length)
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
        
        final_report_location = f"s3://{TARGET_BUCKET}/{output_key}"
        print(f"Workflow complete. Final report available at: {final_report_location}")

        # 7. Return a success response for the Step Function.
        return {
            "status": "success",
            "final_report_location": final_report_location
        }

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(traceback.format_exc())
        raise e

"""
Lambda Test Event
{
  "s3_bucket": "de-processing-bucket",
  "s3_key": "analysis_results/TEST SoP MR_claude_analysis.json"
}

"""
