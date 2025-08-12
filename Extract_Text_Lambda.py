import boto3
import time
import json
import re
import traceback

# --- Configuration ---
# Define the bucket where intermediate processed files will be stored
PROCESSING_BUCKET = "de-processing-bucket"

# Initialize AWS clients once
textract = boto3.client("textract")
s3_client = boto3.client("s3")

def start_textract_job(bucket, key):
    response = textract.start_document_analysis(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        FeatureTypes=["TABLES", "FORMS"]
    )
    return response["JobId"]


def is_job_complete(job_id):
    while True:
        response = textract.get_document_analysis(JobId=job_id)
        status = response["JobStatus"]
        if status in ["SUCCEEDED", "FAILED"]:
            if status == "FAILED":
                print(f"Textract job failed: {response.get('StatusMessage')}")
            return status == "SUCCEEDED"
        time.sleep(2)


def get_all_textract_blocks(job_id):
    all_blocks = []
    next_token = None
    while True:
        if next_token:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract.get_document_analysis(JobId=job_id)
        
        all_blocks.extend(response.get("Blocks", []))
        next_token = response.get("NextToken")
        
        if not next_token:
            break
            
    return all_blocks


def is_block_inside_tables(line_block, table_geometries):
    line_box = line_block.get('Geometry', {}).get('BoundingBox')
    if not line_box:
        return False

    line_center_x = line_box['Left'] + line_box['Width'] / 2
    line_center_y = line_box['Top'] + line_box['Height'] / 2

    for table_box in table_geometries:
        if (table_box['Left'] <= line_center_x <= table_box['Left'] + table_box['Width'] and
            table_box['Top'] <= line_center_y <= table_box['Top'] + table_box['Height']):
            return True
    return False


def extract_text_and_tables(blocks):
    raw_text_lines = []
    tables = []
    table_geometries = []
    page_line_blocks = {}

    block_map = {block["Id"]: block for block in blocks}

    for block in blocks:
        if block["BlockType"] == "LINE":
            raw_text_lines.append(block["Text"])
            page_number = block.get("Page", 1)
            page_line_blocks.setdefault(page_number, []).append(block)

        elif block["BlockType"] == "TABLE":
            if 'Geometry' in block and 'BoundingBox' in block['Geometry']:
                table_geometries.append(block['Geometry']['BoundingBox'])

            cells_by_row = {}
            for relationship in block.get("Relationships", []):
                if relationship["Type"] == "CHILD":
                    for cell_id in relationship["Ids"]:
                        cell = block_map.get(cell_id)
                        if not cell or cell["BlockType"] != "CELL":
                            continue

                        row_index = cell["RowIndex"]
                        col_index = cell["ColumnIndex"]
                        text = ""
                        
                        if "Relationships" in cell:
                            for child_rel in cell.get("Relationships", []):
                                if child_rel["Type"] == "CHILD":
                                    for child_id in child_rel["Ids"]:
                                        word = block_map.get(child_id)
                                        if word:
                                            if word["BlockType"] == "WORD":
                                                text += word["Text"] + " "
                                            elif word["BlockType"] == "SELECTION_ELEMENT":
                                                selected = word.get("SelectionStatus") == "SELECTED"
                                                text += "[X] " if selected else "[ ] "
                        
                        cells_by_row.setdefault(row_index, {})[col_index] = text.strip()

            sorted_rows = []
            for row_index in sorted(cells_by_row.keys()):
                row = []
                col_map = cells_by_row[row_index]
                for col_index in sorted(col_map.keys()):
                    row.append(col_map[col_index])
                sorted_rows.append(row)

            tables.append({
                "page": block.get("Page", 1),
                "source": "TEXTRACT_TABLE",
                "rows": sorted_rows
            })

    for page_num, line_blocks in page_line_blocks.items():
        buffer = []
        for line_block in line_blocks:
            if is_block_inside_tables(line_block, table_geometries):
                continue
            
            line_text = line_block["Text"]
            if re.search(r"\s{2,}", line_text):
                buffer.append(line_text)
            else:
                if len(buffer) >= 2:
                    parsed_rows = [re.split(r"\s{2,}", l.strip()) for l in buffer]
                    tables.append({
                        "page": page_num,
                        "source": "FALLBACK_REGEX",
                        "rows": parsed_rows
                    })
                buffer = []

        if len(buffer) >= 2:
            parsed_rows = [re.split(r"\s{2,}", l.strip()) for l in buffer]
            tables.append({
                "page": page_num,
                "source": "FALLBACK_REGEX",
                "rows": parsed_rows
            })

    return "\n".join(raw_text_lines), tables


# --- UPDATED LAMBDA HANDLER SECTION ---

def lambda_handler(event, context):
    """
    Receives the S3 location of an SOP, runs Textract, saves the full
    result to a new S3 file, and returns the location of that new file.
    """
    try:
        # Step 1: Get the source bucket and key from the Step Function input.
        # This part of code is already compatible.
        if "Records" in event:
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
        else:
            bucket = event["bucket"]
            key = event["key"]
            
    except KeyError as e:
       raise ValueError(f"Missing required event key: {e}")

    sop_filename_with_ext = key.split("/")[-1]
    sop_filename_base = sop_filename_with_ext.rsplit('.', 1)[0]
    
    # Define the S3 location for the output JSON file
    output_key = f"extracted-text/{sop_filename_base}_extracted.json"

    try:
        # Step 2: Run your existing Textract logic to get raw_text and tables.
        # NO CHANGES were made to this logic.
        job_id = start_textract_job(bucket, key)
        print(f"Started Textract job {job_id} for {key}")
        
        if not is_job_complete(job_id):
            raise Exception(f"Textract job {job_id} failed on {key}")

        all_blocks = get_all_textract_blocks(job_id)
        raw_text, tables = extract_text_and_tables(all_blocks)

        # Step 3: Create the full data payload to be saved.
        output_data = {
            "source_bucket": bucket,
            "source_key": key,
            "sop_filename": sop_filename_with_ext,
            "raw_text": raw_text,
            "tables": tables
        }

        # Step 4: Save the full payload to a new JSON file in S3.
        s3_client.put_object(
            Bucket=PROCESSING_BUCKET,
            Key=output_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )
        print(f"Successfully saved extracted data to s3://{PROCESSING_BUCKET}/{output_key}")

        # Step 5: Return ONLY the location of the output file.
        # This creates the 'extracted_text_output' key that the next function needs.
        return {
            "status": "success",
            "sop_filename": sop_filename_with_ext,
            "extracted_text_output": {
                "s3_bucket": PROCESSING_BUCKET,
                "s3_key": output_key
            }
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        # Re-raise the exception to make the Step Function task fail correctly.
        raise e


"""
Lambda Json Test Event
{
  "bucket": "incoming-sop",
  "key": "SOP/TEST SoP MR.pdf"
}

"""
