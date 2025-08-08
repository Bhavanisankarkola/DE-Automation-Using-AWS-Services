import boto3
import time
import json
import re

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

def lambda_handler(event, context):
    try:
        if "Records" in event:
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
        else:
            bucket = event["bucket"]
            key = event["key"]
    except KeyError as e:
         return { "status": "error", "message": f"Missing required event key: {e}" }

    sop_filename = key.split("/")[-1]

    try:
        job_id = start_textract_job(bucket, key)
        print(f"Started Textract job {job_id} for {key}")
        
        success = is_job_complete(job_id)
        if not success:
            return {
                "status": "error",
                "message": f"Textract job {job_id} failed on {key}"
            }

        all_blocks = get_all_textract_blocks(job_id)
        raw_text, tables = extract_text_and_tables(all_blocks)

        return {
            "status": "success",
            "sop_filename": sop_filename,
            "bucket": bucket,
            "key": key,
            "s3_key": key,  # ✅ Added this line for SOP_Structure_Formation_Lambda
            "raw_text": raw_text,
            "tables": tables
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            "status": "error",
            "message": str(e)
        }


"""
Lambda Test event
{
  "bucket": "incoming-sop",
  "key": "SOP/TEST SoP MR.pdf"
}
"""
