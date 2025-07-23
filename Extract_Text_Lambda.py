import boto3
import time
import urllib.parse
import re

s3 = boto3.client('s3')
textract = boto3.client('textract')

# Global variable to track headers between tables
last_headers = {}

def lambda_handler(event, context):
    try:
        print("Event Received:", event)

        if 'Records' in event:
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        else:
            bucket = event.get('bucket')
            key = urllib.parse.unquote_plus(event.get('key'))

        print(f"[Extract_Text_Lambda] File: s3://{bucket}/{key}")

        table_job_id = textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=['TABLES']
        )['JobId']

        text_job_id = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
        )['JobId']

        wait_for_job(table_job_id, analysis=True)
        wait_for_job(text_job_id, analysis=False)

        table_blocks = get_all_blocks(table_job_id, analysis=True)
        block_map = {block['Id']: block for block in table_blocks}

        tables = {}
        table_counter = 1
        for block in table_blocks:
            if block['BlockType'] == 'TABLE':
                table_data = extract_table(block, block_map)
                tables[f"Table_{table_counter}"] = table_data
                table_counter += 1

        print(f"Extracted {len(tables)} tables")

        grouped_roles = {}
        current_role = None
        role_keywords = ['Manager', 'Officer', 'Representative', 'Analyst', 'Lead', 'Executive', 'Team', 'Head', 'Approver']

        for table in tables.values():
            for row in table:
                role_found = False
                for key, value in row.items():
                    if isinstance(value, str) and any(keyword in value for keyword in role_keywords):
                        current_role = value.strip()
                        if current_role not in grouped_roles:
                            grouped_roles[current_role] = []
                        role_found = True
                        responsibilities = [v for k, v in row.items() if k != key and v]
                        grouped_roles[current_role].extend(flatten_responsibilities(responsibilities))
                        break
                if not role_found and current_role:
                    responsibilities = [v for v in row.values() if v]
                    grouped_roles[current_role].extend(flatten_responsibilities(responsibilities))

        text_blocks = get_all_blocks(text_job_id, analysis=False)
        full_text = "\n".join([block['Text'] for block in text_blocks if block['BlockType'] == 'LINE'])

        return {
            'status': 'success',
            'bucket': bucket,
            'key': key,
            'tableCount': len(tables),
            'groupedResponsibilities': grouped_roles,
            'extractedText': full_text,
            'structuredTables': tables
        }

    except Exception as e:
        print(f"[Extract_Text_Lambda] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

def wait_for_job(job_id, analysis=True):
    job_type = 'analysis' if analysis else 'text_detection'
    print(f"Waiting for {job_type} job {job_id}...")
    while True:
        if analysis:
            response = textract.get_document_analysis(JobId=job_id)
        else:
            response = textract.get_document_text_detection(JobId=job_id)
        status = response['JobStatus']
        print(f"{job_type} job status: {status}")
        if status == 'SUCCEEDED':
            break
        elif status == 'FAILED':
            raise Exception(f"{job_type} job failed.")
        time.sleep(2)

def get_all_blocks(job_id, analysis=True):
    blocks = []
    next_token = None
    while True:
        if analysis:
            response = textract.get_document_analysis(JobId=job_id, NextToken=next_token) if next_token else textract.get_document_analysis(JobId=job_id)
        else:
            response = textract.get_document_text_detection(JobId=job_id, NextToken=next_token) if next_token else textract.get_document_text_detection(JobId=job_id)
        blocks.extend(response['Blocks'])
        next_token = response.get('NextToken')
        if not next_token:
            break
    return blocks

def extract_table(table_block, block_map):
    global last_headers
    cells_by_row = {}

    for rel in table_block.get('Relationships', []):
        if rel['Type'] == 'CHILD':
            for cell_id in rel['Ids']:
                cell = block_map[cell_id]
                if cell['BlockType'] == 'CELL':
                    row_idx = cell['RowIndex']
                    col_idx = cell['ColumnIndex']
                    text = extract_cell_text(cell, block_map)
                    if row_idx not in cells_by_row:
                        cells_by_row[row_idx] = {}
                    cells_by_row[row_idx][col_idx] = text

    structured_table = []
    sorted_rows = sorted(cells_by_row.keys())
    first_row = cells_by_row[sorted_rows[0]]
    is_header_row = all(isinstance(v, str) and len(v) < 40 for v in first_row.values())
    col_count = len(first_row)

    if is_header_row:
        headers = {col: first_row[col] or f"Column_{col}" for col in first_row}
        last_headers = headers.copy()
        data_rows = sorted_rows[1:]
    elif len(last_headers) == col_count:
        headers = last_headers.copy()
        data_rows = sorted_rows
    else:
        headers = {col: f"Column_{col}" for col in first_row}
        data_rows = sorted_rows[1:]

    for row_idx in data_rows:
        row_data = cells_by_row[row_idx]
        structured_row = {}
        for col_idx, val in row_data.items():
            header = headers.get(col_idx, f"Column_{col_idx}")
            structured_row[header] = split_to_list_if_points(val)
        structured_table.append(structured_row)

    return structured_table

def extract_cell_text(cell_block, block_map):
    text = ''
    for rel in cell_block.get('Relationships', []):
        if rel['Type'] == 'CHILD':
            for child_id in rel['Ids']:
                child = block_map[child_id]
                if child['BlockType'] == 'WORD':
                    text += child['Text'] + ' '
                elif child['BlockType'] == 'SELECTION_ELEMENT' and child['SelectionStatus'] == 'SELECTED':
                    text += '[X] '
    return text.strip() if text.strip() else None

def split_to_list_if_points(text):
    if not text:
        return None
    bullets = re.split(r'\n|\u2022|\u2023|\u25E6|\*|-|•', text)
    cleaned = [line.strip() for line in bullets if line.strip()]
    return cleaned if len(cleaned) > 1 else cleaned[0]

def flatten_responsibilities(items):
    result = []
    for val in items:
        if isinstance(val, list):
            result.extend(val)
        elif isinstance(val, str):
            result.append(val.strip())
    return result

