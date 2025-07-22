import boto3
import time
import urllib.parse

s3 = boto3.client('s3')
textract = boto3.client('textract')

def lambda_handler(event, context):
    try:
        # 1. Get S3 object info from Step Function or S3 trigger
        if 'Records' in event:
            bucket = event['Records'][0]['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])
        else:
            bucket = event.get('bucket')
            key = urllib.parse.unquote_plus(event.get('key'))  # ✅ Ensure key is decoded

        print(f"[Extract_Text_Lambda] File: s3://{bucket}/{key}")

        # 2. Start Textract table analysis
        table_job_id = textract.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=['TABLES']
        )['JobId']
        print(f"Started TABLE analysis job: {table_job_id}")

        # 3. Start Textract text detection
        text_job_id = textract.start_document_text_detection(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
        )['JobId']
        print(f"Started TEXT detection job: {text_job_id}")

        # 4. Wait for both jobs
        wait_for_job(table_job_id, analysis=True)
        wait_for_job(text_job_id, analysis=False)

        # 5. Process table data
        table_blocks = get_all_blocks(table_job_id, analysis=True)
        block_map = {block['Id']: block for block in table_blocks}

        tables = []
        for block in table_blocks:
            if block['BlockType'] == 'TABLE':
                table = extract_table(block, block_map)
                tables.append(table)
        print(f"Extracted {len(tables)} tables")

        # 6. Group roles and responsibilities
        grouped_roles = {}
        current_role = None
        role_keywords = ['Manager', 'Officer', 'Representative', 'Analyst', 'Lead', 'Executive', 'Team', 'Head', 'Approver']

        for table in tables:
            for row in table:
                role_found = False
                for i, cell in enumerate(row):
                    if any(keyword in cell for keyword in role_keywords):
                        current_role = cell.strip()
                        if current_role not in grouped_roles:
                            grouped_roles[current_role] = []
                        role_found = True
                        responsibilities = [c.strip() for j, c in enumerate(row) if j != i and c.strip()]
                        grouped_roles[current_role].extend(responsibilities)
                        break
                if not role_found and current_role:
                    responsibilities = [c.strip() for c in row if c.strip()]
                    grouped_roles[current_role].extend(responsibilities)

        # 7. Extract full SOP text
        text_blocks = get_all_blocks(text_job_id, analysis=False)
        full_text = "\n".join([block['Text'] for block in text_blocks if block['BlockType'] == 'LINE'])

        # 8. Return structured output for next step
        return {
            'status': 'success',
            'bucket': bucket,
            'key': key,
            'tableCount': len(tables),
            'groupedResponsibilities': grouped_roles,
            'extractedText': full_text
        }

    except Exception as e:
        print(f"[Extract_Text_Lambda] Error: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

# Wait for Textract job
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

# Get all Textract blocks
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

# Extract Table Content
def extract_table(table_block, block_map):
    rows = []
    for rel in table_block.get('Relationships', []):
        if rel['Type'] == 'CHILD':
            for cell_id in rel['Ids']:
                cell = block_map[cell_id]
                if cell['BlockType'] == 'CELL':
                    row_idx = cell['RowIndex']
                    col_idx = cell['ColumnIndex']
                    cell_text = extract_cell_text(cell, block_map)

                    while len(rows) < row_idx:
                        rows.append([])
                    while len(rows[row_idx - 1]) < col_idx:
                        rows[row_idx - 1].append('')
                    rows[row_idx - 1][col_idx - 1] = cell_text
    return rows

# Extract text in table cell
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
    return text.strip()
