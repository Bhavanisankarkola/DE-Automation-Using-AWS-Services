import boto3
import openpyxl
import io
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # 1. Extract bucket and key
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        if not key.startswith("DE_Templates/") or not key.lower().endswith(".xlsx"):
            return {
                "statusCode": 400,
                "body": "Not a valid DE_Templates Excel file."
            }

        # 2. Read Excel file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        file_stream = io.BytesIO(response['Body'].read())
        wb = openpyxl.load_workbook(file_stream, data_only=True)

        if "DE Template" not in wb.sheetnames:
            return {
                "statusCode": 400,
                "body": "Sheet named 'DE' not found in the workbook."
            }

        sheet = wb["DE Template"]

        # 3. Expected headers
        expected_headers = ["Attribute", "Required Questions", "Considerations"]
        header_row_idx = None
        headers = []

        # 4. Locate the header row
        for i, row in enumerate(sheet.iter_rows(values_only=True), start=1):
            if row is None:
                continue
            cleaned_row = [str(cell).strip() if cell else "" for cell in row]
            if all(header in cleaned_row for header in expected_headers):
                header_row_idx = i
                headers = cleaned_row
                break

        if header_row_idx is None:
            return {
                "statusCode": 400,
                "body": "Required headers not found in the DE sheet."
            }

        # 5. Extract data under headers
        data = []
        for row in sheet.iter_rows(min_row=header_row_idx + 1, values_only=True):
            if not row or all(cell is None or str(cell).strip() == "" for cell in row):
                continue  # skip empty rows
            row_dict = dict(zip(headers, row))
            extracted_row = {
                "Attribute": str(row_dict.get("Attribute", "")).strip(),
                "Required Questions": str(row_dict.get("Required Questions", "")).strip(),
                "Considerations": str(row_dict.get("Considerations", "")).strip()
            }
            if extracted_row["Attribute"]:  # only include if Attribute exists
                data.append(extracted_row)

        return {
            "statusCode": 200,
            "body": data
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error occurred: {str(e)}"
        }
