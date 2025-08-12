import re
import boto3
import json
import traceback

# --- Configuration Section ---
METADATA_KEYWORDS = {"Responsible", "Accountable", "Consulted", "Informed"}
REVISION_HEADERS = ["Version", "Date", "Description", "Contributor"]


HEADING_REGEX = re.compile(
    r"^(\d+(?:\.\d+)*\s+(?!"
    r"\d+(?:\.\d+)*\s*$|"
    r"and\s+\d+\s*$|"
    r"and\s+Box\s+\d+\s*$|"
    r"with\s+\w+(?:'\w+)?\s*$|"
    r"for\s+\w+\s*$|"
    r"threshold\s+for\s*$|"
    r"\w{3,9}\s+\d{1,2},\s+\d{4}\s*$|"
    r"\d{4}\s+period\s*$|"
    r"Added\s+[A-Z&\s]+\s*$"
    r").{3,})$",
    re.MULTILINE | re.IGNORECASE
)


MAIN_SECTION_REGEX = re.compile(r"^\d+\s")

# Initialize S3 client once for better performance
s3_client = boto3.client("s3")


# --- NO CHANGES TO YOUR CORE LOGIC BELOW THIS LINE ---

def _parse_sections(raw_text):
    headings = []
    for match in HEADING_REGEX.finditer(raw_text):
        headings.append({
            "heading": match.group(1).strip(),
            "start": match.start(),
            "end": match.end()
        })

    content_map = {}
    for i, head in enumerate(headings):
        content_start = head["end"]
        content_end = headings[i + 1]["start"] if i + 1 < len(headings) else len(raw_text)
        content_map[head["heading"]] = raw_text[content_start:content_end].strip()

    return headings, content_map


def _categorize_tables(tables):
    metadata_table = None
    revision_rows = []
    body_tables = []
    is_revision_section = False

    for table in tables:
        rows = table.get("rows", [])
        if not rows:
            continue

        header = rows[0]

        if not metadata_table and any(kw in str(rows) for kw in METADATA_KEYWORDS):
            metadata_table = table
            continue

        if set(header) == set(REVISION_HEADERS):
            is_revision_section = True
            revision_rows.extend(rows[1:])
        elif is_revision_section and len(header) == len(REVISION_HEADERS):
            revision_rows.extend(rows)
        else:
            is_revision_section = False
            body_tables.append(table)

    return metadata_table, revision_rows, body_tables


def SOP_Structure_Formation(sop_data):
    raw_text = sop_data.get("raw_text", "")
    tables = sop_data.get("tables", [])

    metadata_table, revision_table_data, remaining_tables = _categorize_tables(tables)
    headings, content_map = _parse_sections(raw_text)

    structured_body = []
    current_section = None

    for head_info in headings:
        heading = head_info["heading"]
        content_text = content_map.get(heading, "").strip()

        if MAIN_SECTION_REGEX.match(heading):
            if current_section:
                structured_body.append(current_section)
            current_section = {"Section": heading, "Sub-sections": [], "Content": content_text}
        elif current_section:
            current_section["Sub-sections"].append({
                "Sub-section": heading,
                "Content": content_text
            })
    if current_section:
        structured_body.append(current_section)

    final_output = []

    if metadata_table:
        final_output.append({
            "Section": "Document Information",
            "Metadata": {row[0]: row[1] for row in metadata_table.get("rows", []) if len(row) == 2}
        })

    if headings:
        final_output.append({
            "Section": "Table of Contents",
            "Content": [h["heading"] for h in headings]
        })

    final_output.extend(structured_body)

    if revision_table_data:
        final_output.append({
            "Section": "Revision History",
            "Table": [dict(zip(REVISION_HEADERS, row)) for row in revision_table_data]
        })

    if remaining_tables:
        final_output.append({
            "Section": "Extracted Tables",
            "Tables": [{
                "Table #": i + 1,
                "Page": table.get("page"), "Source": table.get("source"), "Rows": table.get("rows")
            } for i, table in enumerate(remaining_tables)]
        })

    return final_output

# --- UPDATED LAMBDA HANDLER SECTION ---
# This handler is now designed to work correctly within the Step Function.

def lambda_handler(event, context):
    """
    Reads the S3 location of the extracted text file from the previous step,
    runs the structuring logic, saves the result to S3, and returns the
    new S3 location.
    """
    try:
        # Step 1: Get the S3 location of the file from the previous Lambda's output.
        # The Step Function passes this as the input 'event'.
        input_bucket = event["extracted_text_output"]["s3_bucket"]
        input_key = event["extracted_text_output"]["s3_key"]

        print(f"Reading input data from: s3://{input_bucket}/{input_key}")

        # Step 2: Read the JSON file from S3 to get the raw text and tables.
        response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
        sop_data_from_s3 = json.loads(response['Body'].read().decode('utf-8'))

        # Step 3: Run your existing core structuring logic on the loaded data.
        # NO CHANGES were made to this function.
        structured_result = SOP_Structure_Formation(sop_data_from_s3)

        # Step 4: Define the output file path.
        # We get the original filename from the data we just loaded from S3.
        sop_filename_base = sop_data_from_s3.get("sop_filename", "unknown.txt").rsplit('.', 1)[0]
        output_filename = f"{sop_filename_base}_processed.json"
        
        output_bucket = "de-processing-bucket"
        output_key = f"processed-sop/{output_filename}"

        # Step 5: Save the structured result to a new file in S3.
        s3_client.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=json.dumps(structured_result, indent=2),
            ContentType="application/json"
        )
        print(f"Successfully saved structured SOP to: s3://{output_bucket}/{output_key}")

        # Step 6: Return ONLY the location of the new file.
        # This small, clean output fixes the error in the next Step Function step.
        return {
            "s3_bucket": output_bucket,
            "s3_key": output_key
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        # Re-raise the exception to make the Step Function task fail correctly.
        raise e


"""
Lambda Test Event
{
  "status": "success",
  "sop_filename": "TEST SoP MR.pdf",
  "extracted_text_output": {
    "s3_bucket": "de-processing-bucket",
    "s3_key": "extracted-text/TEST SoP MR_extracted.json"
  }
}
"""
