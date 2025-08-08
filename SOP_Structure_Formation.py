import re
import boto3
import json

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

# --- Lambda Handler with S3 Upload ---
def lambda_handler(event, context):
    try:
        result = SOP_Structure_Formation(event)

        # Get SOP file name from the event (from Extract_Text_Lambda output)
        sop_filename = event.get("sop_filename", "output")
        output_filename = sop_filename.rsplit(".", 1)[0] + "_processed.json"

        # Upload result to S3
        s3 = boto3.client("s3")
        bucket_name = "de-processing-bucket"
        s3_key = f"processed-sop/{output_filename}"

        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(result, indent=2),
            ContentType="application/json"
        )

        return {
            "status": "success",
            "message": f"Structured SOP exported to s3://{bucket_name}/{s3_key}",
            "s3_output_path": f"s3://{bucket_name}/{s3_key}",
            "structured_sop": result
        }

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return {
            "status": "error",
            "message": str(e)
        }
