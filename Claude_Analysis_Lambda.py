import boto3
import json
import time
import re

bedrock = boto3.client("bedrock-runtime")

CONTROL_ATTRIBUTES = [  
    {
        "attribute": "Risk and Objective Alignment",
        "required": "Do the activities and the parameters of the control align with the objective and risk it is intending to mitigate?",
        "considerations": [
            "Will the control mitigate the risk if performing as intended?",
            "Is the control frequency and control nature (e.g. Preventive or Detective) in line with the risk that it is intended to mitigate?",
            "Does the control cover all intended products and services?"
        ]
    },
    {
        "attribute": "Process Alignment",
        "required": "Is the control being executed at the proper frequency and point in the process to ensure timely detection / prevention of its associated risks?",
        "considerations": [
            "Is the control being performed less frequently when compared to the process/activity?",
            "Is the control occurring within the process at the right time to achieve the objective?"
        ]
    },
    {
        "attribute": "Standards Alignment",
        "required": "Does the control align to any applicable control testing standards, regulatory requirements and relevant policies/procedures?",
        "considerations": [
            "If applicable, is the control aligned to outdated polices/procedures?",
            "Has there been new regulatory guidance that would be applicable to the control?"
        ]
    },
    {
        "attribute": "Documentation",
        "required": "Are the 5 W's ('Who', 'What', 'When', 'Where', 'Why') clearly articulated in the control descriptions or other dedicated fields?",
        "considerations": [
            "Who - who performs the control",
            "What - what control activity is performed that helps achieve the objective",
            "When - when the control is performed",
            "Where - where a control is positioned in the business process. Where it is performed, where it is evidenced",
            "Why - why the control is performed"
        ]
    },
    {
        "attribute": "Data Integrity",
        "required": "Is the data, information and/or systems in which the control is dependent upon free of any known issues? Does the control owner/performer have the ability to provide the assurance the data utilized is complete and accurate for conducting the control?",
        "considerations": [
            "Consider whether the data or information elements used are appropriate to achieve the control objective.",
            "Is the control leveraging the right data/information source to perform the control?",
            "Are the appropriate data points or pieces of information from the source system being used to perform the control?",
            "Can the data be relied upon to perform the control?"
        ]
    },
    {
        "attribute": "Roles and Responsibilities",
        "required": "Do the control owners/performers have defined roles and responsibilities, including independence or segregation of duties? Is the control performed, reviewed, and monitored by sufficient qualified and authorized individuals?",
        "considerations": [
            "Consider whether all roles and responsibilities around the control activity are clearly defined.",
            "Does control performer know what is expected of them?",
            "Are the various accountabilities for the control clearly defined? (i.e. who is responsible for each part of the control activity)",
            "Is the control operator sufficiently independent from the process owner in the process requiring maker/checker roles?"
        ]
    },
    {
        "attribute": "Reporting",
        "required": "Does the activity have an established reporting process in place for results and does it include notifying the appropriate delivery stakeholders timely?",
        "considerations": [
            "For Controls requiring communication of the outcome of the control activity to fully mitigate the risk, is the control reporting timely to meet the desired objective?",
            "Is the communication being delivered to the correct stakeholders?"
        ]
    },
    {
        "attribute": "Escalation",
        "required": "Does the activity have an established escalation process in place to address any out of tolerance / compliance conditions and does it include notifying the appropriate delivery stakeholders timely?",
        "considerations": [
            "Does the control activity have a process in place to escalate items that are deemed to not be in tolerance or that are to be actioned by the business for failure?",
            "Are items tracked and monitored until they are appropriately addressed or actioned?",
            "Consider if the escalation process includes notifying the right stakeholders.",
            "Consider if the escalation is being conducted in a timely manner"
        ]
    },
    {
        "attribute": "Sustainability",
        "required": "Is the control activity built in a manner that is clear & sufficient to allow repeatable execution of the control on an ongoing basis?",
        "considerations": [
            "Relative to the complexity of the control, are pertinent details of the control activity documented such that current employees can leverage the documentation as a reference to perform the control?",
            "Does the documentation include enough detail for the control performer or is it written at a very high level and is therefore not likely to be useful as a reference?",
            "Consider whether the control can continue to operate in the event of turnover or a change in processes?",
            "Consider whether the control can be repeatably executed in the case that volume increases",
            "Are there defined measures of sustainability and effectiveness?"
        ]
    },
    {
        "attribute": "Verifiability",
        "required": "Is the activity designed in order to produce & retain appropriate documentation/evidence in order to validate that the control was performed and therefore could be re-performed, if necessary?",
        "considerations": [
            "Consider whether a third party can verify that the control was performed",
            "Consider whether appropriate documentation or records are retained. For example, could a third party leverage the data retained and reperform the control?"
        ]
    }
]

def lambda_handler(event, context):
    try:
        sop_text = event['extractedText'][:15000]
        grouped_roles = event.get('groupedResponsibilities', {})

        formatted_results = []

        for attr in CONTROL_ATTRIBUTES:
            prompt = build_prompt(attr, sop_text, grouped_roles)

            body = {
                "prompt": f"\n\nHuman:\n{prompt}\n\nAssistant:",
                "max_tokens_to_sample": 2000,
                "temperature": 0.5,
                "top_k": 250,
                "top_p": 0.9,
                "stop_sequences": [],
                "anthropic_version": "bedrock-2023-05-31"
            }

            response = bedrock.invoke_model(
                modelId="anthropic.claude-v2:1",
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body)
            )

            response_body = json.loads(response['body'].read().decode())
            raw_output = response_body['completion'].strip()

            formatted_results.append(
                format_claude_output(attr, raw_output)
            )

            time.sleep(0.5)  # avoid throttling

        return {
            "status": "success",
            "results": formatted_results
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

def build_prompt(attribute_obj, sop_text, grouped_roles):
    return f"""
You are a control testing expert.
Analyze the following SOP and provide precise, professional responses for ONE control attribute. Try to answer all required questions and considerations questions.

Attribute: {attribute_obj['attribute']}

Required Question:
{attribute_obj['required']}

Consideration Questions:
{json.dumps(attribute_obj['considerations'], indent=2)}

SOP Text:
\"\"\"
{sop_text}
\"\"\"

Grouped Responsibilities (for context):
{json.dumps(grouped_roles, indent=2)}

Respond only for this attribute with answers to:
- Required Question
- Each Consideration
- SOP Section or line used as Evidence
- A professional Summary Comment 
- If no relevant information is found in the SOP for a specific question, you may say: "Related answer not found in the SOP." — but only if the topic is truly not covered at all.
"""

def format_claude_output(attr_obj, raw_output):
    """
    Extract structured fields from Claude output using regex parsing
    """
    required_q = attr_obj["required"]
    required_a_match = re.search(r"Required Question:\s*(.*?)\n+(.*?)\n", raw_output, re.DOTALL)
    required_answer = required_a_match.group(2).strip() if required_a_match else ""

    considerations = []
    for question in attr_obj["considerations"]:
        pattern = re.compile(re.escape(question) + r"\s*[:\-]?\s*\n(.*?)\n(?:\n|Consideration Question|\Z)", re.DOTALL)
        match = pattern.search(raw_output)
        answer = match.group(1).strip() if match else ""
        considerations.append({
            "question": question,
            "answer": answer
        })

    # Extract evidence
    evidence_match = re.search(r"Evidence\s*[:\-]?\s*\n(.*?)\n(?:\n|Summary Comment|\Z)", raw_output, re.DOTALL)
    evidence = evidence_match.group(1).strip() if evidence_match else ""

    # Extract summary comment
    summary_match = re.search(r"Summary Comment\s*[:\-]?\s*\n?(.*)", raw_output, re.DOTALL)
    summary_comment = summary_match.group(1).strip() if summary_match else ""

    return {
        "attribute": attr_obj["attribute"],
        "required_question": required_q,
        "required_answer": required_answer,
        "considerations": considerations,
        "evidence": evidence,
        "summary_comment": summary_comment
    }
