import json
import logging
from typing import Dict, Any, List

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PAGE_SIZE = {{vars.page_size}}
URL = "{{ vars.opera_base_url }}{{ vars.graphql_path }}"

GQL_INPUT = json.loads("""{{ inputs.gql_input_json }}""")
QUERY = """{{ inputs.graphql_query }}"""
KEY_PATH = "{{ inputs.record_key_field }}".strip()

HEADERS = {
    "Content-Type": "application/json",
    "x-app-key": "{{ kv('app_key') }}",
    "x-api-key": "{{ kv('api_key_ra') }}",
    "Authorization": "Bearer {{ outputs.get_token.outputs.token }}",
}


def extract_root_field(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract the first root field from a GraphQL response."""

    data = body.get("data")
    if not isinstance(data, dict) or not data:
        raise ValueError("Invalid GraphQL response: missing 'data'")

    root_key = next(iter(data))
    return data[root_key] or []


def execute_query(
        session: requests.Session,
        offset: int,
        page_size: int
) -> List[Dict[str, Any]]:
    """Execute the GraphQL query and return records."""

    payload = {
        "query": QUERY,
        "variables": {
            "limit": page_size,
            "offset": offset,
            "input": GQL_INPUT,
        },
    }

    response = session.post(
        URL,
        json=payload,
        headers=HEADERS,
        timeout=30,
        verify=False,
    )

    response.raise_for_status()

    body = response.json()

    if body.get("errors"):
        raise RuntimeError(f"GraphQL errors: {body['errors']}")

    return extract_root_field(body)


def extract_key(record: Dict[str, Any], path: str) -> str:
    """Extract Kafka key from record using dot-notation path."""

    if not path:
        return ""

    parts = path.split(".")
    val = record

    for p in parts:
        if isinstance(val, dict):
            val = val.get(p, "")
        else:
            return ""

    return str(val)


def fetch_and_transform_records() -> int:
    """Fetch paginated records, transform them, and write to ION format."""

    offset = 0
    total_records = 0

    with requests.Session() as session, open("records.ion", "w") as output_file:

        while True:
            records = execute_query(session, offset, PAGE_SIZE)
            count = len(records)

            logger.info("offset=%s -> %s records", offset, count)

            for record in records:
                # Write in key-value format for Kafka
                output_file.write(json.dumps({
                    "key": extract_key(record, KEY_PATH),
                    "value": record
                }))
                output_file.write("\n")

            total_records += count
            offset += PAGE_SIZE

            if count < PAGE_SIZE:
                break

    return total_records


def main() -> None:
    total = fetch_and_transform_records()
    logger.info("Total records fetched and transformed: %s", total)


if __name__ == "__main__":
    main()
