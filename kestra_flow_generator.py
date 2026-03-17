#!/usr/bin/env python3
"""
For each R&A txt file, generates:
  1. A numbered .graphql file  (e.g. graphql/01_NAME.graphql)
  2. A numbered Kestra YAML    (e.g. kestra/op_01_name.yaml)

Usage:
    python generate_files.py <input_dir> <output_dir>
"""

import os
import re
import sys
import json
import textwrap

def extract_section(text, section_title):
    pattern = (
            r"={4,}[ \t]*\r?\n"
            r"[ \t]*" + re.escape(section_title) + r"[ \t]*\r?\n"
                                                   r"={4,}[ \t]*\r?\n"
                                                   r"(.*?)"
                                                   r"(?===|\Z)"
    )
    m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else ""

def extract_metadata(text):
    meta = {}
    for line in text.splitlines():
        line = line.strip()
        if ":" in line and not line.startswith("{") and not line.startswith('"'):
            key, _, value = line.partition(":")
            meta[key.strip()] = value.strip()
    return meta

def derive_record_key(variables_json, response_json_text):
    id_fields = re.findall(r'"(\w+(?:Id|Key))"\s*:', response_json_text)
    if id_fields:
        return id_fields[0]
    try:
        v = json.loads(variables_json)
        for k in v.get("input", {}).keys():
            if re.search(r'(Id|Key|Code)$', k, re.IGNORECASE):
                return k
    except Exception:
        pass
    return "id"

def build_gql_input_json(variables_json):
    try:
        v = json.loads(variables_json)
        input_block = v.get("input", {})
    except Exception:
        input_block = {}
    raw = json.dumps(input_block, indent=2)
    raw = raw.replace('"{{ChainCode}}"',        '"{{inputs.chain_code}}"')
    raw = raw.replace('"{{FromBusinessDate}}"', '"{{inputs.from_update_date}}"')
    raw = raw.replace('"{{ToBusinessDate}}"',   '"{{inputs.to_update_date}}"')
    return raw

def kafka_topic_from_family(family):
    clean = re.sub(r"[Rr]&[Aa]\s*", "", family).strip()
    word = clean.split()[0] if clean else family
    return f"opera.{word.lower()}"

def slugify(name):
    return re.sub(r'[^a-z0-9]+', '_', name.strip().lower()).strip('_')

def sanitize_upper(name):
    return re.sub(r'[^A-Z0-9_]+', '_', name.strip().upper()).strip('_')

def process_file(filepath, index, graphql_out_dir, kestra_out_dir):
    with open(filepath, encoding="utf-8-sig") as f:
        text = f.read()

    meta          = extract_metadata(text)
    table         = meta.get("TABLE", "UNKNOWN")
    family        = meta.get("R&A FAMILY", "opera")

    gql_query     = extract_section(text, "GRAPHQL QUERY")
    variables_raw = extract_section(text, "VARIABLES (JSON)")
    response_raw  = extract_section(text, "RESPONSE (EXAMPLE)")

    slug          = slugify(table)
    num           = f"{index:02d}"
    flow_id       = f"op_{num}_{slug}"
    table_safe    = sanitize_upper(table)
    gql_filename  = f"{num}_{table_safe}.graphql"
    yaml_filename = f"{flow_id}.yaml"

    kafka_topic   = kafka_topic_from_family(family)
    gql_input     = build_gql_input_json(variables_raw)
    record_key    = derive_record_key(variables_raw, response_raw)

    # 1. Write .graphql
    with open(os.path.join(graphql_out_dir, gql_filename), "w", encoding="utf-8") as f:
        f.write(gql_query + "\n")

    # 2. Write Kestra YAML
    gql_input_indented = textwrap.indent(gql_input, "        ")

    lines = [
        f"id: {flow_id}",
        f"namespace: opera.cloud",
        f"inputs:",
        f"  - id: chain_code",
        f"    type: STRING",
        f"    defaults: SBMDEVE",
        f"  - id: from_update_date",
        f"    type: STRING",
        f'    defaults: "2021-01-01 00:00:00"',
        f"  - id: to_update_date",
        f"    type: STRING",
        f'    defaults: "2028-01-01 00:00:00"',
        f"tasks:",
        f"  - id: run",
        f"    type: io.kestra.plugin.core.flow.Subflow",
        f"    namespace: opera.cloud",
        f"    flowId: op_02_generic_fetch_and_publish",
        f"    wait: true",
        f"    inputs:",
        f'      chain_code: "{{{{inputs.chain_code}}}}"',
        f'      from_update_date: "{{{{ inputs.from_update_date }}}}"',
        f'      to_update_date: "{{{{ inputs.to_update_date }}}}"',
        f'      kafka_topic: "{kafka_topic}"',
        f'      record_key_field: "{record_key}"',
        f"      gql_input_json: |",
        gql_input_indented,
        f"      graphql_query: \"{{{{ read('graphql/{gql_filename}') }}}}\"",
    ]

    with open(os.path.join(kestra_out_dir, yaml_filename), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return gql_filename, yaml_filename, table

def main():
    input_dir  = sys.argv[1] if len(sys.argv) > 1 else "/mnt/user-data/uploads"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/mnt/user-data/outputs"

    graphql_out = os.path.join(output_dir, "graphql")
    kestra_out  = os.path.join(output_dir, "kestra")
    os.makedirs(graphql_out, exist_ok=True)
    os.makedirs(kestra_out,  exist_ok=True)

    txt_files = sorted([f for f in os.listdir(input_dir) if f.lower().endswith(".txt")])
    if not txt_files:
        print(f"No .txt files found in {input_dir}")
        return

    print(f"Found {len(txt_files)} file(s). Processing...\n")
    errors = 0
    for i, fname in enumerate(txt_files, start=1):
        try:
            gql_f, yaml_f, table = process_file(
                os.path.join(input_dir, fname), i, graphql_out, kestra_out
            )
            print(f"  [{i:02d}] {fname}")
            print(f"        -> graphql/{gql_f}")
            print(f"        -> kestra/{yaml_f}")
        except Exception as e:
            print(f"  [{i:02d}] ERROR processing {fname}: {e}")
            errors += 1

    print(f"\nDone. {len(txt_files)-errors} OK, {errors} error(s). Output -> {output_dir}")

if __name__ == "__main__":
    main()