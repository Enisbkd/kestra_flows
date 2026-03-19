#!/usr/bin/env python3
"""
For each R&A txt file, generates:
  1. A numbered .graphql file  (e.g. graphql/01_NameNotes.graphql)
  2. A numbered Kestra YAML    (e.g. kestra/op_01_name_notes.yaml)

Usage:
    python generate_files.py <input_dir> <output_dir>
"""

import os
import re
import sys
import json
import textwrap

# ─────────────────────────────────────────────────────────────────────────────
# SECTION EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# TOPIC / FILENAME
# ─────────────────────────────────────────────────────────────────────────────

def topic_name_from_table(table, num):
    """
    TABLE metadata → PascalCase label.
    NAME_RESORT → NameResort,  EVENT$TYPES → EventTypes
    """
    parts = re.split(r'[^A-Za-z0-9]+', table)
    pascal = "".join(p[0].upper() + p[1:].lower() for p in parts if p)
    return f"{num}_{pascal}"


# ─────────────────────────────────────────────────────────────────────────────
# VARIABLE DISCOVERY & SUBSTITUTION
#
# Strategy:
#   1. Scan the raw variables section string for ALL {{Placeholder}} tokens.
#   2. Build mapping: placeholder → kestra_input_id
#   3. Extract the "input" block from the JSON (gracefully), re-dump it,
#      then substitute placeholders in that string.
#
# This means discovery always uses the raw text (never fails silently),
# and substitution operates on the clean re-dumped input block.
# ─────────────────────────────────────────────────────────────────────────────

VARIABLE_ALIASES = {
    "frombusinessdate": "from_update_date",
    "tobusinessdate":   "to_update_date",
}

def camel_to_snake(name):
    """HotelId → hotel_id,  ChainCode → chain_code"""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return s.lower()

def discover_variables(raw_text):
    """
    Scan raw text for ALL {{Placeholder}} tokens and return ordered mapping:
      { "HotelId": "hotel_id", "FromBusinessDate": "from_update_date", ... }
    Uses the raw string directly — never fails on bad JSON.
    """
    placeholders = re.findall(r'\{\{([A-Za-z0-9_]+)\}\}', raw_text)
    mapping = {}
    for v in placeholders:
        if v in mapping:
            continue  # deduplicate, preserve first-seen order
        alias_key = v.lower()
        kestra_id = VARIABLE_ALIASES.get(alias_key, camel_to_snake(v))
        mapping[v] = kestra_id
    return mapping

def extract_input_block(variables_raw):
    """
    Parse the variables JSON and return the "input" sub-object as a
    pretty-printed JSON string. Falls back to the raw text on parse error.
    """
    try:
        parsed = json.loads(variables_raw)
        input_block = parsed.get("input", {})
        return json.dumps(input_block, indent=2)
    except Exception:
        # Fallback: strip the outer wrapper heuristically
        m = re.search(r'"input"\s*:\s*(\{.*\})', variables_raw, re.DOTALL)
        if m:
            try:
                return json.dumps(json.loads(m.group(1)), indent=2)
            except Exception:
                return m.group(1).strip()
        return variables_raw  # last resort: return whole thing

def substitute_variables(text, var_mapping):
    """Replace every {{Placeholder}} with {{inputs.<kestra_id>}}."""
    for placeholder, kestra_id in var_mapping.items():
        text = text.replace(f'{{{{{placeholder}}}}}', f'{{{{inputs.{kestra_id}}}}}')
    return text


# ─────────────────────────────────────────────────────────────────────────────
# RECORD KEY
# ─────────────────────────────────────────────────────────────────────────────

def derive_record_key(variables_raw, response_raw):
    # Prefer *Id or *Key fields found in the response JSON
    id_fields = re.findall(r'"(\w+(?:Id|Key))"\s*:', response_raw)
    if id_fields:
        return id_fields[0]
    # Fallback: look in the input variable keys
    try:
        v = json.loads(variables_raw)
        for k in v.get("input", {}).keys():
            if re.search(r'(Id|Key|Code)$', k, re.IGNORECASE):
                return k
    except Exception:
        pass
    return "id"


# ─────────────────────────────────────────────────────────────────────────────
# INPUT DEFAULTS
# Add new {{Placeholder}} defaults here as you encounter them.
# ─────────────────────────────────────────────────────────────────────────────

INPUT_DEFAULTS = {
    "from_update_date": ("STRING", "2021-01-01 00:00:00"),
    "to_update_date":   ("STRING", "2028-01-01 00:00:00"),
    "chain_code":       ("STRING", "SBMDEVE"),
    "hotel_id":         ("STRING", "HPDEV"),
    # Add more as needed:
    # "resort_id":      ("STRING", "MYRESORT"),
}

def input_default(kestra_id):
    return INPUT_DEFAULTS.get(kestra_id, ("STRING", ""))


# ─────────────────────────────────────────────────────────────────────────────
# YAML BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def build_yaml(flow_id, var_mapping, kafka_topic_str, record_key, gql_input_str, gql_filename):
    lines = [
        f"id: {flow_id}",
        f"namespace: opera.cloud",
        f"inputs:",
    ]

    for placeholder, kestra_id in var_mapping.items():
        typ, default = input_default(kestra_id)
        lines.append(f"  - id: {kestra_id}")
        lines.append(f"    type: {typ}")
        lines.append(f'    defaults: "{default}"')

    lines += [
        f"tasks:",
        f"  - id: run",
        f"    type: io.kestra.plugin.core.flow.Subflow",
        f"    namespace: opera.cloud",
        f"    flowId: op_02_generic_fetch_and_publish",
        f"    wait: true",
        f"    inputs:",
    ]

    for placeholder, kestra_id in var_mapping.items():
        lines.append(f'      {kestra_id}: "{{{{inputs.{kestra_id}}}}}"')

    lines.append(f'      kafka_topic: "{kafka_topic_str}"')
    lines.append(f'      record_key_field: "{record_key}"')
    lines.append(f"      gql_input_json: |")
    lines.append(textwrap.indent(gql_input_str, "        "))
    lines.append(f"      graphql_query: \"{{{{ read('graphql/{gql_filename}') }}}}\"")

    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────────────────────────────────────
# SLUGIFY
# ─────────────────────────────────────────────────────────────────────────────

def slugify(name):
    return re.sub(r'[^a-z0-9]+', '_', name.strip().lower()).strip('_')


# ─────────────────────────────────────────────────────────────────────────────
# PROCESS ONE FILE
# ─────────────────────────────────────────────────────────────────────────────

def process_file(filepath, index, graphql_out_dir, kestra_out_dir):
    with open(filepath, encoding="utf-8-sig") as f:
        text = f.read()

    meta          = extract_metadata(text)
    table         = meta.get("TABLE", "UNKNOWN")
    family        = meta.get("R&A FAMILY", "opera")

    gql_query     = extract_section(text, "GRAPHQL QUERY")
    variables_raw = extract_section(text, "VARIABLES (JSON)")
    response_raw  = extract_section(text, "RESPONSE (EXAMPLE)")

    num           = f"{index:02d}"
    topic_label   = topic_name_from_table(table, num)
    gql_filename  = f"{topic_label}.graphql"
    flow_id       = f"op_{slugify(topic_label)}"
    yaml_filename = f"{flow_id}.yaml"

    # Discover variables from RAW text (never silently empty)
    var_mapping   = discover_variables(variables_raw)

    # Build gql_input_json: extract input block, then substitute
    input_block_str = extract_input_block(variables_raw)
    gql_input_str   = substitute_variables(input_block_str, var_mapping)

    record_key    = derive_record_key(variables_raw, response_raw)
    topic_str     = f"opera.{topic_label}"

    # 1. Write .graphql
    with open(os.path.join(graphql_out_dir, gql_filename), "w", encoding="utf-8") as f:
        f.write(gql_query + "\n")

    # 2. Write Kestra YAML
    yaml_content = build_yaml(
        flow_id, var_mapping, topic_str, record_key, gql_input_str, gql_filename
    )
    with open(os.path.join(kestra_out_dir, yaml_filename), "w", encoding="utf-8") as f:
        f.write(yaml_content)

    return gql_filename, yaml_filename


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

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
            gql_f, yaml_f = process_file(
                os.path.join(input_dir, fname), i, graphql_out, kestra_out
            )
            print(f"  [{i:02d}] {fname}")
            print(f"        -> graphql/{gql_f}")
            print(f"        -> kestra/{yaml_f}")
        except Exception as e:
            print(f"  [{i:02d}] ERROR processing {fname}: {e}")
            import traceback; traceback.print_exc()
            errors += 1

    print(f"\nDone. {len(txt_files)-errors} OK, {errors} error(s). Output -> {output_dir}")

if __name__ == "__main__":
    main()