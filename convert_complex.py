import pandas as pd
import csv
import io
import argparse  # Import argparse

# --- Helper Functions ---


def wrap_key_or_value(val_str):
    """Wraps a simple string in the 'VString' JSON format."""
    if pd.isna(val_str):
        return ""
    val_str = str(val_str).replace('"', '\\"')
    return f'{{"type":"VString","value":"{val_str}"}}'


def wrap_get_output(output_str):
    """Wraps a GET operation's output in the 'VOption' JSON format."""
    if pd.isna(output_str) or output_str == "NotFound":
        return '{"type":"VOption","value":null}'
    else:
        wrapped_val = wrap_key_or_value(output_str)
        return f'{{"type":"VOption","value":{wrapped_val}}}'


def get_op_output():
    """Returns the expected output for a successful PUT or DELETE."""
    # We assume DELETE, like PUT, returns an "OK" tuple.
    return '{"type":"VTuple","value":[]}'


# --- Main Conversion Logic ---

# 1. Set up command-line argument parsing
parser = argparse.ArgumentParser(
    description="Convert a simple history CSV to the spec format."
)
parser.add_argument(
    "-i",
    "--input",
    required=True,
    help="Path to the input CSV file (e.g., complex_history.csv)",
)
parser.add_argument(
    "-o",
    "--output",
    required=True,
    help="Path for the converted output CSV file (e.g., complex_history_converted.csv)",
)
args = parser.parse_args()

# Use the filenames from the arguments
input_filename = args.input
output_filename = args.output

try:
    # 2. Load the input CSV from the provided filename
    df = pd.read_csv(input_filename)
    df.columns = [col.strip().lower() for col in df.columns]

    print(f"--- Loaded {input_filename} ---")
    print(f"Found {len(df)} operations.")

    # 3. Iterate and transform
    new_rows = []
    target_headers = [
        "UniqueID",
        "ClientID",
        "Kind",
        "Action",
        "Payload1",
        "Payload2",
        "Payload3",
    ]

    # Use row index as the UniqueID
    for i, row in df.iterrows():
        uid = i
        client_id = row["client_id"]
        op = str(row["op"]).upper().strip()

        action, p1_inv, p2_inv, p3_inv = "", "", "", ""
        p1_resp, p2_resp, p3_resp = "", "", ""

        if op == "PUT":
            action = "Client.Write"
            p2_inv = wrap_key_or_value(row["key"])
            p3_inv = wrap_key_or_value(row["value"])
            p1_resp = get_op_output()

        elif op == "GET":
            action = "Client.Read"
            p2_inv = wrap_key_or_value(row["key"])
            p1_resp = wrap_get_output(row["output"])

        elif op == "DELETE":
            action = "Client.Delete"  # Assigning a new op code
            p2_inv = wrap_key_or_value(row["key"])
            p1_resp = get_op_output()  # Assuming DELETE returns "OK"

        else:
            print(f"Warning: Skipping unknown operation '{op}' at row {i}")
            continue

        # Add the Invocation row
        new_rows.append([uid, client_id, "Invocation", action, p1_inv, p2_inv, p3_inv])
        # Add the Response row
        new_rows.append([uid, client_id, "Response", action, p1_resp, p2_resp, p3_resp])

    # 4. Create new DataFrame and save to CSV
    converted_df = pd.DataFrame(new_rows, columns=target_headers)

    converted_df.to_csv(output_filename, index=False, quoting=csv.QUOTE_MINIMAL)

    print(f"\n--- Conversion Successful ---")
    print(f"Converted {len(df)} operations into {len(converted_df)} event rows.")
    print(f"Saved to: {output_filename}")

except FileNotFoundError:
    print(f"ERROR: Input file not found at {input_filename}")
except Exception as e:
    print(f"An error occurred: {e}")
