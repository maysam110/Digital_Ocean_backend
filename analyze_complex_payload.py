#!/usr/bin/env python3
import asyncio
import json
import os
import csv
import sys
from dotenv import load_dotenv
import asyncpg

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), "Digital_Ocean_backend", ".env")
load_dotenv(env_path)

def extract_flat_mapping(data, parent_key=""):
    """
    Recursively extracts all keys and values from a JSON structure.
    Returns a list of tuples [(flattened_key, value), ...]
    """
    mapping = []
    
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            mapping.append((new_key, value))
            mapping.extend(extract_flat_mapping(value, new_key))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}[{i}]"
            mapping.append((new_key, item))
            mapping.extend(extract_flat_mapping(item, new_key))
            
    return mapping

async def analyze_complex_payload():
    # Connection parameters from .env
    db_params = {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME")
    }

    if not all([db_params["user"], db_params["database"]]):
        print("❌ Error: DB_USER or DB_NAME not found in .env file.")
        return

    print(f"🔍 Connecting to database '{db_params['database']}'...")
    try:
        conn = await asyncpg.connect(**db_params)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    try:
        print("📊 Scanning webhook_events table for the most complex payload (max keys)...")
        print("💡 Using cursor to handle large table efficiently.")
        
        max_keys = -1
        complex_payload = None
        best_row_info = {}

        scanned_count = 0
        
        # Use a transaction and cursor for one-by-one processing
        async with conn.transaction():
            async for row in conn.cursor("SELECT id, provider, event_type, payload FROM webhook_events"):
                scanned_count += 1
                payload = row['payload']
                
                if scanned_count % 1000 == 0:
                    print(f"  ...scanned {scanned_count} rows")

                # Handle if payload is double-encoded string
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except:
                        continue
                
                # Get flat mapping to count keys
                flat_map = extract_flat_mapping(payload)
                key_count = len(flat_map)
                
                if key_count > max_keys:
                    max_keys = key_count
                    complex_payload = payload
                    best_row_info = {
                        "id": row.get('id'),
                        "provider": row['provider'],
                        "event_type": row['event_type']
                    }

        if complex_payload is None:
            print(f"⚠️  No valid JSON payloads found after scanning {scanned_count} rows.")
            return

        print(f"✅ Finished scanning {scanned_count} rows.")
        print(f"🏆 Most complex payload found in row (ID: {best_row_info['id']})")
        print(f"📈 Total unique keys (including nested): {max_keys}")

        # Final extraction for the winner
        final_mapping = extract_flat_mapping(complex_payload)
        output_file = "most_complex_payload_mapping.csv"
        
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["json_key", "value"])
            for key, value in final_mapping:
                # Stringify value if it's complex to keep CSV clean
                val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
                writer.writerow([key, val_str])

        print(f"💾 Results exported to: {output_file}")
        print("="*50)
        print(f"Quick Preview (First 5 keys):")
        for k, v in final_mapping[:5]:
            print(f"  {k}: {v}")
        print("="*50)

    except Exception as e:
        print(f"❌ Error during analysis: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(analyze_complex_payload())
