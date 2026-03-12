#!/usr/bin/env python3
import asyncio
import json
import os
import csv
from dotenv import load_dotenv
import asyncpg

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), "Digital_Ocean_backend", ".env")
load_dotenv(env_path)

async def bfs_key_extraction():
    db_params = {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME")
    }

    print(f"🔍 Connecting to {db_params['host']}...")
    try:
        conn = await asyncpg.connect(**db_params)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    try:
        print("📊 BFS Key Extraction (Zero-Download Method)...")
        
        # unique_keys will store { "path": sample_value }
        unique_keys = {}
        
        # Queue for paths to explore: list of (sql_path_parts, display_path)
        # sql_path_parts is a list of keys for -> operator
        queue = [([], "")]

        visited_paths = set()

        while queue:
            sql_parts, display_prefix = queue.pop(0)
            
            # Construct the SQL fragment for the current path
            # For JSONB, we use 'payload -> key1 -> key2 ...'
            if not sql_parts:
                target_json = "payload"
            else:
                target_json = "payload" + "".join(f" -> '{p}'" for p in sql_parts)

            print(f"  📂 Exploring: {display_prefix if display_prefix else 'root'}")

            # 1. Get all unique keys at this level
            key_query = f"SELECT DISTINCT jsonb_object_keys({target_json}) FROM webhook_events WHERE jsonb_typeof({target_json}) = 'object';"
            try:
                rows = await conn.fetch(key_query)
                current_keys = [r[0] for r in rows]
            except Exception as e:
                # This might happen if target_json is not an object in some rows, which WHERE should handle
                continue

            for key in current_keys:
                full_display_path = f"{display_prefix}.{key}" if display_prefix else key
                
                if full_display_path in visited_paths:
                    continue
                visited_paths.add(full_display_path)

                # Get a sample value for this key
                # We also check the type to see if we should recurse
                sample_query = f"""
                    SELECT 
                        {target_json} -> '{key}' as val,
                        jsonb_typeof({target_json} -> '{key}') as type
                    FROM webhook_events 
                    WHERE {target_json} ? '{key}' 
                    LIMIT 1;
                """
                sample_row = await conn.fetchrow(sample_query)
                
                if sample_row:
                    val = sample_row['val']
                    vtype = sample_row['type']
                    
                    # Store key and sample
                    unique_keys[full_display_path] = val
                    
                    # If it's an object, add to queue
                    if vtype == 'object':
                        queue.append((sql_parts + [key], full_display_path))
                    
                    # If it's a list, we explore the first item structure
                    elif vtype == 'array':
                        # Special handling for lists: path -> 0
                        # But wait, we should check if it has objects inside
                        list_check_query = f"SELECT jsonb_typeof({target_json} -> '{key}' -> 0) FROM webhook_events WHERE jsonb_array_length({target_json} -> '{key}') > 0 LIMIT 1;"
                        list_type_row = await conn.fetchrow(list_check_query)
                        
                        list_display_path = f"{full_display_path}[0]"
                        unique_keys[list_display_path] = "Structure exploration..." # placeholder
                        
                        if list_type_row and list_type_row[0] == 'object':
                             queue.append((sql_parts + [key, 0], list_display_path))

        print(f"✅ Finished! Found {len(unique_keys)} unique keys across all levels.")
        
        output_file = "all_webhook_unique_keys.csv"
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["json_key", "sample_value"])
            for key in sorted(unique_keys.keys()):
                val = unique_keys[key]
                val_str = json.dumps(val) if isinstance(val, (dict, list)) else str(val)
                writer.writerow([key, val_str])
        
        print(f"💾 Results saved to {output_file}")

    except Exception as e:
        print(f"❌ Error during BFS: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(bfs_key_extraction())
