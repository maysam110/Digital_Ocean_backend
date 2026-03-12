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

async def print_unique_keys_hierarchy():
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
        print(f"📊 Scanning 'webhook_events' Table...")
        print("⚡ Exploring all nesting levels (BFS SQL Method)...")
        
        # unique_keys will store { "path": sample_value }
        unique_keys = {}
        queue = [([], "")]
        visited_paths = set()

        while queue:
            sql_parts, display_prefix = queue.pop(0)
            
            # Construct JSONB target path
            if not sql_parts:
                target_json = "payload"
            else:
                target_json = "payload" + "".join(f" -> '{p}'" for p in sql_parts)

            # Get all unique keys at this level
            key_query = f"""
                SELECT DISTINCT jsonb_object_keys({target_json}) 
                FROM webhook_events 
                WHERE jsonb_typeof({target_json}) = 'object';
            """
            try:
                rows = await conn.fetch(key_query)
                current_keys = [r[0] for r in rows]
            except:
                continue

            for key in current_keys:
                full_path = f"{display_prefix}.{key}" if display_prefix else key
                
                if full_path in visited_paths:
                    continue
                visited_paths.add(full_path)

                # Get sample value and type
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
                    vtype = sample_row['type']
                    unique_keys[full_path] = (sample_row['val'], vtype)
                    
                    if vtype == 'object':
                        queue.append((sql_parts + [key], full_path))
                    elif vtype == 'array':
                        # Check first element for structural recursion
                        list_type_query = f"""
                            SELECT jsonb_typeof({target_json} -> '{key}' -> 0) 
                            FROM webhook_events 
                            WHERE jsonb_array_length({target_json} -> '{key}') > 0 
                            LIMIT 1;
                        """
                        list_type_row = await conn.fetchrow(list_type_query)
                        if list_type_row and list_type_row[0] == 'object':
                             queue.append((sql_parts + [key, 0], f"{full_path}[0]"))

        print("="*60)
        print(f"✅ Scanning complete! Found {len(unique_keys)} Total Unique Key Paths.")
        print("="*60)
        print("LIST OF ALL UNIQUE KEYS (Hierarchical):")
        print("="*60)

        # Sort keys to print them somewhat hierarchically (alphabetical by segments)
        sorted_keys = sorted(unique_keys.keys())

        for path in sorted_keys:
            # Count the dots to indent, but ignoring the [0] part for simpler visual level mapping
            clean_path = path.replace("[0]", "")
            level = clean_path.count(".")
            indent = "  " * level
            
            # Determine how to display the sample value
            val, vtype = unique_keys[path]
            val_preview = ""
            if vtype in ('object', 'array'):
                val_preview = f"<{vtype}>"
            else:
                val_preview = str(val)[:50] + "..." if len(str(val)) > 50 else str(val)

            print(f"{indent}🔹 {path: <{50 - (len(indent))}} | Value: {val_preview}")

        print("="*60)
        print(f"📊 Summary: {len(unique_keys)} Unique paths identified across all payloads.")
        print("="*60)

    except Exception as e:
        print(f"❌ Error during extraction: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(print_unique_keys_hierarchy())
