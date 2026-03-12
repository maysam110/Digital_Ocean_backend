#!/usr/bin/env python3
import asyncio
import json
import os
import sys
from dotenv import load_dotenv
import asyncpg

# Load environment variables from the Digital_Ocean_backend/.env file
env_path = os.path.join(os.path.dirname(__file__), "Digital_Ocean_backend", ".env")
load_dotenv(env_path)

async def find_largest_payload():
    """
    Connects to the local PostgreSQL database and retrieves the row 
    with the largest JSON payload in the webhook_events table.
    """
    
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
        print(f"Looked at: {env_path}")
        return

    print(f"🔍 Connecting to {db_params['host']}:{db_params['port']} (Database: {db_params['database']})...")

    try:
        conn = await asyncpg.connect(**db_params)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    try:
        print("📊 Scanning webhook_events table...")
        
        # 1. Get total row count
        total_rows = await conn.fetchval("SELECT count(*) FROM webhook_events;")
        
        # 2. Find largest payload
        query = """
            SELECT 
                provider, 
                event_type, 
                external_event_id, 
                payload, 
                octet_length(payload::text) as size_bytes 
            FROM webhook_events 
            ORDER BY octet_length(payload::text) DESC 
            LIMIT 1;
        """
        
        row = await conn.fetchrow(query)

        if not row:
            print(f"📭 The webhook_events table is empty (0 rows found).")
            return

        print("\n" + "="*50)
        print("🏆 DATABASE SCAN RESULTS")
        print("="*50)
        print(f"📈 Total Rows Scanned: {total_rows:,}")
        print(f"📍 Largest Provider:  {row['provider']}")
        print(f"📝 Event Type:        {row['event_type']}")
        print(f"🆔 External ID:       {row['external_event_id']}")
        print(f"⚖️  Payload Size:      {row['size_bytes'] / 1024:.2f} KB")
        print("-"*50)
        print("📄 PAYLOAD CONTENT (LARGEST):")
        print(json.dumps(row['payload'], indent=4))
        print("="*50)

    except Exception as e:
        print(f"❌ Query failed: {e}")
    finally:
        await conn.close()
        print("\n🔌 Database connection closed.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(find_largest_payload())
