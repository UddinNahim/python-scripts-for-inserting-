import asyncio
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from psqlpy import ConnectionPool

# Load environment variables
load_dotenv()

async def main() -> None:
    # 1. Configuration Validation
    required_vars = ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_DB"]
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        print(f"Critical Error: Missing environment variables: {', '.join(missing)}")
        return

    # 2. Database Connection Setup
    try:
        db_pool = ConnectionPool(
            username=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            db_name=os.getenv("POSTGRES_DB"),
            max_db_pool_size=2,
        )
        connection = await db_pool.connection()
    except Exception as e:
        print(f"Database Connection Error: {e}")
        return

    # 3. File Loading & Data Cleaning
    excel_path = "excel/data.xlsx"
    if not os.path.exists(excel_path):
        print(f"File Error: The path '{excel_path}' does not exist.")
        return

    try:
        # Load Excel and normalize headers
        df = pd.read_excel(excel_path)
        df.columns = [str(col).lower().strip() for col in df.columns]
        
        # Replace NaNs with None so PostgreSQL receives NULL instead of NaN
        df = df.where(pd.notnull(df), None)
        
        if df.empty:
            print("Warning: Excel file is empty. No rows to process.")
            return
    except Exception as e:
        print(f"Excel Read Error: {e}")
        return

    # 4. Query Construction
    # Wrap column names in double quotes to protect against reserved keywords or spaces
    quoted_cols = [f'"{col}"' for col in df.columns]
    column_names = ", ".join(quoted_cols)
    placeholders = ", ".join([f"${i+1}" for i in range(len(df.columns))])
    insert_query = f"INSERT INTO certificate ({column_names}) VALUES ({placeholders})"

    # 5. Row-by-Row Execution (Skip-on-Error Mode)
    rows = df.values.tolist()
    success_count = 0
    error_count = 0

    print(f"Starting import of {len(rows)} rows...")
    print("-" * 60)

    for i, row_data in enumerate(rows):
        # Excel Row = index + 2 (Header is row 1, first data row is 2)
        excel_row_num = i + 2 
        
        try:
            # Convert row_data to a list to ensure psqlpy handles types correctly
            await connection.execute(insert_query, list(row_data))
            success_count += 1
            
        except Exception as e:
            error_count += 1
            # Extract only the relevant database error message
            clean_error = str(e).split('db error: ')[-1] if 'db error: ' in str(e) else str(e)
            
            print(f"Failed: [Excel Row {excel_row_num}]")
            print(f"   Data : {row_data}")
            print(f"   Error: {clean_error}")
            print("-" * 40)

    # 6. Final Summary report
    print("-" * 60)
    print("FINAL IMPORT SUMMARY")
    print(f"   Successfully Inserted: {success_count}")
    print(f"   Failed/Skipped      : {error_count}")
    print(f"   Total Processed     : {len(rows)}")
    print("-" * 60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")
        sys.exit(0)