from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import os

def insert_reviews_data_into_postgres(**kwargs):
    """Insert reviews data from CSV into PostgreSQL database - Optimized Version"""
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='combine_csv_files')
    
    if not csv_file_path:
        raise ValueError("No CSV file path found from combine_csv_files task")
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

    postgres_hook = PostgresHook(postgres_conn_id='reviews_connection')
    
    # Table name and columns
    table_name = 'reviews_table'
    target_fields = ['bank_name', 'address', 'author', 'rating', 'review_date', 'review_text', 'source_url']
    
    try:
        rows_to_insert = []
        
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header row
            
            for row_num, row in enumerate(reader, start=1):
                if len(row) < 7:  # Ensure we have enough columns
                    print(f"Skipping incomplete row {row_num}: {row}")
                    continue
                    
                bank = row[0] if row[0] and row[0].strip() else None
                address = row[1] if row[1] and row[1].strip() else None
                author = row[2] if row[2] and row[2].strip() else None
                rating = row[3] if row[3] and row[3].strip() else None
                date = row[4] if row[4] and row[4].strip() else None
                text = row[5] if row[5] and row[5].strip() else None
                source_url = row[6] if row[6] and row[6].strip() else None
                
                # Clean up rating to extract numeric value
                if rating and rating != "Unknown":
                    try:
                        import re
                        rating_match = re.search(r'(\d+(?:\.\d+)?)', rating)
                        if rating_match:
                            rating = float(rating_match.group(1))
                        else:
                            rating = None
                    except Exception as e:
                        print(f"Error parsing rating '{rating}' in row {row_num}: {e}")
                        rating = None
                else:
                    rating = None
                
                # Prepare row data
                row_data = (bank, address, author, rating, date, text, source_url)
                rows_to_insert.append(row_data)
        
        # Insert all rows at once using insert_rows method
        if rows_to_insert:
            postgres_hook.insert_rows(
                table=table_name,
                rows=rows_to_insert,
                target_fields=target_fields,
                commit_every=100,  # Commit every 100 rows for better performance
                replace=False
            )
            
            print(f"Successfully inserted {len(rows_to_insert)} reviews into PostgreSQL table '{table_name}'")
        else:
            print("No valid rows found to insert")
            
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")
        raise