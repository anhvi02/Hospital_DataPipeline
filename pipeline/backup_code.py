
def find_columns_by_data_type(data_type: str,conn):
    sql_code = f"""
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE DATA_TYPE = {data_type}
    """
    text_columns = query(sql_code, conn)
    return text_columns











li_tables = query(""" 
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE';""")['table_name'].to_list()

print('Number of tables:', len(li_tables))

def get_primary_key_columns(table_name, conn):
    sql_code = f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
    JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU 
    ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
    WHERE TC.TABLE_NAME = '{table_name}' AND TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
    """
    primary_key_columns = query(sql_code, conn)
    return primary_key_columns['COLUMN_NAME'].tolist()

def check_table_data(table_name, conn):
    # Get primary key columns
    primary_key_columns = get_primary_key_columns(table_name, conn)
    print(f"---------------------------------------------")
    print(f"COLUMN: {table_name}")
    print(f"PRIMARY KEY: {', '.join([f'[{col}]' for col in primary_key_columns]) if primary_key_columns else 'None'}")
    
    # Check for missing values in each column
    print("\n- Number of missing values")
    columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    columns = query(columns_query, conn)['COLUMN_NAME'].tolist()
    
    for col in columns:
        null_query = f"SELECT COUNT(*) AS NullCount FROM {table_name} WHERE [{col}] IS NULL"
        null_count = query(null_query, conn).iloc[0, 0]
        if null_count > 0:
            print(f"  + [{col}]: {null_count}")
    
    # Check for duplicates in the entire table
    duplicate_query = f"""
    SELECT COUNT(*) AS TotalDuplicates 
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY {', '.join([f'[{col}]' for col in columns])}) AS cnt FROM {table_name}) AS DupCheck
    WHERE cnt > 1
    """
    duplicate_count = query(duplicate_query, conn).iloc[0, 0]
    print(f"\n- Total number of duplicates in the entire columns: {duplicate_count}")

for table in li_tables:
    check_table_data(table, conn)

