
def find_columns_by_data_type(data_type: str,conn):
    sql_code = f"""
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE DATA_TYPE = {data_type}
    """
    text_columns = query(sql_code, conn)
    return text_columns
