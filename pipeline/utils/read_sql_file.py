def read_sql_file(file_path):
    try:
        with open(file_path,'r') as file:
            sql_string = file.read()
        return sql_string
    except Exception as e:
        raise e