# Filter out string columns and create a dictionary
    non_string_columns = {
        field.name: field.field_type 
        for field in table.schema 
        if field.field_type.lower() != 'string'
    }