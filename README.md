type_mapping = {
        # Numeric Types
        'INTEGER': LongType(),       # BigQuery INTEGER is 64-bit
        'INT64': LongType(),         # Same as INTEGER
        'SMALLINT': ShortType(),     # 16-bit integer
        'INTEGER': IntegerType(),    # 32-bit integer
        'BIGINT': LongType(),        # 64-bit integer
        'TINYINT': ByteType(),       # 8-bit integer
        'FLOAT64': DoubleType(),     # 64-bit floating point
        'FLOAT': FloatType(),        # 32-bit floating point
        'NUMERIC': DecimalType(38, 9), # Default precision and scale
        'BIGNUMERIC': DecimalType(76, 38), # Higher precision
        
        # String Types
        'STRING': StringType(),
        'CHAR': StringType(),
        'VARCHAR': StringType(),
        
        # Boolean Type
        'BOOLEAN': BooleanType(),
        'BOOL': BooleanType(),
        
        # Binary Type
        'BYTES': BinaryType(),
        
        # Date/Time Types
        'DATE': DateType(),
        'DATETIME': TimestampType(),
        'TIMESTAMP': TimestampType(),
        'TIME': StringType(),        # Spark doesn't have a TIME type
        
        # Array Types
        'ARRAY': ArrayType(StringType()),  # Default to string array, modify as needed
        
        # Struct Types
        'STRUCT': StructType([]),    # Empty struct, modify as needed
        
        # Geography Type
        'GEOGRAPHY': StringType()    # Store as WKT string
    }
    
    spark_fields = []
    for field in bq_schema