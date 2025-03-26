import pymysql

try:
    # Attempt to connect to MySQL
    connection = pymysql.connect(
        host='192.168.1.8',  # Your Windows IP address
        user='root',
        password='',  # Empty string for default XAMPP setup
        database='celcom_mar2016',
        port=3306
    )
    
    print("Successfully connected to MySQL database!")
    
    # Test reading data
    with connection.cursor() as cursor:
        # List tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"Tables in database: {tables}")
        
        # Get first table name (if any)
        if tables:
            first_table = tables[0][0]
            cursor.execute(f"SELECT * FROM {first_table} LIMIT 5")
            rows = cursor.fetchall()
            print(f"Sample data from {first_table}: {rows[:2]}")
    
    # Close connection
    connection.close()
    print("Connection closed successfully")
    
except Exception as e:
    print(f"Error connecting to MySQL: {e}")
