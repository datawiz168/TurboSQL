import pyodbc

def audit_table_structure(conn):
    cursor = conn.cursor()

    # Rule 1: 检查缺失的主键
    query1 = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE' 
    AND TABLE_NAME NOT IN (
        SELECT DISTINCT TABLE_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    );
    """
    cursor.execute(query1)
    tables_without_primary_key = cursor.fetchall()
    for table in tables_without_primary_key:
        print(f"警告: 表 {table[0]} 缺失主键。")

    # Rule 2: 检查缺失的外键
    query2 = """
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME NOT IN (
        SELECT DISTINCT FK_TABLE_NAME 
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    );
    """
    cursor.execute(query2)
    tables_without_foreign_key = cursor.fetchall()
    for table, column in tables_without_foreign_key:
        print(f"警告: 表 {table} 的列 {column} 缺失外键约束。")

    # Rule 3: 检查数据冗余
    # 注意：这通常需要业务逻辑和数据知识来确定。以下是一个简化的示例，只是为了演示。
    query3 = """
    SELECT COLUMN_NAME, COUNT(DISTINCT DATA_TYPE) as data_type_count
    FROM INFORMATION_SCHEMA.COLUMNS 
    GROUP BY COLUMN_NAME
    HAVING data_type_count > 1;
    """
    cursor.execute(query3)
    redundant_data_columns = cursor.fetchall()
    for column, count in redundant_data_columns:
        print(f"警告: 列 {column} 在 {count} 个不同的表中使用了不同的数据类型，可能存在数据冗余。")

    # Rule 4: 检查数据类型不匹配
    # 注意：这是一个示例查询，可能需要根据实际情况进行调整。
    query4 = """
    SELECT COLUMN_NAME, DATA_TYPE, TABLE_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_NAME IN (
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        GROUP BY COLUMN_NAME 
        HAVING COUNT(DISTINCT DATA_TYPE) > 1
    );
    """
    cursor.execute(query4)
    mismatched_data_types = cursor.fetchall()
    for column, data_type, table in mismatched_data_types:
        print(f"警告: 列 {column} 在表 {table} 使用了数据类型 {data_type}，这与其他表中的数据类型不一致。")

    # Rule 5: 检查没有设置适当的列默认值的列
    query5 = """
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_DEFAULT IS NULL AND IS_NULLABLE = 'NO';
    """
    cursor.execute(query5)
    columns_without_default = cursor.fetchall()
    for table, column in columns_without_default:
        print(f"警告: 表 {table} 的列 {column} 没有设置默认值。")

    # Rule 6: 检查是否使用非标准的数据类型
    query6 = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE DATA_TYPE IN ('text', 'ntext', 'image');  -- 示例非标准数据类型
    """
    cursor.execute(query6)
    non_standard_types = cursor.fetchall()
    for table, column, data_type in non_standard_types:
        print(f"警告: 表 {table} 的列 {column} 使用了非标准的数据类型 {data_type}。")

    # Rule 7: 检查列命名不规范
    # 这需要定义一个命名约定，以下是一个简化的示例
    query7 = """
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE COLUMN_NAME LIKE '% %';  -- 包含空格的列名
    """
    cursor.execute(query7)
    improperly_named_columns = cursor.fetchall()
    for table, column in improperly_named_columns:
        print(f"警告: 表 {table} 的列 {column} 命名不规范。")

    # Rule 8: 检查数据冗余
    # 注意：这需要具体的业务逻辑和数据知识，以下只是一个简化示例
    query8 = """
    SELECT COLUMN_NAME, COUNT(DISTINCT TABLE_NAME) as table_count
    FROM INFORMATION_SCHEMA.COLUMNS 
    GROUP BY COLUMN_NAME
    HAVING table_count > 1;
    """
    cursor.execute(query8)
    redundant_data = cursor.fetchall()
    for column, count in redundant_data:
        print(f"警告: 列名 {column} 在 {count} 个不同的表中出现，可能存在数据冗余。")

    # Rule 9: 检查使用过大的数据类型
    query9 = """
    SELECT TABLE_NAME, COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE DATA_TYPE = 'BIGINT' AND COLUMN_NAME LIKE '%age%';  -- 示范性地检查是否用BIGINT存储年龄
    """
    cursor.execute(query9)
    oversized_data_types = cursor.fetchall()
    for table, column in oversized_data_types:
        print(f"警告: 表 {table} 的列 {column} 使用了不必要的大数据类型。")

    # Rule 10: 检查没有使用SCHEMA进行组织的表
    query10 = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo';  -- 默认的SCHEMA
    """
    cursor.execute(query10)
    tables_without_schema = cursor.fetchall()
    for table in tables_without_schema:
        print(f"警告: 表 {table[0]} 没有使用SCHEMA进行组织。")
   
    # Rule 11: 检查是否有触发器
    query11 = """
    SELECT TABLE_NAME, TRIGGER_NAME 
    FROM INFORMATION_SCHEMA.TRIGGERS;
    """
    cursor.execute(query11)
    tables_with_triggers = cursor.fetchall()
    for table, trigger in tables_with_triggers:
        print(f"警告: 表 {table} 使用了触发器 {trigger}，可能影响性能。")

    # Rule 12: 检查存储过程和函数的效率
    # 这是一个示例查询，可能需要根据实际情况进行调整。
    query12 = """
    SELECT ROUTINE_NAME, ROUTINE_TYPE 
    FROM INFORMATION_SCHEMA.ROUTINES 
    WHERE ROUTINE_DEFINITION LIKE '%CURSOR%';  -- 查找使用了CURSOR的存储过程或函数
    """
    cursor.execute(query12)
    inefficient_routines = cursor.fetchall()
    for routine, type in inefficient_routines:
        print(f"警告: {type} {routine} 使用了CURSOR，可能效率低下。")

    # Rule 13: 检查缺少统计信息的表
    query13 = """
    -- 由于SQL Server不直接提供此信息，此查询需要使用DMV进行查询
    SELECT OBJECT_NAME(object_id) AS TableName 
    FROM sys.objects 
    WHERE type = 'U' AND object_id NOT IN (SELECT DISTINCT object_id FROM sys.stats);
    """
    cursor.execute(query13)
    tables_without_stats = cursor.fetchall()
    for table in tables_without_stats:
        print(f"警告: 表 {table[0]} 缺少统计信息。")

    # Rule 14: 检查统计信息是否过时
    query14 = """
    -- 以下查询检查上次更新统计信息超过30天的表
    SELECT OBJECT_NAME(s.object_id) AS TableName, s.name AS StatName, STATS_DATE(s.object_id, s.stats_id) AS LastUpdated
    FROM sys.stats s
    WHERE DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30;
    """
    cursor.execute(query14)
    outdated_stats = cursor.fetchall()
    for table, stat, last_updated in outdated_stats:
        print(f"警告: 表 {table} 的统计信息 {stat} 自上次更新已超过30天。")

    # Rule 15: 检查缺少分区的大表
    query15 = """
    -- 示例查询检查行数超过1百万的未分区表
    SELECT OBJECT_NAME(p.object_id) AS TableName, SUM(p.rows) AS TotalRows
    FROM sys.partitions p
    WHERE p.index_id < 2
    GROUP BY p.object_id
    HAVING SUM(p.rows) > 1000000 AND COUNT(p.partition_number) = 1;
    """
    cursor.execute(query15)
    large_unpartitioned_tables = cursor.fetchall()
    for table, rows in large_unpartitioned_tables:
        print(f"警告: 表 {table} 有 {rows} 行，但没有分区。")


    cursor.close()
