import pyodbc


def audit_table_structure(conn, tables_in_query):
    cursor = conn.cursor()

    # 这将保证在函数的其余部分，我们可以像以前一样使用 `tables` 变量，而无需更改任何代码
    tables = tables_in_query

    # 现在，我们可以继续像以前一样使用 `tables` 变量
    tables_str = ', '.join([f"'{table}'" for table in tables])

    # 规则 1: 检查没有主键的表
    query1 = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = 'AuditDemoDB' 
    AND TABLE_NAME NOT IN (
        SELECT DISTINCT TABLE_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_NAME LIKE 'PK_%'
    ) AND TABLE_NAME IN ({tables_str});
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
        SELECT DISTINCT TABLE_NAME 
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
    HAVING COUNT(DISTINCT DATA_TYPE) > 1;
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
    HAVING COUNT(DISTINCT TABLE_NAME) > 1;
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
    tables_placeholder = ', '.join(["'{}'".format(table) for table in tables])

    query10 = f"""
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_NAME IN ({tables_placeholder}) AND TABLE_SCHEMA = 'dbo';  -- 默认的SCHEMA
    """

    cursor.execute(query10)
    tables_without_schema = cursor.fetchall()
    for table in tables_without_schema:
        print(f"警告: 表 {table[0]} 没有使用SCHEMA进行组织。")

    # Rule 11: 检查是否有触发器
    query11 = """
    SELECT OBJECT_NAME(parent_id) AS TABLE_NAME, 
           name AS TRIGGER_NAME 
    FROM sys.triggers 
    WHERE type = 'TR';
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

    # Rule 16: 检查是否所有的表都有主键
    query16 = f"""
    SELECT name 
    FROM sys.tables 
    WHERE name IN ({', '.join(["'" + table + "'" for table in tables])})
    AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    """

    cursor.execute(query16)
    tables_without_primarykey = cursor.fetchall()

    for table in tables_without_primarykey:
        print(f"警告: 表 {table[0]} 没有主键。")


    # 规则 17: 检查是否存在过大的表但未建立任何索引
    query17 = """
    SELECT t.name 
    FROM sys.tables t 
    WHERE NOT EXISTS (
        SELECT 1 
        FROM sys.indexes i 
        WHERE i.object_id = t.object_id
    ) AND (
        SELECT SUM(p.rows) 
        FROM sys.partitions p
        WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)
    ) > 10000
    AND t.name IN ({})
    """

    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query17 = query17.format(placeholders)

    cursor.execute(query17, tables_in_query)
    large_tables_without_indexes = cursor.fetchall()

    for table in large_tables_without_indexes:
        print(f"警告: 表 {table[0]} 有超过100,000行但没有索引。")

    # Rule 18: 检查数据类型是否适当
    query18 = """
    SELECT table_name, column_name, data_type FROM information_schema.columns WHERE data_type IN ('text', 'ntext')
    """
    cursor.execute(query18)
    inappropriate_data_types = cursor.fetchall()
    for table, column, data_type in inappropriate_data_types:
        print(f"警告: 表 {table} 中的列 {column} 使用了 {data_type} 数据类型，考虑使用VARCHAR(MAX)或NVARCHAR(MAX)。")

    # Rule 19: 检查是否存在过多的NULL值
    query19 = """
    SELECT table_name, column_name FROM information_schema.columns WHERE is_nullable = 'YES'
    """
    cursor.execute(query19)
    columns_with_nulls = cursor.fetchall()
    for table, column in columns_with_nulls:
        print(f"警告: 表 {table} 中的列 {column} 允许NULL值。")

    # 规则 20: 检查表是否存在太多的外键关联
    query20 = """
    SELECT OBJECT_NAME(parent_object_id) AS TableName, COUNT(*) AS FKCount
    FROM sys.foreign_keys
    WHERE OBJECT_NAME(parent_object_id) IN ({})
    GROUP BY parent_object_id
    HAVING COUNT(*) > 5
    """

    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query20 = query20.format(placeholders)

    cursor.execute(query20, tables_in_query)
    tables_with_many_foreign_keys = cursor.fetchall()

    for table, fk_count in tables_with_many_foreign_keys:
        print(f"警告: 表 {table} 有超过5个外键关联。")

    # Rule 21: 检查是否存在大量的列是 VARCHAR(MAX) 或 NVARCHAR(MAX)
    query21 = """
    SELECT table_name, column_name FROM information_schema.columns WHERE data_type IN ('varchar', 'nvarchar') AND character_maximum_length = -1
    """
    cursor.execute(query21)
    columns_with_large_varchar = cursor.fetchall()
    for table, column in columns_with_large_varchar:
        print(f"警告: 表 {table} 中的列 {column} 使用了VARCHAR(MAX)或NVARCHAR(MAX)数据类型，可能导致性能下降。")

    # Rule 22: 检查是否存在过大的非聚集索引
    query22 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(ps.used_page_count) * 8 AS IndexSizeKB 
    FROM sys.dm_db_partition_stats ps
    JOIN sys.indexes i ON i.object_id = ps.object_id
    WHERE i.type_desc = 'NONCLUSTERED'
    GROUP BY i.object_id, i.name
    HAVING SUM(ps.used_page_count) * 8 > 5000  -- 5MB为界限
    """
    cursor.execute(query22)
    large_nonclustered_indexes = cursor.fetchall()
    for table, index, size in large_nonclustered_indexes:
        print(f"警告: 表 {table} 的非聚集索引 {index} 大小为 {size}KB，可能导致I/O开销增加。")

    # Rule 23: 检查是否有冗余索引
    query23 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName
    FROM sys.dm_db_index_usage_stats s
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE user_reads = 0 AND user_seeks = 0 AND system_reads = 0 AND system_seeks = 0
    """
    cursor.execute(query23)
    redundant_indexes = cursor.fetchall()
    for table, index in redundant_indexes:
        print(f"警告: 表 {table} 的索引 {index} 可能是冗余的，因为它没有被查询使用过。")

    # # Rule 24: 检查是否有浮点数据类型，可能导致不准确的计算
    # query24 = """
    # SELECT table_name, column_name FROM information_schema.columns WHERE data_type IN ('float', 'real')
    # """
    # cursor.execute(query24)
    # floating_point_columns = cursor.fetchall()
    # for table, column in floating_point_columns:
    #     print(f"警告: 表 {table} 中的列 {column} 使用了浮点数据类型，可能导致不准确的计算。")

    # 规则 24: 检查是否有浮点数据类型，可能导致不准确的计算
    query24 = """
    SELECT OBJECT_NAME(c.object_id) AS TableName, c.name AS ColumnName
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE t.name IN ('float', 'real') AND OBJECT_NAME(c.object_id) IN ({})
    """

    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query24 = query24.format(placeholders)

    cursor.execute(query24, tables_in_query)
    floating_point_columns = cursor.fetchall()

    for table, column in floating_point_columns:
        print(f"警告: 表 {table} 中的列 {column} 使用了浮点数据类型，可能导致不准确的计算。")

    # Rule 25: 检查是否有表没有更新统计信息
    tables_placeholder = ', '.join(["'{}'".format(table) for table in tables])
    query25 = f"""
    SELECT name FROM sys.tables WHERE name IN ({tables_placeholder})
    AND OBJECTPROPERTY(object_id, 'IsUserTable') = 1 AND DATEDIFF(d, STATS_DATE(object_id, NULL), GETDATE()) > 30
    """
    cursor.execute(query25)
    outdated_statistics_tables = cursor.fetchall()
    for table in outdated_statistics_tables:
        print(f"警告: 表 {table[0]} 长时间未更新统计信息，可能导致查询性能下降。")

    # 规则26: 检查是否有表没有主键
    query26 = """
    SELECT name FROM sys.tables 
    WHERE type = 'U' AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query26 = query26.format(placeholders)

    # 执行 SQL 查询，将 tables_in_query 列表作为参数传递给 execute 方法
    # 这样，每个问号占位符都将被 tables_in_query 列表中的相应表名替换
    cursor.execute(query26, tables_in_query)

    # 获取查询结果
    tables_without_primary_key = cursor.fetchall()

    # 对于每一个没有主键的表，打印一个警告消息
    for table in tables_without_primary_key:
        print(f"警告: 表 {table[0]} 没有主键，可能导致数据完整性问题和查询性能下降。")



    # # Rule 27: 检查是否有过大的表没有聚集索引
    query27 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName
    FROM sys.indexes WHERE object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({}', '{}', '{}', '{}).format(*tables_in_query)) i
    WHERE i.type = 0 AND OBJECTPROPERTY(i.object_id, 'TableHasClustIndex') = 0
    """
    cursor.execute(query27)
    large_tables_without_clustered_index = cursor.fetchall()
    for table in large_tables_without_clustered_index:
        print(f"警告: 表 {table[0]} 没有聚集索引，可能导致查询性能下降和数据存储不连续。")

    # 规则 27: 检查是否有过大的表没有聚集索引
    query27 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName
    FROM sys.indexes i
    WHERE i.type = 0 AND OBJECTPROPERTY(i.object_id, 'TableHasClustIndex') = 0
    AND OBJECT_NAME(i.object_id) IN ({})
    """
    placeholders = ', '.join('?' for _ in tables_in_query)
    query27 = query27.format(placeholders)
    cursor.execute(query27, tables_in_query)
    large_tables_without_clustered_index = cursor.fetchall()
    for table in large_tables_without_clustered_index:
        print(f"警告: 表 {table[0]} 没有聚集索引，可能导致查询性能下降和数据存储不连续。")

    # Rule 28: 检查是否有列存储为文本数据类型（text, ntext, image）
    query28 = """
    SELECT table_name, column_name FROM information_schema.columns 
    WHERE data_type IN ('text', 'ntext', 'image')
    """
    cursor.execute(query28)
    columns_with_text_datatype = cursor.fetchall()
    for table, column in columns_with_text_datatype:
        print(f"警告: 表 {table} 中的列 {column} 使用了已过时的文本数据类型，考虑使用varchar(max)、nvarchar(max)或varbinary(max)替代。")

    # Rule 29: 检查是否有表使用了“*”进行查询
    query29 = """
    SELECT DISTINCT OBJECT_NAME(object_id) AS TableName
    FROM sys.dm_exec_query_stats qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
    WHERE st.text LIKE '%SELECT * FROM%'
    """
    cursor.execute(query29)
    tables_using_select_star = cursor.fetchall()
    for table in tables_using_select_star:
        print(f"警告: 表 {table[0]} 使用了“SELECT *”进行查询，这可能导致查询性能下降和未必要的数据传输。")

    # Rule 30: 检查是否有冗余的外键约束
    query30 = """
    SELECT fk.name AS ForeignKey, OBJECT_NAME(fk.parent_object_id) AS TableName
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE EXISTS (
        SELECT 1
        FROM sys.foreign_keys other_fk
        INNER JOIN sys.foreign_key_columns other_fkc ON other_fk.object_id = other_fkc.constraint_object_id
        WHERE fk.object_id != other_fk.object_id
        AND fk.parent_object_id = other_fk.parent_object_id
        AND fk.referenced_object_id = other_fk.referenced_object_id
        AND fkc.parent_column_id = other_fkc.parent_column_id
        AND fkc.referenced_column_id = other_fkc.referenced_column_id
    )
    """
    cursor.execute(query30)
    redundant_foreign_keys = cursor.fetchall()
    for fk, table in redundant_foreign_keys:
        print(f"警告: 表 {table} 的外键约束 {fk} 可能是冗余的。")

    # Rule 31: 检查是否存在不带默认值的非空列
    query31 = """
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE is_nullable = 'NO' AND column_default IS NULL
    """
    cursor.execute(query31)
    non_null_columns_without_default = cursor.fetchall()
    for table, column in non_null_columns_without_default:
        print(f"警告: 表 {table} 中的列 {column} 是非空的，但没有设置默认值，可能导致插入数据时出错。")

    # Rule 32: 检查是否存在数据类型为FLOAT的列
    query32 = """
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE data_type = 'float'
    """
    cursor.execute(query32)
    columns_with_float_datatype = cursor.fetchall()
    for table, column in columns_with_float_datatype:
        print(f"警告: 表 {table} 中的列 {column} 使用了FLOAT数据类型，可能导致精度问题，考虑使用DECIMAL或NUMERIC替代。")

    # Rule 33: 检查是否存在过大的VARCHAR列
    query33 = """
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE data_type = 'varchar' AND character_maximum_length > 4000
    """
    cursor.execute(query33)
    oversized_varchar_columns = cursor.fetchall()
    for table, column in oversized_varchar_columns:
        print(f"警告: 表 {table} 中的列 {column} 的VARCHAR长度超过4000，可能导致存储问题和性能下降。")

    # Rule 34: 检查表是否有更新统计信息
    query34 = """
    SELECT OBJECT_NAME(object_id) AS TableName, last_user_update 
    FROM sys.dm_db_index_usage_stats
    WHERE database_id = DB_ID() AND last_user_update IS NOT NULL
    """
    cursor.execute(query34)
    tables_without_updated_stats = cursor.fetchall()
    for table, last_update in tables_without_updated_stats:
        print(f"警告: 表 {table} 的统计信息上次更新是在 {last_update}，考虑定期更新统计信息以提高查询性能。")

    # Rule 35: 检查是否有大量数据的表没有备份
    query35 = """
    SELECT name AS TableName, SUM(rows) AS TotalRows
    FROM sys.partitions
    WHERE index_id IN (0, 1) AND OBJECTPROPERTY(object_id, 'IsUserTable') = 1
    GROUP BY name
    HAVING SUM(rows) > 1000000
    """
    cursor.execute(query35)
    large_tables_without_backup = cursor.fetchall()
    for table, rows in large_tables_without_backup:
        print(f"警告: 表 {table} 有 {rows} 行数据，但没有备份，可能导致数据丢失风险。")

    # Rule 36: 检查是否存在大量NULL值的列
    query36 = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'dbo' -- 你可以根据实际情况调整schema名称
    AND column_name IN
        (SELECT column_name FROM your_database_name.your_table_name
         WHERE (SELECT COUNT(*) FROM your_database_name.your_table_name WHERE column_name IS NULL) > 1000) -- 根据实际情况调整阈值
    """
    cursor.execute(query36)
    columns_with_excessive_nulls = cursor.fetchall()
    for table, column in columns_with_excessive_nulls:
        print(f"警告: 表 {table} 中的列 {column} 存在大量NULL值，考虑优化表结构。")

    # 规则 37: 检查是否有表缺少主键
    query37 = """
    SELECT name AS TableName 
    FROM sys.tables 
    WHERE OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query37 = query37.format(placeholders)
    cursor.execute(query37, tables_in_query)
    tables_without_primary_keys = cursor.fetchall()
    for table in tables_without_primary_keys:
        print(f"警告: 表 {table[0]} 缺少主键，可能导致数据完整性问题。")

    # Rule 38: 检查是否有表使用GUID作为主键
    query38 = """
    SELECT o.name AS TableName, c.name AS ColumnName
    FROM sys.columns c
    JOIN sys.objects o ON o.object_id = c.object_id
    WHERE c.column_id = 1 AND c.system_type_id = 36 AND o.type = 'U'
    """
    cursor.execute(query38)
    tables_with_guid_primary_keys = cursor.fetchall()
    for table, column in tables_with_guid_primary_keys:
        print(f"警告: 表 {table} 使用GUID ({column}) 作为主键，可能导致性能问题。")

    # Rule 39: 检查是否存在过大的CHAR列
    query39 = """
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE data_type = 'char' AND character_maximum_length > 255
    """
    cursor.execute(query39)
    oversized_char_columns = cursor.fetchall()
    for table, column in oversized_char_columns:
        print(f"警告: 表 {table} 中的列 {column} 的CHAR长度超过255，考虑使用VARCHAR代替。")

    # Rule 40: 检查是否有表的行数超过特定阈值但没有任何索引
    query40 = """
    SELECT OBJECT_NAME(p.object_id) AS TableName, SUM(p.rows) AS TotalRows
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id
    WHERE i.index_id = 0 AND OBJECTPROPERTY(p.object_id, 'IsUserTable') = 1
    GROUP BY p.object_id
    HAVING SUM(p.rows) > 10000
    """
    cursor.execute(query40)
    large_tables_without_indexes = cursor.fetchall()
    for table, rows in large_tables_without_indexes:
        print(f"警告: 表 {table} 有 {rows} 行数据但没有任何索引，可能导致查询性能问题。")

    # Rule 41: 检查是否存在过大的Varchar列，但数据实际使用长度较小
    query41 = """
    SELECT table_name, column_name, max(len(column_name)) as MaxLength
    FROM information_schema.columns 
    WHERE data_type = 'varchar' AND character_maximum_length > 255
    GROUP BY table_name, column_name
    HAVING max(len(column_name)) < 100 -- 实际使用长度小于100
    """
    cursor.execute(query41)
    oversized_varchar_columns = cursor.fetchall()
    for table, column, length in oversized_varchar_columns:
        print(f"警告: 表 {table} 中的列 {column} 的Varchar最大长度为 {length}，但实际使用长度远小于此。考虑减小该列的最大长度。")

    # Rule 42: 检查表是否存在过多的列
    query42 = """
    SELECT table_name, COUNT(column_name) as ColumnCount 
    FROM information_schema.columns 
    GROUP BY table_name
    HAVING COUNT(column_name) > 50  -- 考虑表中超过50个列可能是过多的
    """
    cursor.execute(query42)
    tables_with_excessive_columns = cursor.fetchall()
    for table, count in tables_with_excessive_columns:
        print(f"警告: 表 {table} 有 {count} 列，考虑是否可以优化表结构，避免过多列。")

    # Rule 43: 检查是否存在不带默认值的非NULL列
    query43 = """
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE is_nullable = 'NO' AND column_default IS NULL
    """
    cursor.execute(query43)
    nonnull_columns_without_default = cursor.fetchall()
    for table, column in nonnull_columns_without_default:
        print(f"警告: 表 {table} 中的列 {column} 是非NULL的，但没有默认值。考虑为其设置一个默认值。")

    # Rule 44: 检查是否存在重复的数据
    query44 = """
    SELECT TableName, ColumnName, DuplicateCount
    FROM (
        SELECT table_name as TableName, column_name as ColumnName, count(column_name) as DuplicateCount
        FROM information_schema.columns
        GROUP BY table_name, column_name
    ) sub
    WHERE DuplicateCount > 1
    """
    cursor.execute(query44)
    duplicate_data = cursor.fetchall()
    for table, column, count in duplicate_data:
        print(f"警告: 在表 {table} 的列 {column} 中找到 {count} 个重复项。考虑删除重复数据。")


    # 规则 45: 检查是否存在未使用的表
    query45 = """
    SELECT name AS TableName 
    FROM sys.tables
    WHERE OBJECTPROPERTY(object_id, 'TableHasClustIndex') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasNonClustIndex') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasUniqueCnst') = 0
    AND OBJECTPROPERTY(object_id, 'TableWithNoTriggers') = 1
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query45 = query45.format(placeholders)
    cursor.execute(query45, tables_in_query)
    unused_tables = cursor.fetchall()
    for table in unused_tables:
        print(f"警告: 表 {table[0]} 似乎未被使用。考虑是否可以删除它。")

    # Rule 46: 检查是否存在不带注释的列
    query46 = """
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE column_comment = '' OR column_comment IS NULL
    """
    cursor.execute(query46)
    columns_without_comments = cursor.fetchall()
    for table, column in columns_without_comments:
        print(f"警告: 表 {table} 中的列 {column} 缺少注释。为了更好的文档化，请考虑为其添加注释。")

    # Rule 47: 检查是否存在过大的表但缺少分区
    query47 = """
    SELECT table_name AS TableName, SUM(data_length + index_length) AS TotalSize
    FROM information_schema.tables
    WHERE table_schema = 'your_database_name'  -- 替换为您的数据库名称
    GROUP BY table_name
    HAVING SUM(data_length + index_length) > 5000000000  -- 大于5GB
    """
    cursor.execute(query47)
    large_tables_without_partitioning = cursor.fetchall()
    for table, size in large_tables_without_partitioning:
        print(f"警告: 表 {table} 的大小为 {size} 字节，但未使用分区。考虑为这种大表使用分区。")

    # Rule 48: 检查是否存在带有多个外键约束的表
    query48 = """
    SELECT table_name AS TableName, COUNT(constraint_name) AS ForeignKeyCount
    FROM information_schema.referential_constraints
    GROUP BY table_name
    HAVING COUNT(constraint_name) > 5  -- 考虑超过5个外键可能过多
    """
    cursor.execute(query48)
    tables_with_excessive_foreign_keys = cursor.fetchall()
    for table, count in tables_with_excessive_foreign_keys:
        print(f"警告: 表 {table} 有 {count} 个外键约束，这可能会影响性能。")

    # Rule 49: 检查是否存在没有主键的表
    query49 = """
    SELECT table_name AS TableName 
    FROM information_schema.tables t
    WHERE table_type = 'BASE TABLE' 
    AND NOT EXISTS (
        SELECT 1 
        FROM information_schema.key_column_usage 
        WHERE constraint_name = 'PRIMARY' 
        AND table_schema = t.table_schema 
        AND table_name = t.table_name
    )
    """
    cursor.execute(query49)
    tables_without_primary_keys = cursor.fetchall()
    for table in tables_without_primary_keys:
        print(f"警告: 表 {table} 没有主键，这可能会影响数据的完整性和查询性能。")

    # Rule 50: 检查是否存在没有索引的表
    query50 = """
    SELECT t.name AS TableName
    FROM sys.tables WHERE name IN ({}', '{}', '{}', '{}).format(*tables_in_query) t
    LEFT JOIN sys.indexes i ON t.object_id = i.object_id
    WHERE i.object_id IS NULL
    """
    cursor.execute(query50)
    tables_without_indexes = cursor.fetchall()
    for table in tables_without_indexes:
        print(f"警告: 表 {table} 没有任何索引，这可能会影响查询性能。")

    # 规则 50: 检查是否存在没有索引的表
    query50 = """
     SELECT t.name AS TableName 
     FROM sys.tables t
     LEFT JOIN sys.indexes i ON t.object_id = i.object_id 
     WHERE i.object_id IS NULL
     AND t.name IN ({})
     """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query50 = query50.format(placeholders)
    cursor.execute(query50, tables_in_query)
    tables_without_indexes = cursor.fetchall()
    for table in tables_without_indexes:
        print(f"警告: 表 {table[0]} 没有任何索引，这可能会影响查询性能。")

    # Rule 51: 检查存在自增列但是不是主键的表
    query51 = """
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE extra = 'auto_increment'
    AND table_name NOT IN (
        SELECT table_name
        FROM information_schema.key_column_usage
        WHERE constraint_name = 'PRIMARY'
    )
    """
    cursor.execute(query51)
    auto_increment_not_primary = cursor.fetchall()
    for table, column in auto_increment_not_primary:
        print(f"警告: 表 {table} 的列 {column} 是自增列，但不是主键。考虑将其设置为主键以确保数据的唯一性。")

    # Rule 52: 检查列数据类型是否适当
    # 例如：用于存储日期的VARCHAR列
    query52 = """
    SELECT table_name AS TableName, column_name AS ColumnName, data_type AS DataType
    FROM information_schema.columns 
    WHERE column_name LIKE '%date%' AND data_type = 'varchar'
    """
    cursor.execute(query52)
    improper_date_columns = cursor.fetchall()
    for table, column, dtype in improper_date_columns:
        print(f"警告: 表 {table} 的列 {column} 似乎用于存储日期，但其数据类型为 {dtype}。考虑更改数据类型以提高效率。")

    # Rule 53: 检查是否存在CHAR数据类型的列
    # CHAR类型通常比VARCHAR使用更多的存储空间
    query53 = """
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns 
    WHERE data_type = 'char'
    """
    cursor.execute(query53)
    char_columns = cursor.fetchall()
    for table, column in char_columns:
        print(f"警告: 表 {table} 的列 {column} 使用CHAR数据类型，这可能导致存储浪费。考虑使用VARCHAR数据类型。")

    # Rule 54: 检查是否存在未引用的外键
    query54 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.key_column_usage k
    LEFT JOIN information_schema.referential_constraints r ON k.constraint_name = r.constraint_name
    WHERE r.constraint_name IS NULL
    """
    cursor.execute(query54)
    unreferenced_foreign_keys = cursor.fetchall()
    for table, column in unreferenced_foreign_keys:
        print(f"警告: 表 {table} 的列 {column} 被定义为外键，但未引用任何表。")

    # Rule 55: 检查是否存在冗余的索引
    query55 = """
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName
    FROM sys.indexes WHERE object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({}', '{}', '{}', '{}).format(*tables_in_query)) ix
    INNER JOIN sys.index_columns ic1 ON ix.object_id = ic1.object_id AND ix.index_id = ic1.index_id
    INNER JOIN sys.index_columns ic2 ON ic1.object_id = ic2.object_id AND ic1.column_id = ic2.column_id
    WHERE ic1.index_id <> ic2.index_id
    """
    cursor.execute(query55)
    redundant_indexes = cursor.fetchall()
    for table, index in redundant_indexes:
        print(f"警告: 表 {table} 存在冗余的索引 {index}。考虑删除冗余索引以提高写操作性能。")

    # 规则 55: 检查是否存在冗余的索引
    query55 = """
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName
    FROM sys.indexes ix
    INNER JOIN sys.index_columns ic1 ON ix.object_id = ic1.object_id AND ix.index_id = ic1.index_id
    INNER JOIN sys.index_columns ic2 ON ic1.object_id = ic2.object_id AND ic1.column_id = ic2.column_id
    WHERE ic1.index_id <> ic2.index_id
    AND ix.object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({}))
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query55 = query55.format(placeholders)
    cursor.execute(query55, tables_in_query)
    redundant_indexes = cursor.fetchall()
    for table, index in redundant_indexes:
        print(f"警告: 表 {table} 存在冗余的索引 {index}。考虑删除冗余索引以提高写操作性能。")

    # Rule 56: 检查是否存在只读表，但仍有索引
    # 这样的表可能不需要多个索引，因为它们不涉及写操作
    query56 = """
    SELECT table_name AS TableName
    FROM information_schema.tables t
    LEFT JOIN information_schema.statistics s ON t.table_name = s.table_name
    WHERE t.table_comment = 'read_only' AND COUNT(s.index_name) > 1
    GROUP BY t.table_name
    """
    cursor.execute(query56)
    readonly_with_multiple_indexes = cursor.fetchall()
    for table in readonly_with_multiple_indexes:
        print(f"警告: 表 {table} 是只读的，但存在多个索引。考虑优化其索引结构。")

    # Rule 57: 检查是否存在具有默认值的列，但未被引用
    query57 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE column_default IS NOT NULL AND column_name NOT IN (
        SELECT column_name 
        FROM information_schema.key_column_usage
    )
    """
    cursor.execute(query57)
    default_columns_not_referenced = cursor.fetchall()
    for table, column in default_columns_not_referenced:
        print(f"警告: 表 {table} 的列 {column} 有默认值，但在其他表中未被引用。")

    # Rule 59: 检查是否存在无文档的存储过程
    query59 = """
    SELECT SPECIFIC_NAME AS ProcedureName
    FROM information_schema.routines
    WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_COMMENT IS NULL
    """
    cursor.execute(query59)
    undocumented_procedures = cursor.fetchall()
    for proc in undocumented_procedures:
        print(f"警告: 存储过程 {proc} 没有文档或描述。")

    # Rule 60: 检查是否存在未设置的外键约束
    query60 = """
    SELECT COLUMN_NAME, TABLE_NAME
    FROM information_schema.columns
    WHERE COLUMN_NAME LIKE '%_id' AND COLUMN_NAME NOT IN (
        SELECT COLUMN_NAME 
        FROM information_schema.key_column_usage
    )
    """
    cursor.execute(query60)
    potential_missing_fk = cursor.fetchall()
    for table, column in potential_missing_fk:
        print(f"警告: 表 {table} 的列 {column} 看起来像是一个外键，但未设置外键约束。")

    # Rule 61: 检查使用GUID作为主键的表
    query61 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'uniqueidentifier' AND column_name IN (
        SELECT column_name 
        FROM information_schema.key_column_usage
    )
    """
    cursor.execute(query61)
    guid_as_primary_key = cursor.fetchall()
    for table, column in guid_as_primary_key:
        print(f"警告: 表 {table} 使用GUID {column} 作为主键，这可能会导致性能问题和碎片化。")

    # 规则 62: 检查使用 TEXT 或 NTEXT 数据类型的列
    query62 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type IN ('text', 'ntext')
    """
    cursor.execute(query62)
    text_columns = cursor.fetchall()
    for table, column in text_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 TEXT 或 NTEXT 数据类型，建议使用 VARCHAR(MAX) 或 NVARCHAR(MAX) 代替。")

    # 规则 63: 检查使用 IMAGE 数据类型的列
    query63 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'image'
    """
    cursor.execute(query63)
    image_columns = cursor.fetchall()
    for table, column in image_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 IMAGE 数据类型，建议使用 VARBINARY(MAX) 代替。")

    # 规则 64: 检查存在超过 5 个索引的表
    query64 = """
    SELECT OBJECT_NAME(ind.object_id) AS TableName, COUNT(*) as IndexCount
    FROM sys.indexes ind
    WHERE ind.object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({}))
    GROUP BY ind.object_id
    HAVING COUNT(*) > 5
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query64 = query64.format(placeholders)

    cursor.execute(query64, tables_in_query)
    tables_with_excessive_indexes = cursor.fetchall()
    for table, count in tables_with_excessive_indexes:
        print(f"警告: 表 {table} 有 {count} 个索引，可能导致写入性能下降。")

    # 规则 65: 检查表是否有更新统计信息
    query65 = """
    SELECT OBJECT_NAME(object_id) AS TableName, last_user_update 
    FROM sys.dm_db_index_usage_stats 
    WHERE database_id = DB_ID() AND OBJECTPROPERTY(object_id,'IsUserTable') = 1
    AND last_user_update < DATEADD(DAY, -30, GETDATE()) -- 更改天数根据实际需求
    """
    cursor.execute(query65)
    outdated_statistics_tables = cursor.fetchall()
    for table, last_updated in outdated_statistics_tables:
        print(f"警告: 表 {table} 的统计信息在 {last_updated} 之后未更新，建议更新统计信息。")

    # 规则 66: 检查使用 FLOAT 数据类型的列
    query66 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'float'
    """
    cursor.execute(query66)
    float_columns = cursor.fetchall()
    for table, column in float_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 FLOAT 数据类型，可能导致精度问题。")

    # 规则 67: 检查使用 TEXT 数据类型的列
    query67 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'text'
    """
    cursor.execute(query67)
    text_columns = cursor.fetchall()
    for table, column in text_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 TEXT 数据类型，可能影响查询性能。")

    # 规则 68: 检查使用 BIT 数据类型的列
    query68 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'bit'
    """
    cursor.execute(query68)
    bit_columns = cursor.fetchall()
    for table, column in bit_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 BIT 数据类型，可能导致查询优化器做出不佳的决策。")

    # 规则 69: 检查存在空字符串默认值的列
    query69 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE column_default = ''
    """
    cursor.execute(query69)
    empty_default_value_columns = cursor.fetchall()
    for table, column in empty_default_value_columns:
        print(f"警告: 表 {table} 的列 {column} 的默认值是空字符串，可能会导致意外的行为。")

    # 规则 70: 检查存在空格默认值的列
    query70 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE column_default = ' '
    """
    cursor.execute(query70)
    space_default_value_columns = cursor.fetchall()
    for table, column in space_default_value_columns:
        print(f"警告: 表 {table} 的列 {column} 的默认值是空格，可能会导致意外的行为。")

    # 规则 71: 检查表中是否有多个 TIMESTAMP/ROWVERSION 数据类型的列
    query71 = """
    SELECT table_name AS TableName, COUNT(column_name) AS TimestampColumnsCount 
    FROM information_schema.columns 
    WHERE data_type = 'timestamp'
    GROUP BY table_name
    HAVING COUNT(column_name) > 1
    """
    cursor.execute(query71)
    multiple_timestamp_tables = cursor.fetchall()
    for table, count in multiple_timestamp_tables:
        print(f"警告: 表 {table} 有 {count} 个 TIMESTAMP/ROWVERSION 数据类型的列，通常一个表应该只有一个这样的列。")

    # 规则 72: 检查是否存在对系统视图的直接查询
    query101 = """
       SELECT text AS QueryText
       FROM sys.dm_exec_requests r
       CROSS APPLY sys.dm_exec_sql_text(r.sql_handle)
       WHERE text LIKE '%sys.%'
       """
    cursor.execute(query101)
    sys_view_queries = cursor.fetchall()
    for query in sys_view_queries:
        print(f"警告: 查询 {query.QueryText} 直接查询了系统视图，这可能不是最佳实践。")

    # 规则 73: 检查是否所有的外键都有相应的索引支持
    query73 = """
    SELECT f.name AS ForeignKey, OBJECT_NAME(f.parent_object_id) AS TableName, COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName
    FROM sys.foreign_keys AS f
    JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
    LEFT JOIN sys.index_columns AS ic ON ic.object_id = fc.parent_object_id AND ic.column_id = fc.parent_column_id
    WHERE ic.object_id IS NULL
    """
    cursor.execute(query73)
    unindexed_foreign_keys = cursor.fetchall()
    for fk, table, column in unindexed_foreign_keys:
        print(f"警告: 表 {table} 的外键 {fk} 在列 {column} 上没有相应的索引支持。")

    # 规则 74: 检查是否有大型表（例如，行数超过100万）但没有聚集索引
    query74 = """
    SELECT o.name AS TableName, p.rows AS RowCount
    FROM sys.objects o
    JOIN sys.partitions p ON o.object_id = p.object_id
    WHERE o.type = 'U' AND p.index_id = 0 AND p.rows > 1000000
    """
    cursor.execute(query74)
    large_heap_tables = cursor.fetchall()
    for table, rows in large_heap_tables:
        print(f"警告: 表 {table} 有 {rows} 行，但没有聚集索引。")

    # 规则 75: 检查是否有存储过程或函数使用了 'sp_' 前缀
    query75 = """
    SELECT name AS RoutineName, type_desc AS RoutineType
    FROM sys.objects
    WHERE name LIKE 'sp_%' AND type IN ('P', 'FN', 'TF', 'IF')
    """
    cursor.execute(query75)
    sp_prefixed_routines = cursor.fetchall()
    for routine, type_desc in sp_prefixed_routines:
        print(f"警告: {type_desc} {routine} 使用了 'sp_' 前缀，这可能与系统存储过程发生冲突。")

    # 规则 76: 检查是否存在的外键是否引用了不存在的数据
    query76 = """
    SELECT fk.name AS ForeignKey,
           OBJECT_NAME(fk.parent_object_id) AS TableName,
           COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
           OBJECT_NAME (fk.referenced_object_id) AS ReferenceTableName,
           COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.foreign_key_columns AS fc ON fk.OBJECT_ID = fc.constraint_object_id
    """
    cursor.execute(query76)
    foreign_keys = cursor.fetchall()
    for fk in foreign_keys:
        print(f"信息: 外键 {fk.ForeignKey} 在表 {fk.TableName} 上引用了表 {fk.ReferenceTableName}。")

    # 规则 77: 检查是否有大于 1MB 的 VARCHAR 列
    query77 = """
    SELECT table_name AS TableName, column_name AS ColumnName, character_maximum_length AS MaxLength 
    FROM information_schema.columns 
    WHERE data_type = 'varchar' AND character_maximum_length > 1000000
    """
    cursor.execute(query77)
    large_varchar_columns = cursor.fetchall()
    for table, column, length in large_varchar_columns:
        print(f"警告: 表 {table} 的列 {column} 的 VARCHAR 长度设置为 {length}，考虑使用 VARCHAR(MAX) 或者缩小长度。")

    # 规则 78: 检查是否有空的表（无数据）
    query78 = """
    SELECT o.name AS TableName
    FROM sys.objects o
    JOIN sys.partitions p ON o.object_id = p.object_id
    WHERE o.type = 'U' AND p.rows = 0
    """
    cursor.execute(query78)
    empty_tables = cursor.fetchall()
    for table in empty_tables:
        print(f"警告: 表 {table} 是空的，考虑是否需要这个表。")

    # 规则 79: 检查是否有宽表（列数过多的表）
    query102 = """
    SELECT table_name AS TableName, COUNT(*) AS ColumnCount
    FROM information_schema.columns
    GROUP BY table_name
    HAVING COUNT(*) > 50
    """
    cursor.execute(query102)
    wide_tables = cursor.fetchall()
    for table in wide_tables:
        print(f"警告: 表 {table.TableName} 的列数为 {table.ColumnCount}，可能是一个宽表。")

    # 规则 80: 检查是否有过大的单个事务
    query80 = """
    SELECT transaction_id, name, start_time,
           DATEDIFF(MINUTE, start_time, GETDATE()) AS duration_in_minutes
    FROM sys.dm_tran_active_transactions
    WHERE DATEDIFF(MINUTE, start_time, GETDATE()) > 30
    """
    cursor.execute(query80)
    long_transactions = cursor.fetchall()
    for txn in long_transactions:
        print(f"警告: 事务 {txn.transaction_id} ({txn.name}) 已经运行了 {txn.duration_in_minutes} 分钟。")


    # 规则 81: 检查是否有不常用的索引
    query81 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, 
           s.user_seeks + s.user_scans + s.user_lookups AS TotalUses
    FROM sys.indexes i
    JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE s.user_seeks + s.user_scans + s.user_lookups < 10
    AND i.object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({}))
    """

    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query81 = query81.format(placeholders)

    cursor.execute(query81, tables_in_query)
    infrequently_used_indexes = cursor.fetchall()
    for index in infrequently_used_indexes:
        print(f"警告: 索引 {index.IndexName} 在表 {index.TableName} 很少被使用，考虑删除。")

    # 规则 82: 检查是否有重复的列数据
    query82 = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE column_name IN 
    (SELECT column_name FROM information_schema.columns
    GROUP BY column_name HAVING COUNT(table_name) > 1)
    """
    cursor.execute(query82)
    duplicate_columns = cursor.fetchall()
    for column in duplicate_columns:
        print(f"警告: 列名 {column.column_name} 在表 {column.table_name} 中是重复的。")

    # 规则 83: 检查是否有过大的日志文件
    query83 = """
    SELECT name, size/128.0 AS SizeInMB
    FROM sys.master_files
    WHERE type_desc = 'LOG' AND size/128.0 > 1000
    """
    cursor.execute(query83)
    large_log_files = cursor.fetchall()
    for log in large_log_files:
        print(f"警告: 日志文件 {log.name} 的大小为 {log.SizeInMB} MB，可能需要收缩。")

    # 规则 84: 检查是否有禁用的触发器
    query84 = """
    SELECT OBJECT_NAME(parent_id) AS TableName, name AS TriggerName
    FROM sys.triggers
    WHERE is_disabled = 1
    """
    cursor.execute(query84)
    disabled_triggers = cursor.fetchall()
    for trigger in disabled_triggers:
        print(f"警告: 触发器 {trigger.TriggerName} 在表 {trigger.TableName} 上已被禁用。")

    # 规则 85: 检查表中是否存在大量 NULL 值的列
    query85 = """
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE COLUMNPROPERTY(OBJECT_ID(table_name), column_name, 'ColumnHasNullValues') = 1
    """
    cursor.execute(query85)
    columns_with_nulls = cursor.fetchall()
    for column in columns_with_nulls:
        print(f"警告: 表 {column.table_name} 中的列 {column.column_name} 存在大量的 NULL 值。")

    # 规则 86: 检查是否有未使用的存储过程
    query86 = """
    SELECT OBJECT_NAME(object_id) AS ProcedureName
    FROM sys.procedures
    WHERE OBJECTPROPERTY(object_id, 'ExecIsExecuted') = 0
    """
    cursor.execute(query86)
    unused_procs = cursor.fetchall()
    for proc in unused_procs:
        print(f"警告: 存储过程 {proc.ProcedureName} 似乎从未被执行过。")

    # 规则 87: 检查是否存在未使用的存储过程或函数
    query103 = """
    SELECT name AS ProcedureOrFunctionName
    FROM sys.objects
    WHERE type IN ('P', 'FN') AND OBJECTPROPERTY(object_id, 'ExecIsExecuted') = 0
    """
    cursor.execute(query103)
    unused_procs_funcs = cursor.fetchall()
    for proc_func in unused_procs_funcs:
        print(f"警告: 存储过程或函数 {proc_func.ProcedureOrFunctionName} 似乎从未被使用。")

    # 规则 88: 检查是否存在相同的索引名但在不同的表中
    query88 = """
    SELECT index_name, COUNT(DISTINCT table_name) AS table_count
    FROM information_schema.statistics
    GROUP BY index_name
    HAVING COUNT(DISTINCT table_name) > 1
    """
    cursor.execute(query88)
    duplicate_index_names = cursor.fetchall()
    for index in duplicate_index_names:
        print(f"警告: 索引名 {index.index_name} 在多个表中使用，可能会导致混淆。")

    # 规则 89: 检查是否有表缺失主键
    query104 = """
    SELECT name AS TableName
    FROM sys.tables
    WHERE type = 'U' AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query104 = query104.format(placeholders)

    cursor.execute(query104, tables_in_query)
    tables_without_primary_key = cursor.fetchall()
    for table in tables_without_primary_key:
        print(f"警告: 表 {table.TableName} 没有设置主键。")

    # 规则 90: 检查是否存在超过5个外键约束的表
    query90 = """
    SELECT OBJECT_NAME(fk.parent_object_id) AS TableName, COUNT(fk.name) AS ForeignKeyCount
    FROM sys.foreign_keys fk
    GROUP BY OBJECT_NAME(fk.parent_object_id)
    HAVING COUNT(fk.name) > 5
    """
    cursor.execute(query90)
    tables_with_many_fks = cursor.fetchall()
    for table in tables_with_many_fks:
        print(f"警告: 表 {table.TableName} 有 {table.ForeignKeyCount} 个外键约束，可能导致插入、更新操作变慢。")

    # 规则 91: 检查是否有的表缺少主键
    query91 = """
    SELECT name AS TableName
    FROM sys.tables
    WHERE type = 'U' 
    AND OBJECTPROPERTY(object_id,'TableHasPrimaryKey') = 0
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query91 = query91.format(placeholders)

    cursor.execute(query91, tables_in_query)
    tables_without_pk = cursor.fetchall()
    for table in tables_without_pk:
        print(f"警告: 表 {table.TableName} 缺少主键。")

    # 规则 92: 检查表是否使用了旧版本的数据类型（如 datetime）
    query92 = """
    SELECT table_name AS TableName, column_name AS ColumnName 
    FROM information_schema.columns 
    WHERE data_type = 'datetime'
    """
    cursor.execute(query92)
    datetime_columns = cursor.fetchall()
    for column in datetime_columns:
        print(
            f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 使用了旧版本的 datetime 数据类型，建议使用 datetime2 类型。")

    # 规则 93: 检查是否存在宽表（列数超过50的表）
    query93 = """
    SELECT table_name AS TableName, COUNT(column_name) AS ColumnCount
    FROM information_schema.columns
    GROUP BY table_name
    HAVING COUNT(column_name) > 50
    """
    cursor.execute(query93)
    wide_tables = cursor.fetchall()
    for table in wide_tables:
        print(f"警告: 表 {table.TableName} 是宽表，有 {table.ColumnCount} 列，可能导致查询性能下降。")

    # 规则 94: 检查是否存在未使用的触发器
    query94 = """
    SELECT OBJECT_NAME(t.object_id) AS TriggerName
    FROM sys.triggers t
    WHERE is_disabled = 1
    """
    cursor.execute(query94)
    unused_triggers = cursor.fetchall()
    for trigger in unused_triggers:
        print(f"警告: 触发器 {trigger.TriggerName} 当前是禁用状态，考虑是否需要移除。")

    # 规则 95: 检查表是否有很多空值的列
    query95 = """
    SELECT table_name AS TableName, column_name AS ColumnName, 
    (SELECT COUNT(*) FROM information_schema.tables WHERE column_name IS NULL) AS NullCount
    FROM information_schema.columns
    """
    cursor.execute(query95)
    null_columns = cursor.fetchall()
    for column in null_columns:
        if column.NullCount > 10000:  # 假设10,000作为警告阈值
            print(f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 有很多空值。")

    # 规则 96: 检查是否有非二进制列用 VARBINARY 数据类型
    query96 = """
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'varbinary' AND column_name NOT LIKE '%binary%'
    """
    cursor.execute(query96)
    non_binary_columns = cursor.fetchall()
    for column in non_binary_columns:
        print(
            f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 使用了 VARBINARY 数据类型，但可能并不是用于二进制数据。")

    # 规则 97: 检查是否存在没有 CHECK 约束的 ENUM 类型列
    query97 = """
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'varchar' AND column_name LIKE '%enum%'
    AND column_name NOT IN (
        SELECT column_name 
        FROM information_schema.check_constraints
    )
    """
    cursor.execute(query97)
    enum_columns_without_check = cursor.fetchall()
    for column in enum_columns_without_check:
        print(f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 似乎是 ENUM 类型但没有相应的 CHECK 约束。")

    # 规则 98: 检查是否有存储过程或函数未使用
    query98 = """
    SELECT name AS ProcedureOrFunctionName
    FROM sys.objects
    WHERE type IN ('P', 'FN') AND name NOT IN (
        SELECT OBJECT_NAME(object_id) 
        FROM sys.dm_exec_procedure_stats
    )
    """
    cursor.execute(query98)
    unused_procs_funcs = cursor.fetchall()
    for proc_func in unused_procs_funcs:
        print(f"警告: 存储过程或函数 {proc_func.ProcedureOrFunctionName} 似乎从未被使用。")

    # 规则 99: 检查是否存在使用 * 的查询
    query99 = """
    SELECT text AS QueryText
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle)
    WHERE text LIKE '%SELECT *%'
    """
    cursor.execute(query99)
    select_all_queries = cursor.fetchall()
    for query in select_all_queries:
        print(f"警告: 查询 {query.QueryText} 使用了 SELECT *，可能导致性能下降。")


    # 规则 100: 检查是否有的表没有统计信息
    query100 = """
    SELECT name AS TableName
    FROM sys.tables t
    WHERE NOT EXISTS (
        SELECT 1 
        FROM sys.stats s 
        WHERE s.object_id = t.object_id
    )
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query100 = query100.format(placeholders)

    cursor.execute(query100, tables_in_query)
    tables_without_stats = cursor.fetchall()
    for table in tables_without_stats:
        print(f"警告: 表 {table.TableName} 没有相关的统计信息，可能导致查询性能下降。")


    # 规则 101: 检查是否有表缺失索引
    query105 = """
    SELECT name AS TableName
    FROM sys.tables
    WHERE type = 'U' AND OBJECTPROPERTY(object_id, 'TableWithNoClusteredIndex') = 1
    AND name IN ({})
    """
    # 为 tables_in_query 列表中的每个表名创建一个问号（?）占位符
    placeholders = ', '.join('?' for _ in tables_in_query)
    query105 = query105.format(placeholders)

    cursor.execute(query105, tables_in_query)
    tables_without_clustered_index = cursor.fetchall()
    for table in tables_without_clustered_index:
        print(f"警告: 表 {table.TableName} 没有聚集索引。")

    cursor.close()
