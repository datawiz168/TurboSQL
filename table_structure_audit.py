import pyodbc

def audit_table_structure(conn, tables_in_query):

    cursor = conn.cursor()

    # 这将保证在函数的其余部分，我们可以像以前一样使用 `tables` 变量，而无需更改任何代码
    tables = tables_in_query
    # 现在，我们可以继续像以前一样使用 `tables` 变量
    tables_str = ', '.join([f"'{table}'" for table in tables])
    print("开始进行表结构审计...")
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

    # 规则 2: 检查缺失的外键
    query2 = f"""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  --使用 tables_str 限制表范围
    AND TABLE_NAME NOT IN (
        SELECT DISTINCT TABLE_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    );
    """
    cursor.execute(query2)
    tables_without_foreign_key = cursor.fetchall()
    for table, column in tables_without_foreign_key:
        print(f"警告: 表 {table} 的列 {column} 缺失外键约束。")

    # 规则 3: 检查数据冗余
    # 注意：这通常需要业务逻辑和数据知识来确定。以下是一个简化的示例，只是为了演示。
    query3 = f"""
    SELECT COLUMN_NAME, COUNT(DISTINCT DATA_TYPE) as data_type_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  
    GROUP BY COLUMN_NAME
    HAVING COUNT(DISTINCT DATA_TYPE) > 1;
    """
    cursor.execute(query3)
    redundant_data_columns = cursor.fetchall()
    for column, count in redundant_data_columns:
        print(f"警告: 列 {column} 在 {count} 个不同的表中使用了不同的数据类型，可能存在数据冗余。")

    # 规则 4: 检查数据类型不匹配
    # 注意：这是一个示例查询，可能需要根据实际情况进行调整。
    query4 = f"""
    SELECT COLUMN_NAME, DATA_TYPE, TABLE_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  -- 使用 tables_str 限制表范围
    AND COLUMN_NAME IN (
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME IN ({tables_str})  -- 再次使用 tables_str 限制表范围
        GROUP BY COLUMN_NAME
        HAVING COUNT(DISTINCT DATA_TYPE) > 1
    );
    """
    cursor.execute(query4)
    mismatched_data_types = cursor.fetchall()
    for column, data_type, table in mismatched_data_types:
        print(f"警告: 列 {column} 在表 {table} 使用了数据类型 {data_type}，这与其他表中的数据类型不一致。")

    # 规则 5: 检查没有设置适当的列默认值的列
    query5 = f"""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  -- 使用 tables_str 限制表范围
    AND COLUMN_DEFAULT IS NULL 
    AND IS_NULLABLE = 'NO';
    """
    cursor.execute(query5)
    columns_without_default = cursor.fetchall()
    for table, column in columns_without_default:
        print(f"警告: 表 {table} 的列 {column} 没有设置默认值。")

    # 规则 6: 检查是否使用非标准的数据类型
    query6 = f"""
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  -- 使用 tables_str 限制表范围
    AND DATA_TYPE IN ('text', 'ntext', 'image');  -- 示例非标准数据类型
    """
    cursor.execute(query6)
    non_standard_types = cursor.fetchall()
    for table, column, data_type in non_standard_types:
        print(f"警告: 表 {table} 的列 {column} 使用了非标准的数据类型 {data_type}。")

    # 规则 7: 检查列命名不规范
    # 这需要定义一个命名约定，以下是一个简化的示例
    query7 = f"""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  -- 使用 tables_str 限制表范围
    AND COLUMN_NAME LIKE '% %';  -- 包含空格的列名
    """
    cursor.execute(query7)
    improperly_named_columns = cursor.fetchall()
    for table, column in improperly_named_columns:
        print(f"警告: 表 {table} 的列 {column} 命名不规范。")

    # 规则 8: 检查数据冗余
    # 注意：这需要具体的业务逻辑和数据知识，以下只是一个简化示例
    query8 = f"""
    SELECT COLUMN_NAME, COUNT(DISTINCT TABLE_NAME) as table_count
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME IN ({tables_str})  -- 使用 tables_str 限制表范围
    GROUP BY COLUMN_NAME
    HAVING COUNT(DISTINCT TABLE_NAME) > 1;
    """
    cursor.execute(query8)
    redundant_data = cursor.fetchall()
    for column, count in redundant_data:
        print(f"警告: 列名 {column} 在 {count} 个不同的表中出现，可能存在数据冗余。")

    # 规则 9: 检查使用过大的数据类型
    query9 = f"""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE DATA_TYPE = 'BIGINT' 
    AND COLUMN_NAME LIKE '%age%'  -- 示范性地检查是否用BIGINT存储年龄
    AND TABLE_NAME IN ({tables_str});  -- 使用 tables_str 限制表范围
    """
    cursor.execute(query9)
    oversized_data_types = cursor.fetchall()
    for table, column in oversized_data_types:
        print(f"警告: 表 {table} 的列 {column} 使用了不必要的大数据类型。")

    # 规则 10: 检查没有使用SCHEMA进行组织的表
    query10 = f"""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME IN ({tables_str}) AND TABLE_SCHEMA = 'dbo';  -- 默认的SCHEMA
    """

    cursor.execute(query10)
    tables_without_schema = cursor.fetchall()
    for table in tables_without_schema:
        print(f"警告: 表 {table[0]} 没有使用SCHEMA进行组织。")

    # 规则 11: 检查是否有触发器
    query11 = f"""
    SELECT OBJECT_NAME(parent_id) AS TABLE_NAME,
           name AS TRIGGER_NAME
    FROM sys.triggers
    WHERE type = 'TR'
    AND OBJECT_NAME(parent_id) IN ({tables_str});  -- 使用 tables_str 限制表范围
    """
    cursor.execute(query11)
    tables_with_triggers = cursor.fetchall()
    for table, trigger in tables_with_triggers:
        print(f"警告: 表 {table} 使用了触发器 {trigger}，可能影响性能。")

    # 规则 12: 检查存储过程和函数的效率
    # 这是一个示例查询，可能需要根据实际情况进行调整。
    query12 = f"""
    SELECT ROUTINE_NAME, ROUTINE_TYPE
    FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_DEFINITION LIKE '%CURSOR%' AND 
    ({' OR '.join([f"ROUTINE_DEFINITION LIKE '%{table}%'" for table in tables])});  -- 查找使用了CURSOR且涉及特定表的存储过程或函数
    """
    cursor.execute(query12)
    inefficient_routines = cursor.fetchall()
    for routine, type in inefficient_routines:
        print(f"警告: {type} {routine} 使用了CURSOR，可能效率低下。")

    # 规则 13: 检查缺少统计信息的表
    tables_str = ', '.join([f"'{table}'" for table in tables])
    query13 = f"""
    -- 由于SQL Server不直接提供此信息，此查询需要使用DMV进行查询
    SELECT OBJECT_NAME(object_id) AS TableName
    FROM sys.objects
    WHERE type = 'U' AND OBJECT_NAME(object_id) IN ({tables_str}) AND object_id NOT IN (SELECT DISTINCT object_id FROM sys.stats);
    """
    cursor.execute(query13)
    tables_without_stats = cursor.fetchall()
    for table in tables_without_stats:
        print(f"警告: 表 {table[0]} 缺少统计信息。")

    # 规则 14: 检查统计信息是否过时
    query14 = f"""
    -- 以下查询检查上次更新统计信息超过30天的表
    SELECT OBJECT_NAME(s.object_id) AS TableName, s.name AS StatName, STATS_DATE(s.object_id, s.stats_id) AS LastUpdated
    FROM sys.stats s
    WHERE DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30
    AND OBJECT_NAME(s.object_id) IN ({tables_str});
    """
    cursor.execute(query14)
    outdated_stats = cursor.fetchall()
    for table, stat, last_updated in outdated_stats:
        print(f"警告: 表 {table} 的统计信息 {stat} 自上次更新已超过30天。")

    # 规则 15: 检查缺少分区的大表
    query15 = f"""
    -- 示例查询检查行数超过1百万的未分区表
    SELECT OBJECT_NAME(p.object_id) AS TableName, SUM(p.rows) AS TotalRows
    FROM sys.partitions p
    WHERE p.index_id < 2
    AND OBJECT_NAME(p.object_id) IN ({tables_str})
    GROUP BY p.object_id
    HAVING SUM(p.rows) > 1000000 AND COUNT(p.partition_number) = 1;
    """
    cursor.execute(query15)
    large_unpartitioned_tables = cursor.fetchall()
    for table, rows in large_unpartitioned_tables:
        print(f"警告: 表 {table} 有 {rows} 行，但没有分区。")

    # 规则 16: 检查是否所有的表都有主键
    query16 = f"""
    SELECT name
    FROM sys.tables
    WHERE name IN ({tables_str})
    AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    """
    cursor.execute(query16)
    tables_without_primarykey = cursor.fetchall()

    for table in tables_without_primarykey:
        print(f"警告: 表 {table[0]} 没有主键。")

    # 规则 17: 检查是否存在过大的表但未建立任何索引
    query17 = f"""
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
    AND t.name IN ({tables_str})
    """
    cursor.execute(query17)
    large_tables_without_indexes = cursor.fetchall()

    for table in large_tables_without_indexes:
        print(f"警告: 表 {table[0]} 有超过100,000行但没有索引。")

    # 规则 18: 检查数据类型是否适当
    query18 = f"""
    SELECT table_name, column_name, data_type 
    FROM information_schema.columns 
    WHERE data_type IN ('text', 'ntext')
    AND table_name IN ({tables_str})
    """
    cursor.execute(query18)
    inappropriate_data_types = cursor.fetchall()
    for table, column, data_type in inappropriate_data_types:
        print(f"警告: 表 {table} 中的列 {column} 使用了 {data_type} 数据类型，考虑使用VARCHAR(MAX)或NVARCHAR(MAX)。")

    # 规则 19: 检查是否存在过多的NULL值
    query19 = f"""
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE is_nullable = 'YES' AND table_name IN ({tables_str})
    """
    cursor.execute(query19)
    columns_with_nulls = cursor.fetchall()
    for table, column in columns_with_nulls:
        print(f"警告: 表 {table} 中的列 {column} 允许NULL值。")

    # 规则 20: 检查表是否存在太多的外键关联
    query20 = f"""
    SELECT OBJECT_NAME(parent_object_id) AS TableName, COUNT(*) AS FKCount
    FROM sys.foreign_keys
    WHERE OBJECT_NAME(parent_object_id) IN ({tables_str})
    GROUP BY parent_object_id
    HAVING COUNT(*) > 5
    """
    cursor.execute(query20)
    tables_with_many_foreign_keys = cursor.fetchall()

    for table, fk_count in tables_with_many_foreign_keys:
        print(f"警告: 表 {table} 有超过5个外键关联。")

    # 规则 21: 检查是否存在大量的列是 VARCHAR(MAX) 或 NVARCHAR(MAX)
    query21 = f"""
    SELECT table_name, column_name 
    FROM information_schema.columns 
    WHERE table_name IN ({tables_str})
    AND data_type IN ('varchar', 'nvarchar') 
    AND character_maximum_length = -1
    """
    cursor.execute(query21)
    columns_with_large_varchar = cursor.fetchall()
    for table, column in columns_with_large_varchar:
        print(f"警告: 表 {table} 中的列 {column} 使用了VARCHAR(MAX)或NVARCHAR(MAX)数据类型，可能导致性能下降。")

    # 规则 22: 检查是否存在过大的非聚集索引
    query22 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(ps.used_page_count) * 8 AS IndexSizeKB
    FROM sys.dm_db_partition_stats ps
    JOIN sys.indexes i ON i.object_id = ps.object_id
    WHERE i.type_desc = 'NONCLUSTERED'
    AND OBJECT_NAME(i.object_id) IN ({tables_str})
    GROUP BY i.object_id, i.name
    HAVING SUM(ps.used_page_count) * 8 > 5000  -- 5MB为界限
    """
    cursor.execute(query22)
    large_nonclustered_indexes = cursor.fetchall()
    for table, index, size in large_nonclustered_indexes:
        print(f"警告: 表 {table} 的非聚集索引 {index} 大小为 {size}KB，可能导致I/O开销增加。")

    # 规则 23: 检查是否有冗余索引
    query23 = f"""
    SELECT OBJECT_NAME(s.object_id) AS TableName, i.name AS IndexName
    FROM sys.dm_db_index_usage_stats s
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE database_id = DB_ID() AND user_seeks = 0 AND user_scans = 0 AND user_lookups = 0
    AND OBJECT_NAME(s.object_id) IN ({tables_str})
    """
    cursor.execute(query23)
    redundant_indexes = cursor.fetchall()

    for table, index in redundant_indexes:
        print(f"警告: 表 {table} 的索引 {index} 可能是冗余的，因为它没有被查询使用过。")

    # 规则 24: 检查是否有浮点数据类型，可能导致不准确的计算
    query24 = f"""
    SELECT OBJECT_NAME(c.object_id) AS TableName, c.name AS ColumnName
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE t.name IN ('float', 'real') AND OBJECT_NAME(c.object_id) IN ({tables_str})
    """
    cursor.execute(query24)
    floating_point_columns = cursor.fetchall()

    for table, column in floating_point_columns:
        print(f"警告: 表 {table} 中的列 {column} 使用了浮点数据类型，可能导致不准确的计算。")

    # 规则 25: 检查是否有表没有更新统计信息
    query25 = f"""
    SELECT name FROM sys.tables WHERE name IN ({tables_str})
    AND OBJECTPROPERTY(object_id, 'IsUserTable') = 1 AND DATEDIFF(d, STATS_DATE(object_id, NULL), GETDATE()) > 30
    """
    cursor.execute(query25)
    outdated_statistics_tables = cursor.fetchall()
    for table in outdated_statistics_tables:
        print(f"警告: 表 {table[0]} 长时间未更新统计信息，可能导致查询性能下降。")

    # 规则26: 检查是否有表没有主键
    query26 = f"""
    SELECT name FROM sys.tables
    WHERE type = 'U' AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND name IN ({tables_str})
    """
    cursor.execute(query26)
    # 获取查询结果
    tables_without_primary_key = cursor.fetchall()
    # 对于每一个没有主键的表，打印一个警告消息
    for table in tables_without_primary_key:
        print(f"警告: 表 {table[0]} 没有主键，可能导致数据完整性问题和查询性能下降。")

    # 规则 27: 检查是否有过大的表没有聚集索引
    query27 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName
    FROM sys.indexes i
    WHERE i.type = 0
    AND OBJECT_NAME(i.object_id) IN ({tables_str})
    """
    cursor.execute(query27)
    # 获取查询结果
    large_tables_without_clustered_index = cursor.fetchall()
    # 对于每一个没有聚集索引的表，打印一个警告消息
    for table in large_tables_without_clustered_index:
        print(f"警告: 表 {table[0]} 没有聚集索引，可能导致查询性能下降和数据存储不连续。")

    # 规则 28: 检查是否有列存储为文本数据类型（text, ntext, image）
    query28 = f"""
    SELECT table_name, column_name FROM information_schema.columns
    WHERE data_type IN ('text', 'ntext', 'image') AND table_name IN ({tables_str})
    """
    cursor.execute(query28)
    # 获取查询结果
    columns_with_text_datatype = cursor.fetchall()
    # 对于每一个存储为文本数据类型的列，打印一个警告消息
    for table, column in columns_with_text_datatype:
        print(
            f"警告: 表 {table} 中的列 {column} 使用了已过时的文本数据类型，考虑使用varchar(max)、nvarchar(max)或varbinary(max)替代。")

    # 规则 29: 检查大型 BLOB 数据
    query29 = f"""
    SELECT t.name AS TableName, c.name AS ColumnName
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE c.system_type_id = 34 AND t.name IN ({tables_str})  -- 34 是 SQL Server 中 IMAGE 数据类型的 ID，用于存储 BLOB 数据
    """
    cursor.execute(query29)
    # 获取查询结果
    blob_columns = cursor.fetchall()
    # 对于每一个存储为 BLOB 数据的列，打印一个警告消息
    for column in blob_columns:
        print(f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 是大型 BLOB 数据。")

    # 规则 30: 检查是否有冗余的外键约束
    query30 = f"""
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
    ) AND OBJECT_NAME(fk.parent_object_id) IN ({tables_str})
    """
    cursor.execute(query30)
    # 获取查询结果
    redundant_foreign_keys = cursor.fetchall()
    # 对于每一个冗余的外键约束，打印一个警告消息
    for fk, table in redundant_foreign_keys:
        print(f"警告: 表 {table} 的外键约束 {fk} 可能是冗余的。")

    # 规则 31: 检查是否存在不带默认值的非空列
    query31 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE is_nullable = 'NO' AND column_default IS NULL
    AND table_name IN ({tables_str})
    """
    cursor.execute(query31)
    # 获取查询结果
    non_null_columns_without_default = cursor.fetchall()
    # 对于每一个非空且没有默认值的列，打印一个警告消息
    for table, column in non_null_columns_without_default:
        print(f"警告: 表 {table} 中的列 {column} 是非空的，但没有设置默认值，可能导致插入数据时出错。")

    # 规则 32: 检查是否存在数据类型为FLOAT的列
    query32 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE data_type = 'float'
    AND table_name IN ({tables_str})
    """
    cursor.execute(query32)
    # 获取查询结果
    columns_with_float_datatype = cursor.fetchall()
    # 对于每一个数据类型为 FLOAT 的列，打印一个警告消息
    for table, column in columns_with_float_datatype:
        print(f"警告: 表 {table} 中的列 {column} 使用了FLOAT数据类型，可能导致精度问题，考虑使用DECIMAL或NUMERIC替代。")

    # 规则 33: 检查是否存在过大的VARCHAR列
    query33 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE data_type = 'varchar' AND character_maximum_length > 4000
    AND table_name IN ({tables_str})
    """
    cursor.execute(query33)
    # 获取查询结果
    oversized_varchar_columns = cursor.fetchall()
    # 对于每一个VARCHAR长度超过4000的列，打印一个警告消息
    for table, column in oversized_varchar_columns:
        print(f"警告: 表 {table} 中的列 {column} 的VARCHAR长度超过4000，可能导致存储问题和性能下降。")

    # 规则 34: 检查表是否有更新统计信息
    query34 = f"""
    SELECT OBJECT_NAME(object_id) AS TableName, last_user_update
    FROM sys.dm_db_index_usage_stats
    WHERE database_id = DB_ID() AND last_user_update IS NOT NULL
    AND OBJECT_NAME(object_id) IN ({tables_str})
    """
    cursor.execute(query34)
    # 获取查询结果
    tables_without_updated_stats = cursor.fetchall()
    # 对于每一个没有更新统计信息的表，打印一个警告消息
    for table, last_update in tables_without_updated_stats:
        print(f"警告: 表 {table} 的统计信息上次更新是在 {last_update}，考虑定期更新统计信息以提高查询性能。")

    # 规则 35: 检查是否有大量数据的表没有备份
    query35 = f"""
    SELECT OBJECT_NAME(object_id) AS TableName, SUM(rows) AS TotalRows
    FROM sys.partitions
    WHERE index_id IN (0, 1) AND OBJECT_NAME(object_id) IN ({tables_str})
    GROUP BY OBJECT_NAME(object_id)
    HAVING SUM(rows) > 1000000
    """
    cursor.execute(query35)
    # 获取查询结果
    large_tables_without_backup = cursor.fetchall()
    # 对于每一个行数超过1000000的表，打印一个警告消息
    for table, rows in large_tables_without_backup:
        print(f"警告: 表 {table} 有 {rows} 行数据，但没有备份，可能导致数据丢失风险。")

    # 规则 36: 检查是否存在大量NULL值的列
    query36 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_schema = 'dbo' AND table_name IN ({tables_str})
    """
    cursor.execute(query36)
    # 获取查询结果
    all_columns = cursor.fetchall()

    # 对于每一列，检查是否存在大量的 NULL 值
    for table, column in all_columns:
        count_query = f"""
        SELECT COUNT(*)
        FROM {table}
        WHERE {column} IS NULL
        """
        cursor.execute(count_query)
        null_count = cursor.fetchone()[0]

        # 如果 NULL 值的数量超过 1000，打印一个警告消息
        if null_count > 1000:  # 根据实际情况调整阈值
            print(f"警告: 表 {table} 中的列 {column} 存在大量NULL值，考虑优化表结构。")

    # 规则 37: 检查只含有一行的表
    query37 = f"""
    SELECT t.name AS TableName
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.rows = 1 AND t.name IN ({tables_str})
    """
    cursor.execute(query37)
    # 获取查询结果
    single_row_tables = cursor.fetchall()
    # 对于每一个只含有一行数据的表，打印一个警告消息
    for table in single_row_tables:
        print(f"警告: 表 {table.TableName} 只有一行数据。")

    # 规则 38: 检查是否有表使用GUID作为主键
    query38 = f"""
    SELECT o.name AS TableName, c.name AS ColumnName
    FROM sys.columns c
    JOIN sys.objects o ON o.object_id = c.object_id
    WHERE c.column_id = 1 AND c.system_type_id = 36 AND o.type = 'U' AND o.name IN ({tables_str})
    """
    cursor.execute(query38)
    # 获取查询结果
    tables_with_guid_primary_keys = cursor.fetchall()
    # 对于每一个使用GUID作为主键的表，打印一个警告消息
    for table, column in tables_with_guid_primary_keys:
        print(f"警告: 表 {table} 使用GUID ({column}) 作为主键，可能导致性能问题。")

    # 规则 39: 检查是否存在过大的CHAR列
    query39 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE data_type = 'char' AND character_maximum_length > 255 AND table_name IN ({tables_str})
    """
    cursor.execute(query39)
    # 获取查询结果
    oversized_char_columns = cursor.fetchall()
    # 对于每一个CHAR长度超过255的列，打印一个警告消息
    for table, column in oversized_char_columns:
        print(f"警告: 表 {table} 中的列 {column} 的CHAR长度超过255，考虑使用VARCHAR代替。")

    # 规则 40: 检查是否有表的行数超过特定阈值但没有任何索引
    query40 = f"""
    SELECT OBJECT_NAME(p.object_id) AS TableName, SUM(p.rows) AS TotalRows
    FROM sys.partitions p
    JOIN sys.indexes i ON p.object_id = i.object_id
    WHERE i.index_id = 0 AND OBJECTPROPERTY(p.object_id, 'IsUserTable') = 1
    AND OBJECT_NAME(p.object_id) IN ({tables_str})
    GROUP BY p.object_id
    HAVING SUM(p.rows) > 10000
    """
    cursor.execute(query40)
    # 获取查询结果
    large_tables_without_indexes = cursor.fetchall()
    # 对于每一个行数超过特定阈值但没有任何索引的表，打印一个警告消息
    for table, rows in large_tables_without_indexes:
        print(f"警告: 表 {table} 有 {rows} 行数据但没有任何索引，可能导致查询性能问题。")

    # 规则 41: 检查是否存在过大的Varchar列，但数据实际使用长度较小
    query41 = f"""
    SELECT table_name, column_name, max(len(column_name)) as MaxLength
    FROM information_schema.columns
    WHERE data_type = 'varchar' AND character_maximum_length > 255 AND table_name IN ({tables_str})
    GROUP BY table_name, column_name
    HAVING max(len(column_name)) < 100 -- 实际使用长度小于100
    """
    cursor.execute(query41)
    # 获取查询结果
    oversized_varchar_columns = cursor.fetchall()
    # 对于每一个Varchar最大长度大于255，但实际使用长度小于100的列，打印一个警告消息
    for table, column, length in oversized_varchar_columns:
        print(
            f"警告: 表 {table} 中的列 {column} 的Varchar最大长度为 {length}，但实际使用长度远小于此。考虑减小该列的最大长度。")

    # 规则 42: 检查表是否存在过多的列
    query42 = f"""
    SELECT table_name, COUNT(column_name) as ColumnCount
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})
    GROUP BY table_name
    HAVING COUNT(column_name) > 50  -- 考虑表中超过50个列可能是过多的
    """
    cursor.execute(query42)
    # 获取查询结果
    tables_with_excessive_columns = cursor.fetchall()
    # 对于每一个列数量超过50的表，打印一个警告消息
    for table, count in tables_with_excessive_columns:
        print(f"警告: 表 {table} 有 {count} 列，考虑是否可以优化表结构，避免过多列。")

    # 规则 43: 检查是否存在不带默认值的非NULL列
    query43 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE is_nullable = 'NO' AND column_default IS NULL AND table_name IN ({tables_str})
    """
    cursor.execute(query43)
    # 获取查询结果
    nonnull_columns_without_default = cursor.fetchall()
    # 对于每一个非NULL但没有默认值的列，打印一个警告消息
    for table, column in nonnull_columns_without_default:
        print(f"警告: 表 {table} 中的列 {column} 是非NULL的，但没有默认值。考虑为其设置一个默认值。")

    # 规则 44: 检查是否存在重复的数据
    query44 = f"""
    SELECT TableName, ColumnName, DuplicateCount
    FROM (
        SELECT table_name as TableName, column_name as ColumnName, count(column_name) as DuplicateCount
        FROM information_schema.columns
        WHERE table_name IN ({tables_str})
        GROUP BY table_name, column_name
    ) sub
    WHERE DuplicateCount > 1
    """
    cursor.execute(query44)
    # 获取查询结果
    duplicate_data = cursor.fetchall()
    # 对于每一个包含重复数据的列，打印一个警告消息
    for table, column, count in duplicate_data:
        print(f"警告: 在表 {table} 的列 {column} 中找到 {count} 个重复项。考虑删除重复数据。")

    # 规则 45: 检查是否存在未使用的表
    query45 = f"""
    SELECT name AS TableName
    FROM sys.tables
    WHERE OBJECTPROPERTY(object_id, 'TableHasClustIndex') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasNonClustIndex') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasPrimaryKey') = 0
    AND OBJECTPROPERTY(object_id, 'TableHasUniqueCnst') = 0
    AND OBJECTPROPERTY(object_id, 'TableWithNoTriggers') = 1
    AND name IN ({tables_str})
    """
    cursor.execute(query45)
    # 获取查询结果
    unused_tables = cursor.fetchall()
    # 对于每一个未使用的表，打印一个警告消息
    for table in unused_tables:
        print(f"警告: 表 {table[0]} 似乎未被使用。考虑是否可以删除它。")

    # 规则 46: 检查是否存在不带注释的列
    query46 = f"""
    SELECT t.name AS TableName, c.name AS ColumnName
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    LEFT JOIN sys.extended_properties e ON t.object_id = e.major_id AND c.column_id = e.minor_id
    WHERE (e.value IS NULL OR e.value = '') AND t.name IN ({tables_str})
    """
    cursor.execute(query46)
    # 获取查询结果
    columns_without_comments = cursor.fetchall()
    # 对于每一个不带注释的列，打印一个警告消息
    for table, column in columns_without_comments:
        print(f"警告: 表 {table} 中的列 {column} 缺少注释。为了更好的文档化，请考虑为其添加注释。")

    # 规则 47: 检查是否存在过大的表但缺少分区
    query47 = f"""
    SELECT
        t.name AS TableName,
        SUM(ps.reserved_page_count * 8 * 1024) AS TotalSize
    FROM
        sys.dm_db_partition_stats ps
    JOIN
        sys.tables t ON ps.object_id = t.object_id
    WHERE
        t.name IN ({tables_str})
    GROUP BY
        t.name
    HAVING
        SUM(ps.reserved_page_count * 8 * 1024) > 5000000000  -- 大于5GB
    """
    cursor.execute(query47)
    # 获取查询结果
    large_tables_without_partitioning = cursor.fetchall()
    # 对于每一个过大但未分区的表，打印一个警告消息
    for table, size in large_tables_without_partitioning:
        print(f"警告: 表 {table} 的大小为 {size} 字节，但未使用分区。考虑为这种大表使用分区。")

    # 规则 48: 检查是否存在带有多个外键约束的表
    query48 = f"""
    SELECT t.name AS TableName, COUNT(fk.name) AS ForeignKeyCount
    FROM sys.foreign_keys AS fk
    JOIN sys.tables AS t ON fk.parent_object_id = t.object_id
    WHERE
        t.name IN ({tables_str})
    GROUP BY t.name
    HAVING COUNT(fk.name) > 5  -- 考虑超过5个外键可能过多
    """
    cursor.execute(query48)
    # 获取查询结果
    tables_with_excessive_foreign_keys = cursor.fetchall()
    # 对于每一个带有过多外键约束的表，打印一个警告消息
    for table, count in tables_with_excessive_foreign_keys:
        print(f"警告: 表 {table} 有 {count} 个外键约束，这可能会影响性能。")

    # 规则 49: 检查空表
    query49 = f"""
    SELECT DISTINCT t.name AS TableName
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE p.rows = 0
    AND t.name IN ({tables_str})
    """

    cursor.execute(query49)
    # 获取查询结果
    empty_tables = cursor.fetchall()
    # 对于每一个空的表，打印一个警告消息
    for table in empty_tables:
        print(f"警告: 表 {table[0]} 是空的。")

    # 规则 50: 检查是否存在没有索引的表
    query50 = f"""
    SELECT t.name AS TableName
    FROM sys.tables t
    LEFT JOIN sys.indexes i ON t.object_id = i.object_id
    WHERE i.object_id IS NULL
    AND t.name IN ({tables_str})
    """
    cursor.execute(query50)
    # 获取查询结果
    tables_without_indexes = cursor.fetchall()
    # 对于每一个没有索引的表，打印一个警告消息
    for table in tables_without_indexes:
        print(f"警告: 表 {table[0]} 没有任何索引，这可能会影响查询性能。")

    # 规则 51: 检查存在自增列但是不是主键的表
    query51 = f"""
    SELECT t.name AS TableName, c.name AS ColumnName
    FROM sys.tables t
    JOIN sys.identity_columns c ON t.object_id = c.object_id
    LEFT JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    LEFT JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE (i.is_primary_key IS NULL OR i.is_primary_key = 0)
    AND t.name IN ({tables_str})
    """
    cursor.execute(query51)
    # 获取查询结果
    auto_increment_not_primary = cursor.fetchall()
    # 对于每一个存在自增列但是不是主键的表，打印一个警告消息
    for table, column in auto_increment_not_primary:
        print(f"警告: 表 {table} 的列 {column} 是自增列，但不是主键。考虑将其设置为主键以确保数据的唯一性。")

    # 规则 52: 检查列数据类型是否适当
    # 例如：用于存储日期的VARCHAR列
    query52 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName, data_type AS DataType
    FROM information_schema.columns
    WHERE column_name LIKE '%date%' AND data_type = 'varchar'
    AND table_name IN ({tables_str})
    """
    cursor.execute(query52)
    # 获取查询结果
    improper_date_columns = cursor.fetchall()
    # 对于每一个数据类型不适当的列，打印一个警告消息
    for table, column, dtype in improper_date_columns:
        print(f"警告: 表 {table} 的列 {column} 似乎用于存储日期，但其数据类型为 {dtype}。考虑更改数据类型以提高效率。")

    # 规则 53: 检查是否存在CHAR数据类型的列
    # CHAR类型通常比VARCHAR使用更多的存储空间
    query53 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'char' AND table_name IN ({tables_str})
    """
    cursor.execute(query53)
    # 获取查询结果
    char_columns = cursor.fetchall()
    # 对于每一个使用CHAR数据类型的列，打印一个警告消息
    for table, column in char_columns:
        print(f"警告: 表 {table} 的列 {column} 使用CHAR数据类型，这可能导致存储浪费。考虑使用VARCHAR数据类型。")

    # 规则 54: 检查是否存在未引用的外键
    query54 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.key_column_usage k
    LEFT JOIN information_schema.referential_constraints r ON k.constraint_name = r.constraint_name
    WHERE r.constraint_name IS NULL AND table_name IN ({tables_str})
    """
    cursor.execute(query54)
    # 获取查询结果
    unreferenced_foreign_keys = cursor.fetchall()
    # 对于每一个未引用的外键，打印一个警告消息
    for table, column in unreferenced_foreign_keys:
        print(f"警告: 表 {table} 的列 {column} 被定义为外键，但未引用任何表。")

    # 规则 55: 检查是否存在冗余的索引
    query55 = f"""
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName
    FROM sys.indexes ix
    INNER JOIN sys.index_columns ic1 ON ix.object_id = ic1.object_id AND ix.index_id = ic1.index_id
    INNER JOIN sys.index_columns ic2 ON ic1.object_id = ic2.object_id AND ic1.column_id = ic2.column_id
    WHERE ic1.index_id <> ic2.index_id
    AND ix.object_id IN (SELECT object_id FROM sys.tables WHERE name IN ({tables_str}))
    """
    cursor.execute(query55)
    # 获取查询结果
    redundant_indexes = cursor.fetchall()
    # 对于每一个存在冗余索引的表，打印一个警告消息
    for table, index in redundant_indexes:
        print(f"警告: 表 {table} 存在冗余的索引 {index}。考虑删除冗余索引以提高写操作性能。")

    # 规则 56: 检查是否存在只读表，但仍有索引
    # 这样的表可能不需要多个索引，因为它们不涉及写操作
    # 这个查询假设只读表的名称都包含 'readonly' 这个词
    # 如果你有其他的方式来标记只读表，你需要修改这个查询

    # 插入占位符到查询中
    query56 = f"""
    SELECT t.name AS TableName, COUNT(i.name) AS IndexCount
    FROM sys.tables t
    JOIN sys.indexes i ON t.object_id = i.object_id
    WHERE t.name LIKE '%readonly%'
    AND t.name IN ({tables_str})  -- 使用逗号分隔的表名字符串
    GROUP BY t.name
    HAVING COUNT(i.name) > 1
    """
    cursor.execute(query56)
    readonly_with_multiple_indexes = cursor.fetchall()
    for table, index_count in readonly_with_multiple_indexes:
        print(f"警告: 表 {table} 是只读的，但存在 {index_count} 个索引。考虑优化其索引结构。")

    # 规则 57: 检查是否存在具有默认值的列，但未被引用
    query57 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE column_default IS NOT NULL 
    AND column_name NOT IN (
        SELECT column_name
        FROM information_schema.key_column_usage
    )
    AND table_name IN ({tables_str})  -- 限制在查询表的范围内
    """
    cursor.execute(query57)
    default_columns_not_referenced = cursor.fetchall()
    for table, column in default_columns_not_referenced:
        print(f"警告: 表 {table} 的列 {column} 有默认值，但在其他表中未被引用。")

    #规则58 待递增

    # 规则 59: 检查是否存在无文档的存储过程
    query59 = """
    SELECT p.name AS ProcedureName
    FROM sys.procedures p
    LEFT JOIN sys.extended_properties e ON p.object_id = e.major_id
    WHERE e.value IS NULL OR e.value = ''
    """
    cursor.execute(query59)
    undocumented_procedures = cursor.fetchall()
    for proc in undocumented_procedures:
        print(f"警告: 存储过程 {proc} 没有文档或描述。")

    # 规则 60: 检查是否存在未设置的外键约束
    query60 = f"""
    SELECT COLUMN_NAME, TABLE_NAME
    FROM information_schema.columns
    WHERE COLUMN_NAME LIKE '%_id' 
    AND COLUMN_NAME NOT IN (
        SELECT COLUMN_NAME
        FROM information_schema.key_column_usage
    )
    AND TABLE_NAME IN ({tables_str})
    """
    cursor.execute(query60)
    potential_missing_fk = cursor.fetchall()
    for column, table in potential_missing_fk:
        print(f"警告: 表 {table} 的列 {column} 看起来像是一个外键，但未设置外键约束。")

    # 规则 61: 检查使用GUID作为主键的表
    query61 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'uniqueidentifier' AND column_name IN (
        SELECT column_name
        FROM information_schema.key_column_usage
    )
    AND table_name IN ({tables_str})  -- 添加这行来限制查询的表范围
    """
    cursor.execute(query61)
    guid_as_primary_key = cursor.fetchall()
    for table, column in guid_as_primary_key:
        print(f"警告: 表 {table} 使用GUID {column} 作为主键，这可能会导致性能问题和碎片化。")

    # 规则 62: 检查使用 TEXT 或 NTEXT 数据类型的列
    query62 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type IN ('text', 'ntext')
    AND table_name IN ({tables_str})  -- 添加这行来限制查询的表范围
    """
    cursor.execute(query62)
    text_columns = cursor.fetchall()
    for table, column in text_columns:
        print(
            f"警告: 表 {table} 的列 {column} 使用了 TEXT 或 NTEXT 数据类型，建议使用 VARCHAR(MAX) 或 NVARCHAR(MAX) 代替。")

    # 规则 63: 检查使用 IMAGE 数据类型的列
    query63 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'image'
    AND table_name IN ({tables_str})  -- 添加这行来限制查询的表范围
    """
    cursor.execute(query63)
    image_columns = cursor.fetchall()
    for table, column in image_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 IMAGE 数据类型，建议使用 VARBINARY(MAX) 代替。")

    # 规则 64: 检查存在超过 5 个索引的表
    query64 = f"""
    SELECT OBJECT_NAME(ind.object_id) AS TableName, COUNT(*) as IndexCount
    FROM sys.indexes ind
    WHERE ind.object_id IN (
        SELECT object_id FROM sys.tables 
        WHERE name IN ({tables_str})  -- 使用 tables_str，而不是占位符
    )
    GROUP BY ind.object_id
    HAVING COUNT(*) > 5
    """
    cursor.execute(query64)
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
        if table in tables_in_query:  # 检查表名是否在列表中
            print(f"警告: 表 {table} 的统计信息在 {last_updated} 之后未更新，建议更新统计信息。")

    # 规则 66: 检查表的大小
    query66 = f'''
    SELECT TableName, TotalRows
    FROM (
        SELECT t.name TableName, SUM(p.rows) TotalRows
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0, 1)
        AND t.name IN ({tables_str})  -- 在这里添加表名范围限制
        GROUP BY t.name
    ) AS SubQuery
    WHERE SubQuery.TotalRows > 5000000
    '''
    cursor.execute(query66)
    large_tables = cursor.fetchall()
    for table in large_tables:
        print(f"警告: 表 {table.TableName} 的行数为 {table.TotalRows}，可能过大。")

    # 规则 67: 检查使用 TEXT 数据类型的列
    query67 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'text'
    AND table_name IN ({tables_str})  -- 在这里添加表名范围限制
    """
    cursor.execute(query67)
    text_columns = cursor.fetchall()
    for table, column in text_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 TEXT 数据类型，可能影响查询性能。")

    # 规则 68: 检查使用 BIT 数据类型的列
    query68 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'bit'
    AND table_name IN ({tables_str})  -- 在这里添加表名范围限制
    """
    cursor.execute(query68)
    bit_columns = cursor.fetchall()
    for table, column in bit_columns:
        print(f"警告: 表 {table} 的列 {column} 使用了 BIT 数据类型，可能导致查询优化器做出不佳的决策。")

    # 规则 69: 检查存在空字符串默认值的列
    query69 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE column_default = ''
    AND table_name IN ({tables_str})  -- 在这里添加表名范围限制
    """
    cursor.execute(query69)
    empty_default_value_columns = cursor.fetchall()
    for table, column in empty_default_value_columns:
        print(f"警告: 表 {table} 的列 {column} 的默认值是空字符串，可能会导致意外的行为。")

    # 规则 70: 检查存在空格默认值的列
    query70 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE column_default = ' '
    AND table_name IN ({tables_str})  -- 在这里添加表名范围限制
    """
    cursor.execute(query70)
    space_default_value_columns = cursor.fetchall()
    for table, column in space_default_value_columns:
        print(f"警告: 表 {table} 的列 {column} 的默认值是空格，可能会导致意外的行为。")

    # 规则 71: 检查表中是否有多个 TIMESTAMP/ROWVERSION 数据类型的列
    query71 = f"""
    SELECT table_name AS TableName, COUNT(column_name) AS TimestampColumnsCount
    FROM information_schema.columns
    WHERE data_type = 'timestamp'
    AND table_name IN ({tables_str})  -- 在这里添加表名范围限制
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
    query73 = f"""
    SELECT f.name AS ForeignKey, OBJECT_NAME(f.parent_object_id) AS TableName, COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName
    FROM sys.foreign_keys AS f
    JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
    LEFT JOIN sys.index_columns AS ic ON ic.object_id = fc.parent_object_id AND ic.column_id = fc.parent_column_id
    WHERE ic.object_id IS NULL
    AND OBJECT_NAME(f.parent_object_id) IN ({tables_str})  -- 在这里添加表名范围限制
    """
    cursor.execute(query73)
    unindexed_foreign_keys = cursor.fetchall()
    for fk, table, column in unindexed_foreign_keys:
        print(f"警告: 表 {table} 的外键 {fk} 在列 {column} 上没有相应的索引支持。")

    # 规则 74: 检查是否有大型表（例如，行数超过100万）但没有聚集索引
    query74 = f"""
    SELECT o.name AS TableName, p.rows AS NumRows
    FROM sys.objects o
    JOIN sys.partitions p ON o.object_id = p.object_id
    WHERE o.type = 'U' 
    AND p.index_id = 0 
    AND p.rows > 1000000
    AND o.name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query74)
    large_heap_tables = cursor.fetchall()
    for table, rows in large_heap_tables:
        print(f"警告: 表 {table} 有 {rows} 行，但没有聚集索引。")

    # 规则 75: 检查是否有存储过程或函数使用了 'sp_' 前缀
    query75 = f"""
    SELECT name AS RoutineName, type_desc AS RoutineType
    FROM sys.objects
    WHERE name LIKE 'sp_%' AND type IN ('P', 'FN', 'TF', 'IF')
    AND name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query75)
    sp_prefixed_routines = cursor.fetchall()
    for routine, type_desc in sp_prefixed_routines:
        print(f"警告: {type_desc} {routine} 使用了 'sp_' 前缀，这可能与系统存储过程发生冲突。")

    # 规则 76: 检查是否存在的外键是否引用了不存在的数据
    query76 = f"""
    SELECT fk.name AS ForeignKey,
           OBJECT_NAME(fk.parent_object_id) AS TableName,
           COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
           OBJECT_NAME (fk.referenced_object_id) AS ReferenceTableName,
           COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.foreign_key_columns AS fc ON fk.OBJECT_ID = fc.constraint_object_id
    WHERE OBJECT_NAME(fk.parent_object_id) IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query76)
    foreign_keys = cursor.fetchall()
    for fk in foreign_keys:
        print(f"信息: 外键 {fk.ForeignKey} 在表 {fk.TableName} 上引用了表 {fk.ReferenceTableName}。")

    # 规则 77: 检查是否有大于 1MB 的 VARCHAR 列
    query77 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName, character_maximum_length AS MaxLength
    FROM information_schema.columns
    WHERE data_type = 'varchar' AND character_maximum_length > 1000000
    AND table_name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query77)
    large_varchar_columns = cursor.fetchall()
    for table, column, length in large_varchar_columns:
        print(f"警告: 表 {table} 的列 {column} 的 VARCHAR 长度设置为 {length}，考虑使用 VARCHAR(MAX) 或者缩小长度。")

    # 规则 78: 检查是否有空的表（无数据）
    query78 = f"""
    SELECT o.name AS TableName
    FROM sys.objects o
    JOIN sys.partitions p ON o.object_id = p.object_id
    WHERE o.type = 'U' AND p.rows = 0
    AND o.name IN ({tables_str})  -- 添加表名范围限制
    GROUP BY o.name  -- 每个表只出现一次
    """
    cursor.execute(query78)
    empty_tables = cursor.fetchall()
    for table in empty_tables:
        print(f"警告: 表 {table} 是空的，考虑是否需要这个表。")

    # 规则 79: 检查是否有宽表（列数过多的表）
    query79 = f"""
    SELECT table_name AS TableName, COUNT(*) AS ColumnCount
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})  -- 添加表名范围限制
    GROUP BY table_name
    HAVING COUNT(*) > 50
    """
    cursor.execute(query79)
    wide_tables = cursor.fetchall()
    for table in wide_tables:
        print(f"警告: 表 {table.TableName} 的列数为 {table.ColumnCount}，可能是一个宽表。")

    # 规则 80: 检查是否有过大的单个事务
    query80 = """
    SELECT t.transaction_id, t.name, s.login_time,
           DATEDIFF(MINUTE, s.login_time, GETDATE()) AS duration_in_minutes
    FROM sys.dm_tran_active_transactions t
    JOIN sys.dm_tran_session_transactions st ON t.transaction_id = st.transaction_id
    JOIN sys.dm_exec_sessions s ON st.session_id = s.session_id
    WHERE DATEDIFF(MINUTE, s.login_time, GETDATE()) > 30
    """
    cursor.execute(query80)
    long_transactions = cursor.fetchall()
    for txn in long_transactions:
        print(f"警告: 事务 {txn.transaction_id} ({txn.name}) 已经运行了 {txn.duration_in_minutes} 分钟。")

    # 规则 81: 检查使用频率不高的索引
    query81 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName,
           s.user_seeks + s.user_scans + s.user_lookups AS TotalUses
    FROM sys.indexes i
    JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE s.user_seeks + s.user_scans + s.user_lookups < 10
    AND OBJECT_NAME(i.object_id) IN ({tables_str})
    """
    cursor.execute(query81)
    infrequently_used_indexes = cursor.fetchall()
    for index in infrequently_used_indexes:
        print(f"警告: 索引 {index.IndexName} 在表 {index.TableName} 很少被使用，考虑删除。")

    # 规则 82: 检查重复的列数据
    # 对于这个规则，我们需要将搜索限制在 tables_str 列表中的特定表的列上。
    # 我们可以通过在 WHERE 子句中添加额外的条件来实现。
    query82 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})  -- 限制到特定的表
    AND column_name IN
    (SELECT column_name FROM information_schema.columns WHERE table_name IN ({tables_str}) GROUP BY column_name HAVING COUNT(table_name) > 1)
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
    query84 = f"""
    SELECT OBJECT_NAME(parent_id) AS TableName, name AS TriggerName
    FROM sys.triggers
    WHERE is_disabled = 1 AND OBJECT_NAME(parent_id) IN ({tables_str})
    """
    cursor.execute(query84)
    disabled_triggers = cursor.fetchall()
    for trigger in disabled_triggers:
        print(f"警告: 触发器 {trigger.TriggerName} 在表 {trigger.TableName} 上已被禁用。")

    # 规则 85: 检查表中是否存在大量 NULL 值的列
    query85 = f"""
    SELECT table_name, column_name
    FROM information_schema.columns
    WHERE COLUMNPROPERTY(OBJECT_ID(table_name), column_name, 'ColumnHasNullValues') = 1
    AND table_name IN ({tables_str})
    """
    cursor.execute(query85)
    columns_with_nulls = cursor.fetchall()
    for column in columns_with_nulls:
        print(f"警告: 表 {column.table_name} 中的列 {column.column_name} 存在大量的 NULL 值。")

    # 规则 86: 检查是否有未使用的存储过程
    # 请注意，我们无法直接在查询中限制表范围，因为这是关于存储过程的查询，而不是表的查询
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
    # 注意，我们无法在查询中直接限制表的范围，因为这是关于存储过程和函数的查询，而不是表的查询
    query87 = """
    SELECT name AS ProcedureOrFunctionName
    FROM sys.objects
    WHERE type IN ('P', 'FN') AND OBJECTPROPERTY(object_id, 'ExecIsExecuted') = 0
    """
    cursor.execute(query87)
    unused_procs_funcs = cursor.fetchall()
    for proc_func in unused_procs_funcs:
        print(f"警告: 存储过程或函数 {proc_func.ProcedureOrFunctionName} 似乎从未被使用。")

    # 规则 88: 检查是否存在相同的索引名但在不同的表中
    query88 = f"""
    SELECT i.name AS IndexName, COUNT(DISTINCT t.name) AS TableCount
    FROM sys.indexes i
    JOIN sys.tables t ON i.object_id = t.object_id
    WHERE t.name IN ({tables_str})  -- 添加表名范围限制
    GROUP BY i.name
    HAVING COUNT(DISTINCT t.name) > 1
    """
    cursor.execute(query88)
    duplicate_index_names = cursor.fetchall()
    for index in duplicate_index_names:
        print(f"警告: 索引名 {index.IndexName} 在多个表中使用，可能会导致混淆。")

    # 规则 89: 检查宽表
    query89 = f'''
    SELECT t.name AS TableName, COUNT(c.name) AS ColumnCount
    FROM sys.tables t
    JOIN sys.columns c ON t.object_id = c.object_id
    WHERE t.name IN ({tables_str})  -- 添加表名范围限制
    GROUP BY t.name
    HAVING COUNT(c.name) > 20  -- 考虑超过20列可能过宽
    '''
    cursor.execute(query89)
    wide_tables = cursor.fetchall()
    for table in wide_tables:
        print(f"警告: 表 {table.TableName} 的列数为 {table.ColumnCount}，可能过宽。")

    # 规则 90: 检查是否存在超过5个外键约束的表
    query90 = f"""
    SELECT OBJECT_NAME(fk.parent_object_id) AS TableName, COUNT(fk.name) AS ForeignKeyCount
    FROM sys.foreign_keys fk
    WHERE OBJECT_NAME(fk.parent_object_id) IN ({tables_str})  -- 添加表名范围限制
    GROUP BY OBJECT_NAME(fk.parent_object_id)
    HAVING COUNT(fk.name) > 5
    """
    cursor.execute(query90)
    tables_with_many_fks = cursor.fetchall()
    for table in tables_with_many_fks:
        print(f"警告: 表 {table.TableName} 有 {table.ForeignKeyCount} 个外键约束，可能导致插入、更新操作变慢。")

    # 规则 91: 检查是否存在大型表但无索引
    query91 = f"""
    SELECT t.name AS TableName, SUM(p.rows) AS TotalRows
    FROM sys.tables t
    JOIN sys.partitions p ON t.object_id = p.object_id
    WHERE t.name IN ({tables_str})  -- 添加表名范围限制
    AND p.index_id IN (0, 1)  -- 只考虑堆或聚集索引
    AND NOT EXISTS (SELECT 1 FROM sys.indexes i WHERE t.object_id = i.object_id AND i.type > 0)  -- 不存在非堆索引
    GROUP BY t.name
    HAVING SUM(p.rows) > 1000000  -- 根据实际情况调整行数阈值
    """
    cursor.execute(query91)
    large_tables_without_indexes = cursor.fetchall()
    for table in large_tables_without_indexes:
        print(f"警告: 表 {table.TableName} 的大小为 {table.RowCount} 行，但没有非堆索引，这可能会影响查询性能。")

    # 规则 92: 检查表是否使用了旧版本的数据类型（如 datetime）
    query92 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})  -- 添加表名范围限制
    AND data_type = 'datetime'
    """
    cursor.execute(query92)
    datetime_columns = cursor.fetchall()
    for column in datetime_columns:
        print(
            f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 使用了旧版本的 datetime 数据类型，建议使用 datetime2 类型。")

    # 规则 93: 检查是否存在宽表（列数超过50的表）
    query93 = f"""
    SELECT table_name AS TableName, COUNT(column_name) AS ColumnCount
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})  -- 添加表名范围限制
    GROUP BY table_name
    HAVING COUNT(column_name) > 50
    """
    cursor.execute(query93)
    wide_tables = cursor.fetchall()
    for table in wide_tables:
        print(f"警告: 表 {table.TableName} 是宽表，有 {table.ColumnCount} 列，可能导致查询性能下降。")

    # 规则 94: 检查是否存在未使用的触发器
    query94 = f"""
    SELECT OBJECT_NAME(t.object_id) AS TriggerName
    FROM sys.triggers t
    WHERE is_disabled = 1
    AND OBJECT_NAME(t.parent_id) IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query94)
    unused_triggers = cursor.fetchall()
    for trigger in unused_triggers:
        print(f"警告: 触发器 {trigger.TriggerName} 当前是禁用状态，考虑是否需要移除。")

    # 修复规则 95，并限定表范围
    query95 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE table_name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query95)
    columns = cursor.fetchall()

    for column in columns:
        null_query = f"SELECT COUNT(*) AS NullCount FROM {column.TableName} WHERE {column.ColumnName} IS NULL"
        cursor.execute(null_query)
        null_count = cursor.fetchone().NullCount
        if null_count > 10000:  # 假设10,000作为警告阈值
            print(f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 有很多空值。")

    # 规则 96: 检查是否有非二进制列用 VARBINARY 数据类型，并限定表范围
    query96 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'varbinary' AND column_name NOT LIKE '%binary%'
    AND table_name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query96)
    non_binary_columns = cursor.fetchall()
    for column in non_binary_columns:
        print(
            f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 使用了 VARBINARY 数据类型，但可能并不是用于二进制数据。")

    # 规则 97: 检查是否存在没有 CHECK 约束的 ENUM 类型列，并限定表范围
    query97 = f"""
    SELECT table_name AS TableName, column_name AS ColumnName
    FROM information_schema.columns
    WHERE data_type = 'varchar' AND column_name LIKE '%enum%'
    AND column_name NOT IN (
        SELECT column_name
        FROM information_schema.check_constraints
    )
    AND table_name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query97)
    enum_columns_without_check = cursor.fetchall()
    for column in enum_columns_without_check:
        print(f"警告: 表 {column.TableName} 中的列 {column.ColumnName} 似乎是 ENUM 类型但没有相应的 CHECK 约束。")

    # 规则 98: 检查是否有存储过程或函数未使用，并限定表范围
    query98 = f"""
    SELECT name AS ProcedureOrFunctionName
    FROM sys.objects
    WHERE type IN ('P', 'FN') AND name NOT IN (
        SELECT OBJECT_NAME(object_id)
        FROM sys.dm_exec_procedure_stats
    )
    AND OBJECT_NAME(parent_object_id) IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query98)
    unused_procs_funcs = cursor.fetchall()
    for proc_func in unused_procs_funcs:
        print(f"警告: 存储过程或函数 {proc_func.ProcedureOrFunctionName} 似乎从未被使用。")

    # 修复规则 99: 检查是否存在使用 * 的查询
    query99 = f"""
    SELECT text AS QueryText, OBJECT_NAME(qt.objectid) AS TableName
    FROM sys.dm_exec_requests r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS qt
    WHERE text LIKE '%SELECT *%'
    AND OBJECT_NAME(qt.objectid) IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query99)
    select_all_queries = cursor.fetchall()
    for query in select_all_queries:
        print(f"警告: 查询 {query.QueryText} 使用了 SELECT *，可能导致性能下降。")

    # 修复规则 100: 检查是否有的表没有统计信息
    query100 = f"""
    SELECT name AS TableName
    FROM sys.tables t
    WHERE NOT EXISTS (
        SELECT 1
        FROM sys.stats s
        WHERE s.object_id = t.object_id
    )
    AND name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query100)
    tables_without_stats = cursor.fetchall()
    for table in tables_without_stats:
        print(f"警告: 表 {table.TableName} 没有相关的统计信息，可能导致查询性能下降。")

    # 修复规则 101: 检查是否有表缺失索引
    query101 = f"""
    SELECT name AS TableName
    FROM sys.tables
    WHERE type = 'U' AND OBJECTPROPERTY(object_id, 'TableWithNoClusteredIndex') = 1
    AND name IN ({tables_str})  -- 添加表名范围限制
    """
    cursor.execute(query101)
    tables_without_clustered_index = cursor.fetchall()
    for table in tables_without_clustered_index:
        print(f"警告: 表 {table.TableName} 没有聚集索引。")

    # 修复规则 102: 检查表中 NULL 值超过 50% 的列
    query102 = f'''
    SELECT
        t.[name] AS TableName,
        c.[name] AS ColumnName,
        p.[rows] AS TotalRows,
        SUM(CASE WHEN c.is_nullable = 1 THEN 1 ELSE 0 END) AS NullCount
    FROM [sys].[tables] t
    JOIN [sys].[columns] c ON t.[object_id] = c.[object_id]
    JOIN [sys].[partitions] p ON t.[object_id] = p.[object_id]
    WHERE p.[index_id] IN (0, 1)  -- 只考虑堆或聚集索引
    AND p.[rows] != 0  -- Ensure that we don't divide by zero
    AND t.[name] IN ({tables_str})  -- 添加表名范围限制
    GROUP BY t.[name], c.[name], p.[rows]
    HAVING SUM(CASE WHEN c.is_nullable = 1 THEN 1 ELSE 0 END) / CAST(p.[rows] AS FLOAT) > 0.5  -- NULL 值超过 50%
    '''

    cursor.execute(query102)
    columns_with_many_nulls = cursor.fetchall()

    for column in columns_with_many_nulls:
        print(
            f"警告: 表 {column.TableName} 的列 {column.ColumnName} 存在大量的 NULL 值，NULL 值的比例为 {column.NullCount / column.TotalRows}。")

    # 关闭游标
    cursor.close()
    print("表结构审计完成。")
