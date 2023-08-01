import pyodbc


def audit_indexes(conn):
    cursor = conn.cursor()

    # Rule 1: 检查未使用的索引
    query1 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i 
    LEFT JOIN sys.dm_db_index_usage_stats s 
    ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE s.index_id IS NULL AND i.is_primary_key = 0 AND i.is_unique = 0 AND i.is_unique_constraint = 0;
    """
    cursor.execute(query1)
    unused_indexes = cursor.fetchall()
    for index in unused_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 未被使用，考虑删除。")

    # Rule 2: 检查重复的索引
    query2 = """
    -- (查询代码，如之前提供的检查重复索引的代码)
    """
    cursor.execute(query2)
    duplicate_indexes = cursor.fetchall()
    for index in duplicate_indexes:
        print(f"警告: 表 {index[0]} 上存在重复的索引 {index[1]}。")

    # Rule 3: 检查表没有主键
    query3 = """
    SELECT name AS TableName
    FROM sys.tables 
    WHERE OBJECTPROPERTY(object_id,'TableHasPrimaryKey') = 0;
    """
    cursor.execute(query3)
    tables_without_pk = cursor.fetchall()
    for table in tables_without_pk:
        print(f"警告: 表 {table[0]} 没有主键。")

    # Rule 4: 检查表没有聚集索引
    query4 = """
    SELECT name AS TableName
    FROM sys.tables
    WHERE OBJECTPROPERTY(object_id, 'TableHasClustIndex') = 0;
    """
    cursor.execute(query4)
    tables_without_clustindex = cursor.fetchall()
    for table in tables_without_clustindex:
        print(f"警告: 表 {table[0]} 没有聚集索引。")

    # Rule 5: 检查索引的碎片程度
    query5 = """
    SELECT OBJECT_NAME(ips.object_id) AS TableName, 
           i.name AS IndexName, 
           ips.avg_fragmentation_in_percent
    FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, NULL) ips
    JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 30;
    """
    cursor.execute(query5)
    fragmented_indexes = cursor.fetchall()
    for index in fragmented_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 存在高碎片化。")

    # Rule 6: 检查具有宽列的索引
    query6 = """
    SELECT OBJECT_NAME(ic.object_id) AS TableName, 
           i.name AS IndexName, 
           SUM(c.max_length) AS TotalIndexWidth
    FROM sys.index_columns ic
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    GROUP BY ic.object_id, i.name
    HAVING SUM(c.max_length) > 900;
    """
    cursor.execute(query6)
    wide_indexes = cursor.fetchall()
    for index in wide_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 列宽度过大。")

    # Rule 7: 检查索引中是否缺少统计信息
    query7 = """
    SELECT OBJECT_NAME(s.object_id) AS TableName, 
           s.name AS StatName
    FROM sys.stats s
    WHERE s.auto_created = 0 AND NOT EXISTS (
        SELECT 1
        FROM sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
        WHERE sp.last_updated IS NOT NULL
    );
    """
    cursor.execute(query7)
    missing_statistics = cursor.fetchall()
    for stat in missing_statistics:
        print(f"警告: 表 {stat[0]} 上的统计信息 {stat[1]} 缺失。")

    # Rule 8: 检查过多的非聚集索引
    query8 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           COUNT(*) AS NonClusteredIndexCount
    FROM sys.indexes i
    WHERE i.type_desc = 'NONCLUSTERED'
    GROUP BY i.object_id
    HAVING COUNT(*) > 10;
    """
    cursor.execute(query8)
    tables_with_many_indexes = cursor.fetchall()
    for table in tables_with_many_indexes:
        print(f"警告: 表 {table[0]} 存在过多的非聚集索引。")

    # Rule 9: 检查禁用的索引
    query9 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i
    WHERE i.is_disabled = 1;
    """
    cursor.execute(query9)
    disabled_indexes = cursor.fetchall()
    for index in disabled_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 已被禁用。")

    # Rule 10: 检查索引创建或修改的日期
    query10 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName, 
           STATS_DATE(i.object_id, i.index_id) AS LastModifiedDate
    FROM sys.indexes i
    WHERE DATEDIFF(DAY, STATS_DATE(i.object_id, i.index_id), GETDATE()) > 365;
    """
    cursor.execute(query10)
    old_indexes = cursor.fetchall()
    for index in old_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 自上次修改或创建已经超过一年。")

     # Rule 11: 检查重建索引的需求
    query11 = """
    SELECT OBJECT_NAME(ips.object_id) AS TableName, 
           i.name AS IndexName, 
           ips.avg_fragmentation_in_percent
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'DETAILED') ips
    JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 70;
    """
    cursor.execute(query11)
    indexes_needing_rebuild = cursor.fetchall()
    for index in indexes_needing_rebuild:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 存在高碎片化，建议重建。")

    # Rule 12: 检查过期的统计信息
    query12 = """
    SELECT OBJECT_NAME(s.object_id) AS TableName, 
           s.name AS StatName
    FROM sys.stats s
    WHERE DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30;
    """
    cursor.execute(query12)
    outdated_statistics = cursor.fetchall()
    for stat in outdated_statistics:
        print(f"警告: 表 {stat[0]} 上的统计信息 {stat[1]} 已过期，建议更新。")

    # Rule 13: 检查没有使用的索引
    query13 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id
    WHERE us.user_seeks + us.user_scans + us.user_lookups = 0;
    """
    cursor.execute(query13)
    unused_indexes = cursor.fetchall()
    for index in unused_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 从未被使用。")

    # Rule 14: 检查表上的统计信息的最后更新时间
    query14 = """
    SELECT OBJECT_NAME(s.object_id) AS TableName, 
           s.name AS StatName,
           s.last_updated
    FROM sys.stats s
    WHERE DATEDIFF(day, s.last_updated, GETDATE()) > 30;  # 30天为阈值
    """
    cursor.execute(query14)
    outdated_stats = cursor.fetchall()
    for stat in outdated_stats:
        print(f"警告: 表 {stat[0]} 的统计信息 {stat[1]} 自上次更新已超过30天。")

    # Rule 15: 检查超过5MB的索引大小
    query15 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName,
           SUM(p.used_page_count * 8) AS IndexSizeKB
    FROM sys.indexes i
    JOIN sys.dm_db_partition_stats p ON i.object_id = p.object_id AND i.index_id = p.index_id
    GROUP BY i.object_id, i.name
    HAVING SUM(p.used_page_count * 8) > 5120;  # 5MB的大小
    """
    cursor.execute(query15)
    large_indexes = cursor.fetchall()
    for index in large_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 的大小超过5MB。")

    # Rule 16: 检查索引的填充因子
    query16 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName,
           i.fill_factor
    FROM sys.indexes i
    WHERE i.fill_factor < 70 OR i.fill_factor > 90;
    """
    cursor.execute(query16)
    fill_factor_issues = cursor.fetchall()
    for index in fill_factor_issues:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 的填充因子为 {index[2]}%。建议将其设置在70-90%之间。")

    # Rule 17: 检查索引的行锁定
    query17 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i
    WHERE i.allow_row_locks = 0;
    """
    cursor.execute(query17)
    no_row_locks = cursor.fetchall()
    for index in no_row_locks:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 禁止行锁定。")

    # Rule 18: 检查索引的页锁定
    query18 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i
    WHERE i.allow_page_locks = 0;
    """
    cursor.execute(query18)
    no_page_locks = cursor.fetchall()
    for index in no_page_locks:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 禁止页锁定。")

    # Rule 19: 检查索引的列数
    query19 = """
    SELECT OBJECT_NAME(ic.object_id) AS TableName, 
           i.name AS IndexName, 
           COUNT(ic.column_id) AS ColumnCount
    FROM sys.index_columns ic
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    GROUP BY ic.object_id, i.name
    HAVING COUNT(ic.column_id) > 5;
    """
    cursor.execute(query19)
    indexes_with_many_columns = cursor.fetchall()
    for index in indexes_with_many_columns:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 包含超过5个列。")

    # Rule 20: 检查离线重建的索引
    query20 = """
    SELECT OBJECT_NAME(i.object_id) AS TableName, 
           i.name AS IndexName
    FROM sys.indexes i
    WHERE i.is_online = 0;
    """
    cursor.execute(query20)
    offline_rebuild_indexes = cursor.fetchall()
    for index in offline_rebuild_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 被设置为离线重建。")

    cursor.close()