import pyodbc
from sql_metadata import Parser

def audit_indexes(conn, tables):
    issues = []  # 初始化issues为一个空列表
    print("开始进行索引审计...")
    cursor = conn.cursor()

    # 生成动态的表名列表
    formatted_tables = ", ".join([f"'{table}'" for table in tables])

    # 规则 1: 检查未使用的索引
    query1 = f'''
    SELECT 
        obj.name AS TableName,
        idx.name AS IndexName,
        idx.type_desc AS IndexType
    FROM 
        sys.indexes idx
    JOIN 
        sys.objects obj ON idx.object_id = obj.object_id
    LEFT JOIN 
        sys.dm_db_index_usage_stats stats ON idx.object_id = stats.object_id AND idx.index_id = stats.index_id
    WHERE 
        obj.name IN ({formatted_tables})
        AND stats.index_id IS NULL
        AND idx.type_desc <> 'CLUSTERED'
        AND idx.is_primary_key = 0
        AND idx.is_unique = 0
        AND idx.is_unique_constraint = 0;
    '''
    cursor.execute(query1)
    unused_indexes = cursor.fetchall()
    for index in unused_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} ({index[2]}) 未被使用。")

    # 规则 2: 检查可能丢失的索引 #考虑删除权限一直有问题。
        query2 = f"""
        SELECT
            dm_mid.database_id,
            dm_mid.OBJECT_ID,
            dm_migs.avg_total_user_cost * (dm_migs.avg_user_impact / 100.0) AS improvement_measure,
            'CREATE INDEX missing_index_' + CONVERT (VARCHAR, dm_mid.index_handle) + '_' + CONVERT (VARCHAR, dm_mid.OBJECT_ID) + ' ON ' + dm_mid.statement + ' (' + ISNULL(dm_mid.equality_columns,'') + CASE WHEN dm_mid.equality_columns IS NOT NULL AND dm_mid.inequality_columns IS NOT NULL THEN ',' ELSE '' END + ISNULL(dm_mid.inequality_columns, '') + ')' + ISNULL(' INCLUDE (' + dm_mid.included_columns + ')', '') AS create_index_statement,
            dm_migs.user_seeks,
            dm_migs.user_scans
        FROM sys.dm_db_missing_index_groups dm_mig
        INNER JOIN sys.dm_db_missing_index_group_stats dm_migs ON dm_mig.index_group_handle = dm_migs.group_handle
        INNER JOIN sys.dm_db_missing_index_details dm_mid ON dm_mig.index_handle = dm_mid.index_handle
        WHERE dm_mid.database_id = DB_ID()
        ORDER BY improvement_measure DESC;
        """
        try:
            cursor.execute(query2)
            missing_indexes = cursor.fetchall()
            for index in missing_indexes:
                if len(index) < 5:
                    print("警告: 查询返回的丢失索引记录不完整。")
                    continue
                print(f"提示: 考虑在表 {index[3].split(' ON ')[1].split(' (')[0]} 上创建索引 {index[3]} 来提高性能。")
        except pyodbc.ProgrammingError:
            print("警告: 检查丢失的索引失败，可能由于权限问题。")

    # 规则 3: 检查表没有主键
    query3 = f'''
    SELECT name AS TableName
    FROM sys.tables
    WHERE OBJECTPROPERTY(object_id,'TableHasPrimaryKey') = 0
    AND name IN ({formatted_tables});
    '''
    cursor.execute(query3)
    tables_without_pk = cursor.fetchall()
    for table in tables_without_pk:
        print(f"警告: 表 {table[0]} 没有主键。")

    # 规则 4: 检查表没有聚集索引
    query4 = f'''
    SELECT name AS TableName
    FROM sys.tables
    WHERE OBJECTPROPERTY(object_id, 'TableHasClustIndex') = 0
    AND name IN ({formatted_tables});
    '''
    cursor.execute(query4)
    tables_without_clustindex = cursor.fetchall()
    for table in tables_without_clustindex:
        print(f"警告: 表 {table[0]} 没有聚集索引。")

    # 规则 5: 检查索引的碎片程度
    query5 = f"""
         SELECT OBJECT_NAME(ips.object_id) AS TableName,
                i.name AS IndexName,
                ips.avg_fragmentation_in_percent
         FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, NULL) ips
         JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
         WHERE ips.avg_fragmentation_in_percent > 30
         AND OBJECT_NAME(ips.object_id) IN ({formatted_tables});
    """
    try:
        cursor.execute(query5)
        fragmented_indexes = cursor.fetchall()
        for index in fragmented_indexes:
            if len(index) < 3:
                print("警告: 查询返回的索引记录不完整。")
                continue
            print(f"警告: 表 {index[0]} 上的索引 {index[1]} 存在 {index[2]:.2f}% 的碎片化。")
    except pyodbc.Error as e:
        if "列名" in str(e) or "无效" in str(e) or "语法" in str(e):
            print(f"SQL错误：{e}")
        else:
            print(f"数据库错误：{e}")

    # 规则 6: 检查具有宽列的索引
    query6 = f"""
        SELECT OBJECT_NAME(ic.object_id) AS TableName,
               i.name AS IndexName,
               SUM(c.max_length) AS TotalIndexWidth
        FROM sys.index_columns ic
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE OBJECT_NAME(ic.object_id) IN ({formatted_tables})
        GROUP BY ic.object_id, i.name
        HAVING SUM(c.max_length) > 900;
    """
    cursor.execute(query6)
    wide_indexes = cursor.fetchall()
    for index in wide_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 列宽度过大。")

    # 规则 7: 检查索引中是否缺少统计信息
    query7 = f"""
        SELECT OBJECT_NAME(s.object_id) AS TableName,
               s.name AS StatName
        FROM sys.stats s
        WHERE s.auto_created = 0 AND NOT EXISTS (
            SELECT 1
            FROM sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
            WHERE sp.last_updated IS NOT NULL
        )
        AND OBJECT_NAME(s.object_id) IN ({formatted_tables});
    """
    cursor.execute(query7)
    missing_statistics = cursor.fetchall()
    for stat in missing_statistics:
        print(f"警告: 表 {stat[0]} 上的统计信息 {stat[1]} 缺失。")

    # 规则 8: 检查过多的非聚集索引
    query8 = f'''
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               COUNT(*) AS NonClusteredIndexCount
        FROM sys.indexes i WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED'
        GROUP BY i.object_id
        HAVING COUNT(*) > 10;
    '''
    cursor.execute(query8)
    tables_with_many_indexes = cursor.fetchall()
    for table in tables_with_many_indexes:
        print(f"警告: 表 {table[0]} 存在过多的非聚集索引。")

    # 规则 9: 检查禁用的索引
    query9 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName
        FROM sys.indexes i WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.is_disabled = 1;
    """
    cursor.execute(query9)
    disabled_indexes = cursor.fetchall()
    for index in disabled_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 已被禁用。")

    # 规则 10: 检查索引创建或修改的日期
    query10 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               STATS_DATE(i.object_id, i.index_id) AS LastModifiedDate
        FROM sys.indexes i WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND DATEDIFF(DAY, STATS_DATE(i.object_id, i.index_id), GETDATE()) > 365;
    """
    cursor.execute(query10)
    old_indexes = cursor.fetchall()
    for index in old_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 自上次修改或创建已经超过一年。")

    # 规则 11: 检查重建索引的需求
    query11 = f"""
        SELECT OBJECT_NAME(ips.object_id) AS TableName,
               i.name AS IndexName,
               ips.avg_fragmentation_in_percent
        FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'DETAILED') ips
        JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
        WHERE ips.avg_fragmentation_in_percent > 70
        AND OBJECT_NAME(ips.object_id) IN ({formatted_tables});
    """
    cursor.execute(query11)
    indexes_needing_rebuild = cursor.fetchall()
    for index in indexes_needing_rebuild:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 存在高碎片化，建议重建。")

    # 规则 12: 检查过期的统计信息
    query12 = f'''
        SELECT OBJECT_NAME(s.object_id) AS TableName,
               s.name AS StatName
        FROM sys.stats s WHERE OBJECT_NAME(s.object_id) IN ({formatted_tables})
        AND DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30;
    '''
    cursor.execute(query12)
    outdated_statistics = cursor.fetchall()
    for stat in outdated_statistics:
        print(f"警告: 表 {stat[0]} 上的统计信息 {stat[1]} 已过期，建议更新。")

    # 规则 13: 检查没有使用的索引
    query13 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName
        FROM sys.indexes i 
        LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id AND us.database_id = DB_ID()
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED' AND (us.user_seeks IS NULL AND us.user_scans IS NULL AND us.user_lookups IS NULL);
    """
    cursor.execute(query13)
    unused_indexes = cursor.fetchall()
    for index in unused_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 从未被使用。")

    # 规则 14: 检查表上的统计信息的最后更新时间
    query14 = f"""
        SELECT OBJECT_NAME(s.object_id) AS TableName,
               s.name AS StatName,
               STATS_DATE(s.object_id, s.stats_id) AS LastUpdated
        FROM sys.stats s
        WHERE OBJECT_NAME(s.object_id) IN ({formatted_tables}) AND DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30;  -- 30天为阈值
    """

    cursor.execute(query14)
    outdated_stats = cursor.fetchall()
    for stat in outdated_stats:
        print(f"警告: 表 {stat[0]} 的统计信息 {stat[1]} 自上次更新已超过30天。")

    # 规则 15: 检查超过5MB的索引大小
    query15 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               SUM(p.used_page_count * 8) AS IndexSizeKB
        FROM sys.indexes i 
        JOIN sys.dm_db_partition_stats p ON i.object_id = p.object_id AND i.index_id = p.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY i.object_id, i.name
        HAVING SUM(p.used_page_count * 8) > 5120;  -- 5MB的大小
    """
    cursor.execute(query15)
    large_indexes = cursor.fetchall()
    for index in large_indexes:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 的大小超过5MB。")

    # 规则 16: 检查索引的填充因子
    query16 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               i.fill_factor
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND (i.fill_factor < 70 OR i.fill_factor > 90);
    """
    cursor.execute(query16)
    fill_factor_issues = cursor.fetchall()
    for index in fill_factor_issues:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 的填充因子为 {index[2]}%。建议将其设置在70-90%之间。")

    # 规则 17: 检查索引的行锁定
    query17 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.allow_row_locks = 0;
    """
    cursor.execute(query17)
    no_row_locks = cursor.fetchall()
    for index in no_row_locks:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 禁止行锁定。")

    # 规则 18: 检查索引的页锁定
    query18 = f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.allow_page_locks = 0;
    """
    cursor.execute(query18)
    no_page_locks = cursor.fetchall()
    for index in no_page_locks:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 禁止页锁定。")

    # 规则 19: 检查索引的列数
    query19 = f"""
    SELECT OBJECT_NAME(ic.object_id) AS TableName,
           i.name AS IndexName,
           COUNT(ic.column_id) AS ColumnCount
    FROM sys.index_columns ic
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    WHERE OBJECT_NAME(ic.object_id) IN ({formatted_tables})
    GROUP BY ic.object_id, i.name
    HAVING COUNT(ic.column_id) > 5;
    """

    cursor.execute(query19)
    indexes_with_many_columns = cursor.fetchall()
    for index in indexes_with_many_columns:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 包含超过5个列。")

    # 规则 20: 检查有大量INSERT操作的表的索引使用情况
    query20 = f"""
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName, SUM(ps.[used_page_count]) * 8 IndexSizeKB 
    FROM sys.dm_db_partition_stats ps 
    INNER JOIN sys.indexes AS ix ON ps.object_id = ix.object_id 
    WHERE ix.type_desc = 'NONCLUSTERED' AND ps.index_id = ix.index_id AND OBJECT_NAME(ix.object_id) IN ({formatted_tables})
    GROUP BY OBJECT_NAME(ix.object_id), ix.name 
    HAVING SUM(ps.[used_page_count]) * 8 < 50
    """

    cursor.execute(query20)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result[0]} 上的索引 {result[1]} 由于大量的INSERT操作可能未被频繁使用。")

    # 规则 21: 检查未使用的索引
    query21 = f"""
    SELECT o.name AS TableName, i.name AS IndexName 
    FROM sys.indexes AS i 
    JOIN sys.objects AS o ON i.object_id = o.object_id 
    LEFT JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id 
    WHERE s.user_seeks + s.user_scans + s.user_lookups = 0 
    AND o.type = 'U' 
    AND o.name IN ({formatted_tables});
    """
    cursor.execute(query21)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 似乎从未被使用过，考虑删除它以减少维护开销。")

    # 规则 22: 检查过大的非聚集索引
    query22 = f"""
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName, SUM(ps.[used_page_count]) * 8 IndexSizeKB 
    FROM sys.dm_db_partition_stats ps 
    INNER JOIN sys.indexes AS ix ON ps.object_id = ix.object_id 
    WHERE ix.type_desc = 'NONCLUSTERED' 
    AND OBJECT_NAME(ix.object_id) IN ({formatted_tables})
    GROUP BY OBJECT_NAME(ix.object_id), ix.name 
    HAVING SUM(ps.[used_page_count]) * 8 > 5000;
    """
    cursor.execute(query22)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 大小超过了5MB，考虑优化或删除它以减少存储和I/O开销。")

    # 规则 23: 检查索引的碎片化情况
    query23 = f"""
    SELECT OBJECT_NAME(ps.object_id) AS TableName, ix.name AS IndexName, avg_fragmentation_in_percent 
    FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'LIMITED') ps 
    JOIN sys.indexes AS ix ON ps.object_id = ix.object_id AND ps.index_id = ix.index_id 
    WHERE avg_fragmentation_in_percent > 30 
    AND OBJECT_NAME(ps.object_id) IN ({formatted_tables});
    """
    cursor.execute(query23)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的碎片化超过了30%，考虑重新组织或重建这个索引。")

    # 规则 24: 检查是否有过多的重复索引
    query24 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ic.column_id, c.name AS column_name 
    FROM sys.indexes i 
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
    JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id 
    WHERE EXISTS (
        SELECT 1 
        FROM sys.indexes i2 
        JOIN sys.index_columns ic2 ON i2.object_id = ic2.object_id AND i2.index_id = ic2.index_id 
        JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id 
        WHERE i.object_id = i2.object_id AND i.index_id <> i2.index_id AND ic.column_id = ic2.column_id AND c.name = c2.name
        AND OBJECT_NAME(i2.object_id) IN ({formatted_tables})
    )
    AND OBJECT_NAME(i.object_id) IN ({formatted_tables});
    """

    cursor.execute(query24)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 存在重复的索引 {result.IndexName}，请考虑删除或合并重复的索引。")

    # 规则 25: 重复跟16待补充

    # 规则 26: 检查索引的页面密度
    query26 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_page_space_used_in_percent 
    FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ps.avg_page_space_used_in_percent < 80
    """  # 页面密度低于80%可能是一个问题
    cursor.execute(query26)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的页面密度低于80%，可能导致存储浪费。")

    # 规则 27: 检查禁用的索引
    query27 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
    FROM sys.indexes i 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.is_disabled = 1
    """
    cursor.execute(query27)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 存在被禁用的索引 {result.IndexName}，请考虑启用或删除该索引。")

    # 规则 28: 检查只在索引中有的列，而在表中没有的列
    query28 = f"""
    SELECT OBJECT_NAME(ic.object_id) AS TableName, c1.name 
    FROM sys.index_columns ic 
    JOIN sys.columns c1 ON c1.object_id = ic.object_id AND c1.column_id = ic.column_id 
    LEFT JOIN sys.columns c2 ON ic.object_id = c2.object_id AND ic.column_id = c2.column_id 
    WHERE OBJECT_NAME(ic.object_id) IN ({formatted_tables}) AND c2.name IS NULL
    """
    cursor.execute(query28)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 在表 {result.TableName} 的索引中存在列 {result.name}，但在表中没有此列。")

    # 规则 29: 检查未使用的索引
    query29 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
    FROM sys.indexes i 
    LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
    AND (us.user_seeks = 0 AND us.user_scans = 0 AND us.user_lookups = 0)
    """
    cursor.execute(query29)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 似乎从未被使用过。考虑删除此索引以节省存储和维护成本。")

    # 规则 30: 检查索引的深度
    query30 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.index_depth 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'DETAILED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ps.index_depth > 3
    """
    cursor.execute(query30)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的深度为 {result.index_depth}，可能导致查询性能下降。")

    # 规则 31: 检查是否有可能不支持在线操作的索引
    query31 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
    FROM sys.indexes i 
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
    AND EXISTS (
        SELECT 1 
        FROM sys.columns c 
        WHERE c.object_id = t.object_id AND c.system_type_id IN (34, 35, 36, 99, 165, 167, 231, 239, 241)
    )
    """
    cursor.execute(query31)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 可能不支持在线操作，因为它涉及到可能不支持在线操作的数据类型。")

    # 规则 32: 检查索引的页数
    query32 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.page_count 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE ps.page_count < 1000 AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """
    cursor.execute(query32)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 只有 {result.page_count} 页，可能不是最佳索引选择。")

    # 规则 33: 检查索引的读写比率
    query33 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, us.user_updates, us.user_seeks + us.user_scans + us.user_lookups AS Reads 
    FROM sys.dm_db_index_usage_stats us 
    JOIN sys.indexes i ON i.object_id = us.object_id AND i.index_id = us.index_id 
    WHERE us.user_updates > (us.user_seeks + us.user_scans + us.user_lookups) * 10 AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """
    cursor.execute(query33)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的读写比率可能不是最佳选择，因为其被更新的次数远远超过读取的次数。")

    # 规则 34: 检查过多的非聚集索引
    query34 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, COUNT(*) AS NonClusteredIndexCount 
    FROM sys.indexes i 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED' 
    GROUP BY OBJECT_NAME(i.object_id) 
    HAVING COUNT(*) > 5
    """
    cursor.execute(query34)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上有 {result.NonClusteredIndexCount} 个非聚集索引，可能导致写操作的性能下降。")

    # 规则 35: 检查是否有过多的包含列
    query35 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS IncludedColumnCount 
    FROM sys.indexes i 
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ic.is_included_column = 1 
    GROUP BY OBJECT_NAME(i.object_id), i.name 
    HAVING COUNT(*) > 5
    """
    cursor.execute(query35)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 包含 {result.IncludedColumnCount} 个列，可能导致存储和I/O开销增加。")

    # 规则 36: 检查非聚集索引的键列数
    query36 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS KeyColumnCount 
    FROM sys.indexes i 
    JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED' AND ic.is_included_column = 0 
    GROUP BY OBJECT_NAME(i.object_id), i.name 
    HAVING COUNT(*) > 5
    """
    cursor.execute(query36)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的非聚集索引 {result.IndexName} 的键列数为 {result.KeyColumnCount}，可能导致查询性能下降。")

    # 规则 37: 检查索引的空间利用率
    query37 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_page_space_used_in_percent 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ps.avg_page_space_used_in_percent < 70
    """  # 假设索引的空间利用率低于70%可能不是最佳选择

    cursor.execute(query37)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的空间利用率只有 {result.avg_page_space_used_in_percent}%，可能导致存储空间浪费。")

    # 规则 38: 检查索引的大小
    query38 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(ps.page_count) AS PageCount 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
    GROUP BY i.object_id, i.name 
    HAVING SUM(ps.page_count) < 128
    """  # 假设索引大小小于128页可能不是最佳选择

    cursor.execute(query38)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 只有 {result.PageCount} 页，可能不是最佳索引选择。")

    # 规则 39: 检查索引的填充因子
    query39 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_fragmentation_in_percent 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ps.avg_fragmentation_in_percent > 30
    """  # 假设索引的填充因子超过30%可能导致性能下降

    cursor.execute(query39)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的填充因子为 {result.avg_fragmentation_in_percent}%，可能导致查询性能下降。")

    # 规则 40: 检查索引的总体大小
    query40 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.page_count * 8 / 1024 AS IndexSizeMB 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps 
    JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ps.page_count * 8 / 1024 > 1000
    """  # 假设索引的总体大小超过1GB可能导致存储和I/O开销增加

    cursor.execute(query40)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的大小为 {result.IndexSizeMB} MB，可能导致存储和I/O开销增加。")

    # 规则 41: 检查未使用的索引
    query41 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName
    FROM sys.dm_db_index_usage_stats u
    JOIN sys.indexes i ON i.object_id = u.object_id AND i.index_id = u.index_id
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND u.user_seeks = 0 AND u.user_scans = 0 AND u.user_lookups = 0
    """

    cursor.execute(query41)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 从未被使用过，考虑删除此索引以减少维护成本。")

    # 规则 42: 检查禁用的索引
    query42 = f"""
    SELECT OBJECT_NAME(object_id) AS TableName, name AS IndexName 
    FROM sys.indexes 
    WHERE OBJECT_NAME(object_id) IN ({formatted_tables}) AND is_disabled = 1
    """
    cursor.execute(query42)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 已被禁用，考虑启用或删除此索引。")

    # 规则 43: 检查存在多个非聚集索引的表
    query43 = f"""
    SELECT OBJECT_NAME(object_id) AS TableName, COUNT(*) AS NonClusteredIndexCount 
    FROM sys.indexes 
    WHERE OBJECT_NAME(object_id) IN ({formatted_tables}) AND type_desc = 'NONCLUSTERED' 
    GROUP BY object_id HAVING COUNT(*) > 5
    """
    cursor.execute(query43)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上存在 {result.NonClusteredIndexCount} 个非聚集索引，可能导致写操作性能下降。")

    # 规则 44: 检查索引的深度
    query44 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.index_depth 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s 
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND s.index_depth > 4
    """
    cursor.execute(query44)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的深度为 {result.index_depth}，可能导致读操作性能下降。")

    # 规则 45: 检查索引是否有过多的前导列
    query45 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS LeadingColumnsCount 
    FROM sys.index_columns c 
    JOIN sys.indexes i ON i.object_id = c.object_id AND i.index_id = c.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND key_ordinal > 0 AND key_ordinal < 4
    GROUP BY i.object_id, i.name 
    HAVING COUNT(*) > 3
    """
    cursor.execute(query45)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.LeadingColumnsCount} 个前导列，可能导致查询性能下降。")

    # 规则 46: 检查过大的索引键大小
    query46 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.avg_record_size_in_bytes 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s 
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND s.avg_record_size_in_bytes > 900
    """
    cursor.execute(query46)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的平均记录大小为 {result.avg_record_size_in_bytes} 字节，可能导致查询性能下降。")

    # 规则 47: 检查含有大量重复值的索引
    query47 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, 
           (s.page_count * 8 * 1024 / NULLIF(s.avg_record_size_in_bytes, 0)) - s.record_count AS EstimatedDuplicateKeyCount 
    FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s 
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
    WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
    AND (s.page_count * 8 * 1024 / NULLIF(s.avg_record_size_in_bytes, 0)) - s.record_count > 1000
    """
    cursor.execute(query47)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 估计有 {result.EstimatedDuplicateKeyCount} 个重复键，可能导致查询性能下降。")

    # 规则 48: 检查过期的索引维护计划
    query48 = f"""
    SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.last_system_update 
    FROM sys.dm_db_index_usage_stats s 
    JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
    WHERE DATEDIFF(DAY, s.last_system_update, GETDATE()) > 180
    AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """
    cursor.execute(query48)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 已有超过180天未进行系统维护，请检查索引维护计划。")

    # 规则 49: 检查导致大量页分裂的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, 
               s.leaf_insert_count + s.leaf_delete_count + s.leaf_update_count AS TotalOperations
        FROM sys.dm_db_index_operational_stats(NULL, NULL, NULL, NULL) s 
        JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
        WHERE s.leaf_insert_count + s.leaf_delete_count + s.leaf_update_count > 1000  -- 根据您的实际情况选择适当的阈值
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 可能导致大量页分裂，因为它有 {result.TotalOperations} 次的叶操作，可能需要优化填充因子或考虑重新组织索引。")

    # 规则 50: 检查导致锁争用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, 
               s.row_lock_wait_count + s.page_lock_wait_count AS TotalLockWaitCount
        FROM sys.dm_db_index_operational_stats(NULL, NULL, NULL, NULL) s 
        JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
        WHERE s.row_lock_wait_count + s.page_lock_wait_count > 10
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 导致锁争用，可能需要调整事务或考虑优化索引设计。")

    # 规则 51: 检查过长的索引名称
    cursor.execute(f"""
        SELECT OBJECT_NAME(object_id) AS TableName, name AS IndexName 
        FROM sys.indexes 
        WHERE LEN(name) > 50
        AND OBJECT_NAME(object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引名称 {result.IndexName} 过长，可能导致管理困难。")

    # 规则 52: 检查使用了不建议的数据类型的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, col.name AS ColumnName, t.name AS DataType 
        FROM sys.index_columns ic 
        JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id 
        JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
        JOIN sys.types t ON t.user_type_id = col.user_type_id 
        WHERE t.name IN ('text', 'ntext', 'image')
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 使用了不建议的数据类型 {result.DataType}，可能影响性能。")

    # 规则 53: 检查是否存在非唯一的聚集索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE i.type_desc = 'CLUSTERED' AND i.is_unique = 0 
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的聚集索引 {result.IndexName} 是非唯一的，可能影响查询性能。")

    # 规则 54: 检查使用GUID作为主键的表的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id 
        WHERE c.system_type_id = 36 AND i.is_primary_key = 1 
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 使用GUID作为主键的索引 {result.IndexName}，可能导致性能问题。")

    # 规则 55: 检查没有任何读取操作的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.dm_db_index_usage_stats s 
        JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
        WHERE s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0 
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 没有任何读取操作，可能是多余的。")

    # 规则 56: 检查索引的列是否有大量重复的值
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName 
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id 
        WHERE c.system_type_id IN (SELECT user_type_id FROM sys.types WHERE is_replicated = 1)
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的列 {result.ColumnName} 有大量重复值，可能不适合作为索引。")

    # 规则 57: 检查有大量DELETE操作的表的索引碎片情况
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.dm_db_index_usage_stats s 
        JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id 
        WHERE s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups) * 10
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 更新次数远多于读取次数，可能需要重新评估。")

    # 规则 58: 检查索引的总体大小
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND i.type_desc = 'NONCLUSTERED' 
        AND INDEXPROPERTY(i.object_id, i.name, 'IndexKeySize') > 900
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 的键大小过大，超过了900字节的限制。")

    # 规则 59: 检查索引是否在文件组上有适当的放置
    # 为了提供最佳性能，索引通常应该放在与其相关表相同的文件组上。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, f.name AS FileGroupName 
        FROM sys.indexes i 
        JOIN sys.filegroups f ON f.data_space_id = i.data_space_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND f.name != 'PRIMARY'
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 被放置在文件组 {result.FileGroupName} 而不是在PRIMARY文件组上。")

    # 规则 60: 检查是否存在没有作为键的列的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND NOT EXISTS (
            SELECT 1 FROM sys.index_columns ic 
            WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        )
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 没有作为键的列，可能导致不必要的写操作开销。")

    # 规则 61: 检查是否存在禁用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        AND i.is_disabled = 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在已被禁用的索引 {result.IndexName}，考虑是否删除。")

    # 规则 62: 检查是否存在重复的索引
    cursor.execute(f"""
        WITH IndexColumns AS (
            SELECT 
                i.object_id,
                i.index_id,
                i.name AS IndexName,
                STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.index_column_id) AS ColumnNames
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
            GROUP BY i.object_id, i.index_id, i.name
        )
        SELECT OBJECT_NAME(ic1.object_id) AS TableName, ic1.IndexName
        FROM IndexColumns ic1
        JOIN IndexColumns ic2 ON ic1.object_id = ic2.object_id AND ic1.index_id <> ic2.index_id
        WHERE ic1.ColumnNames = ic2.ColumnNames
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在重复的索引 {result.IndexName}，考虑合并或删除其中的某些索引。")

    # 规则 63: 检查过大的索引键列
    # SQL Server 对索引键列有900字节的限制
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, 
               SUM(CASE WHEN t.name = 'nvarchar' THEN c.max_length / 2 ELSE c.max_length END) AS TotalKeySize
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        JOIN sys.types t ON t.system_type_id = c.system_type_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING SUM(CASE WHEN t.name = 'nvarchar' THEN c.max_length / 2 ELSE c.max_length END) > 900
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的键大小超过900字节的限制。")

    # 规则 64: 检查包含大量NULL值的列的索引
    # 索引中的NULL值可能不提供查询性能优势。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName 
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id 
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id 
        WHERE c.is_nullable = 1
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含可以包含NULL值的列 {result.ColumnName}，可能不提供查询性能优势。")

    # 规则 65: 检查是否存在仅覆盖一列的非聚集索引
    # 在某些情况下，可以考虑将它们合并到其他索引中。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.type_desc = 'NONCLUSTERED' AND i.is_unique = 0
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(*) = 1
    """)

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上存在仅覆盖一列的非聚集索引 {result.IndexName}，考虑是否合并到其他索引。")

    # # 规则 66: 检查是否存在未使用的索引
    # 未使用的索引不仅占用存储空间，而且会增加写操作的开销。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName
        FROM sys.indexes i
        LEFT JOIN sys.dm_db_index_usage_stats s ON s.object_id = i.object_id AND s.index_id = i.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND (s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0 OR s.object_id IS NULL)
    """)

    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在未使用的索引 {result.IndexName}，考虑删除。")

    # 规则 67: 检查非聚集索引的深度
    # 非聚集索引的深度过深可能影响查询性能。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.index_depth
        FROM sys.indexes i 
        JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s 
        ON s.object_id = i.object_id AND s.index_id = i.index_id
        WHERE i.type_desc = 'NONCLUSTERED' 
        AND s.index_depth > 3
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 的深度为 {result.index_depth}，可能影响查询性能。")

    # 规则 68: 检查非聚集索引的页数
    # 如果非聚集索引的页数过少，可能不值得保留。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count
        FROM sys.indexes i 
        JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s 
        ON s.object_id = i.object_id AND s.index_id = i.index_id
        WHERE i.type_desc = 'NONCLUSTERED' 
        AND s.page_count < 10
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 仅有 {result.page_count} 页，考虑是否删除。")

    # 规则 69: 检查索引的平均碎片化情况
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.avg_fragmentation_in_percent
        FROM sys.indexes i
        JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s 
        ON s.object_id = i.object_id AND s.index_id = i.index_id
        WHERE s.avg_fragmentation_in_percent > 30
        AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的碎片化率为 {result.avg_fragmentation_in_percent}%，考虑重新组织或重建。")

    # 规则 70: 检查索引的填充因子
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, i.fill_factor
        FROM sys.indexes i
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND i.fill_factor NOT BETWEEN 70 AND 90
    """)

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的填充因子为 {result.fill_factor}%，可能需要调整。")

    # # 规则 71: 检查索引的列数
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 5
    """)
    results = cursor.fetchall()

    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.ColumnCount} 列，考虑简化索引结构。")

    # 规则 72: 检查索引的页分配情况
    # 描述: 索引的页数可以提供有关索引大小的信息。一个只有很少页的索引可能不会为查询提供很大的帮助，
    # 因为它可能不会包含足够的数据来帮助SQL Server更快地定位行。但是，这样的索引仍然会导致写操作的开销。
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count 
        FROM sys.indexes i 
        JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s 
        ON s.object_id = i.object_id AND s.index_id = i.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND s.page_count < 10
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 仅占用了 {result.page_count} 页，考虑是否真的需要此索引。")

    # 规则 73: 检查是否有过多的非聚集索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, COUNT(i.index_id) AS NonClusteredIndexCount 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED' 
        GROUP BY OBJECT_NAME(i.object_id) 
        HAVING COUNT(i.index_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上有 {result.NonClusteredIndexCount} 个非聚集索引，可能过多。")

    # 规则 74: 检查过小的索引页数
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count 
        FROM sys.indexes i 
        JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s 
        ON s.object_id = i.object_id AND s.index_id = i.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND s.page_count < 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 仅有 {result.page_count} 页，考虑是否删除。")

    # 规则 75: 检查过大的索引数据大小
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(p.used_page_count * 8) AS IndexSizeKB 
        FROM sys.indexes i 
        JOIN sys.allocation_units a ON a.container_id = i.index_id 
        JOIN sys.dm_db_partition_stats p ON p.object_id = i.object_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND a.type = 2  -- IN_ROW_DATA
        GROUP BY OBJECT_NAME(i.object_id), i.name 
        HAVING SUM(p.used_page_count * 8) > 50000  -- 假设索引大于50MB可能被认为是大索引
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 大小为 {result.IndexSizeKB} KB，可能过大。")

    # 规则 76: 检查重复的索引
    cursor.execute(f"""
        SELECT ind1.name AS Index1, ind2.name AS Index2, OBJECT_NAME(ind1.object_id) AS TableName 
        FROM sys.indexes ind1
        JOIN sys.indexes ind2 ON ind1.object_id = ind2.object_id 
        WHERE ind1.index_id < ind2.index_id 
        AND OBJECT_NAME(ind1.object_id) IN ({formatted_tables})
        AND EXISTS (
            SELECT 1 
            FROM sys.index_columns ic1 
            JOIN sys.columns c1 ON c1.object_id = ic1.object_id AND c1.column_id = ic1.column_id
            JOIN sys.index_columns ic2 ON ic1.column_id = ic2.column_id 
            WHERE ic1.index_id = ind1.index_id AND ic2.index_id = ind2.index_id 
            GROUP BY ic1.index_id, ic2.index_id 
            HAVING COUNT(ic1.column_id) = COUNT(ic2.column_id)
        )
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 在表 {result.TableName} 上，索引 {result.Index1} 和 {result.Index2} 可能是重复的。")

    # 规则 77: 检查包含大文本列的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName 
        FROM sys.index_columns ic 
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id 
        JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
        WHERE c.max_length > 4000 AND OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含大文本列 {result.ColumnName}，可能导致性能下降。")

    # 规则 78: 检查未使用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        JOIN sys.dm_db_index_usage_stats s ON s.object_id = i.object_id AND s.index_id = i.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 似乎从未被使用过，考虑是否删除。")

    # 规则 79: 检查独立的非聚集索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        AND i.type_desc = 'NONCLUSTERED' AND i.is_unique = 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的独立非聚集索引 {result.IndexName} 可能不需要设置为唯一。")

    # 规则 80: 检查是否有不必要的列包含在索引中
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               c.name AS ColumnName
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ic.key_ordinal = 0
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含不必要的列 {result.ColumnName}。")

    # 规则 81: 检查非聚集索引是否有过多的列
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 有过多的列，列数为 {result.ColumnCount}。")

    # 规则 82: 检查重复的索引名
    cursor.execute(f"""
        SELECT i.name AS IndexName, COUNT(*) AS Count 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY i.name 
        HAVING COUNT(*) > 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引名称 {result.IndexName} 在不同的表上重复使用了 {result.Count} 次。")

    # 规则 83: 检查是否有超出建议长度的索引名称
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, LEN(i.name) AS Length 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND LEN(i.name) > 50
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 名称长度为 {result.Length}，超过建议的长度。")

    # 规则 84: 检查是否有使用保留关键字的索引名称
    # 假设我们有一个保留关键字列表
    reserved_keywords = ['INDEX', 'TABLE', 'COLUMN']

    # 根据表的范围查询索引名称
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()

    # 检查结果并打印警告
    for result in results:
        if result.IndexName and result.IndexName.upper() in reserved_keywords:
            print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 使用了保留关键字。")

    # 规则 85: 检查是否有未使用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        AND (s.user_seeks = 0 OR s.user_seeks IS NULL)
        AND (s.user_scans = 0 OR s.user_scans IS NULL)
        AND (s.user_lookups = 0 OR s.user_lookups IS NULL)
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 未被使用，考虑删除它。")

    # 规则 86: 检查是否有不支持ONLINE操作的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        AND (i.type_desc = 'XML' OR i.type_desc = 'SPATIAL')
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 不支持ONLINE操作。")

    # 规则 87: 检查禁用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        AND i.is_disabled = 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 被禁用，考虑重新启用或删除它。")

    # 规则 88: 检查索引平均碎片化率
    cursor.execute(f"""
        SELECT OBJECT_NAME(a.object_id) AS TableName, a.index_id, avg_fragmentation_in_percent 
        FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, NULL) a 
        WHERE OBJECT_NAME(a.object_id) IN ({formatted_tables})
        AND avg_fragmentation_in_percent > 30
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引ID {result.index_id} 的碎片化率为 {result.avg_fragmentation_in_percent}%，考虑进行重建或重组。")

    # 规则 89: 检查是否有过多的分区
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(p.partition_number) AS PartitionCount
        FROM sys.indexes i
        JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(p.partition_number) > 10
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.PartitionCount} 个分区，可能导致查询性能下降。")

    # 规则 90: 检查是否有超过建议大小的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, 
               i.name AS IndexName, 
               SUM(a.used_pages) * 8 / 1024 AS IndexSizeMB 
        FROM sys.indexes i
        JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id 
        JOIN sys.allocation_units a ON p.partition_id = a.container_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables})
        GROUP BY OBJECT_NAME(i.object_id), i.name 
        HAVING SUM(a.used_pages) * 8 / 1024 > 1000  -- 1GB的建议阈值
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 大小为 {result.IndexSizeMB}MB，超过建议的大小。")

    # 规则 91: 检查非聚集索引的键列数是否过多
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 非聚集索引 {result.IndexName} 在表 {result.TableName} 上的键列数为 {result.ColumnCount}，超过建议的数量。")

    # 规则 92: 检查是否有重复的索引名称
    cursor.execute(f"""
        SELECT i.name AS IndexName, COUNT(*) AS Duplicates 
        FROM sys.indexes i 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        GROUP BY i.name 
        HAVING COUNT(*) > 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引名称 {result.IndexName} 重复 {result.Duplicates} 次。请确保索引名称的唯一性。")

    # 规则 93: 检查是否有不经常使用的索引
    cursor.execute(
        "SELECT OBJECT_NAME(ius.object_id) AS TableName, i.name AS IndexName, ius.user_seeks + ius.user_scans AS AccessCount "
        "FROM sys.dm_db_index_usage_stats ius "
        "JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id "
        "WHERE ius.user_seeks + ius.user_scans < 10")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上的访问次数只有 {result.AccessCount} 次，考虑是否需要该索引。")

    # 规则 94: 检查是否有与主键重复的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(ic.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) 
        AND i.is_primary_key = 0 
        AND EXISTS (
            SELECT 1 
            FROM sys.indexes pi 
            JOIN sys.index_columns pic ON pi.object_id = pic.object_id AND pi.index_id = pic.index_id 
            WHERE pi.is_primary_key = 1 
            AND i.object_id = pi.object_id 
            AND ic.column_id = pic.column_id 
            AND ic.key_ordinal = pic.key_ordinal
        )
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 与主键重复，可能是冗余的。")

    # 规则 95: 检查是否有未绑定的外键
    cursor.execute(f"""
        SELECT OBJECT_NAME(fk.parent_object_id) AS TableName, fk.name AS ForeignKeyName 
        FROM sys.foreign_keys fk 
        LEFT JOIN sys.indexes i ON fk.referenced_object_id = i.object_id AND fk.key_index_id = i.index_id 
        WHERE i.index_id IS NULL 
        AND OBJECT_NAME(fk.parent_object_id) IN ({formatted_tables})
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的外键 {result.ForeignKeyName} 未绑定到任何索引。")

    # 规则 96: 检查是否有重复的索引结构
    cursor.execute(f"""
        SELECT i1.name AS IndexName1, i2.name AS IndexName2, OBJECT_NAME(i1.object_id) AS TableName 
        FROM sys.indexes i1 
        JOIN sys.indexes i2 ON i1.object_id = i2.object_id 
        WHERE i1.index_id < i2.index_id 
        AND OBJECT_NAME(i1.object_id) IN ({formatted_tables})
        AND EXISTS (
            SELECT 1 
            FROM sys.index_columns ic1
            JOIN sys.index_columns ic2 ON ic1.object_id = ic2.object_id AND ic1.column_id = ic2.column_id 
            WHERE ic1.index_id = i1.index_id AND ic2.index_id = i2.index_id
        )
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 在表 {result.TableName} 上，索引 {result.IndexName1} 和 {result.IndexName2} 的结构相似，可能存在冗余。")

    # 规则 97: 检查是否有非簇集索引的键列数过多
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 7
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 非簇集索引 {result.IndexName} 在表 {result.TableName} 上的键列数为 {result.ColumnCount}，超出建议的数量。")

    # 规则 98: 检查索引是否只包含了部分列，而没有包括所有需要的列
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND ic.is_included_column = 1
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上可能只包含了部分列，而没有包括所有需要的列。")

    # 规则 99: 检查是否有大量的未使用的索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(ius.object_id) AS TableName, i.name AS IndexName 
        FROM sys.dm_db_index_usage_stats ius 
        JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id 
        WHERE OBJECT_NAME(ius.object_id) IN ({formatted_tables})
        AND ius.user_seeks + ius.user_scans + ius.user_lookups = 0 
        AND ius.system_seeks + ius.system_scans + ius.system_lookups = 0
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上未被使用，考虑删除它以减少存储和维护开销。")

    # 规则 100: 检查是否有冷数据被索引
    cursor.execute(f"""
        SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName 
        FROM sys.indexes i 
        JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id 
        WHERE OBJECT_NAME(i.object_id) IN ({formatted_tables}) AND p.rows = 0
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上只索引了冷数据，考虑重新评估该索引的需求。")

    print("索引审计完成。")
    cursor.close()
    return issues if issues is not None else []




