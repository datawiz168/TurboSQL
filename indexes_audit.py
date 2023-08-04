import pyodbc

def audit_indexes(conn):
    issues = []  # 初始化issues为一个空列表
    cursor = conn.cursor()


    # 查询找出未使用的索引
    query1 = """
    SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName
    FROM sys.indexes ix
    LEFT JOIN sys.dm_db_index_usage_stats stats
    ON ix.object_id = stats.object_id
    AND ix.index_id = stats.index_id
    WHERE stats.user_seeks + stats.user_scans + stats.user_lookups = 0
    AND ix.type_desc = 'NONCLUSTERED'
    AND ix.is_primary_key = 0
    AND ix.is_unique = 0
    AND ix.is_unique_constraint = 0
    """

    # 执行查询并捕获任何异常
    try:
        cursor.execute(query1)
        unused_indexes = cursor.fetchall()
        for index in unused_indexes:
            if len(index) < 2:
                print("警告: 查询返回的索引记录不完整。")
                continue
            print(f"警告: 表 {index[0]} 上的索引 {index[1]} 未被使用，考虑删除。")
    except pyodbc.ProgrammingError:
        print("警告: 检查未使用的索引失败，可能由于权限问题。")




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
        if len(index) >= 2:  # 确保查询返回的每一行都有至少两个值
            print(f"警告: 表 {index[0]} 上的索引 {index[1]} 未被使用，考虑删除。")
        else:
            print(f"查询返回的数据格式不正确: {index}")

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

    # # Rule 6: 检查具有宽列的索引
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
           STATS_DATE(s.object_id, s.stats_id) AS LastUpdated
    FROM sys.stats s
    WHERE DATEDIFF(day, STATS_DATE(s.object_id, s.stats_id), GETDATE()) > 30;  -- 30天为阈值
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
    HAVING SUM(p.used_page_count * 8) > 5120;  -- 5MB的大小
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
    FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
    GROUP BY ic.object_id, i.name
    HAVING COUNT(ic.column_id) > 5;
    """
    cursor.execute(query19)
    indexes_with_many_columns = cursor.fetchall()
    for index in indexes_with_many_columns:
        print(f"警告: 表 {index[0]} 上的索引 {index[1]} 包含超过5个列。")

    # 规则 20: 检查有大量INSERT操作的表的索引使用情况
    # 频繁的INSERT操作可能导致某些索引不被频繁使用，造成不必要的开销。
    cursor.execute(
        "SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName, SUM(ps.[used_page_count]) * 8 IndexSizeKB "
        "FROM sys.dm_db_partition_stats ps "
        "INNER JOIN sys.indexes AS ix ON ps.object_id = ix.object_id "
        "WHERE ix.type_desc = 'NONCLUSTERED' AND ps.index_id = ix.index_id AND OBJECT_NAME(ix.object_id) = 'your_table_name_here' "
        "GROUP BY OBJECT_NAME(ix.object_id), ix.name "
        "HAVING SUM(ps.[used_page_count]) * 8 < 50")  # 用实际的表名替换 'your_table_name_here'
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 由于大量的INSERT操作可能未被频繁使用。")

    # 规则 21: 检查未使用的索引
    cursor.execute("SELECT o.name AS TableName, i.name AS IndexName "
                   "FROM sys.indexes AS i "
                   "JOIN sys.objects AS o ON i.object_id = o.object_id "
                   "LEFT JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE s.user_seeks + s.user_scans + s.user_lookups = 0 AND o.type = 'U'")  # U代表用户表
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 似乎从未被使用过，考虑删除它以减少维护开销。")

    # 规则 22: 检查过大的非聚集索引
    cursor.execute(
        "SELECT OBJECT_NAME(ix.object_id) AS TableName, ix.name AS IndexName, SUM(ps.[used_page_count]) * 8 IndexSizeKB "
        "FROM sys.dm_db_partition_stats ps "
        "INNER JOIN sys.indexes AS ix ON ps.object_id = ix.object_id "
        "WHERE ix.type_desc = 'NONCLUSTERED' "
        "GROUP BY OBJECT_NAME(ix.object_id), ix.name "
        "HAVING SUM(ps.[used_page_count]) * 8 > 5000")  # 认为大于5MB的索引为大索引
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 大小超过了5MB，考虑优化或删除它以减少存储和I/O开销。")

    # 规则 23: 检查索引的碎片化情况
    cursor.execute("SELECT OBJECT_NAME(ps.object_id) AS TableName, ix.name AS IndexName, avg_fragmentation_in_percent "
                   "FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'LIMITED') ps "
                   "JOIN sys.indexes AS ix ON ps.object_id = ix.object_id AND ps.index_id = ix.index_id "
                   "WHERE avg_fragmentation_in_percent > 30")  # 认为碎片化超过30%的索引
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的碎片化超过了30%，考虑重新组织或重建这个索引。")

    # Rule 24: 检查是否有过多的重复索引
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ic.column_id, c.name AS column_name "
        "FROM sys.indexes i "
        "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
        "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
        "WHERE EXISTS ("
        "   SELECT 1 "
        "   FROM sys.indexes i2 "
        "   JOIN sys.index_columns ic2 ON i2.object_id = ic2.object_id AND i2.index_id = ic2.index_id "
        "   JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id "
        "   WHERE i.object_id = i2.object_id AND i.index_id <> i2.index_id AND ic.column_id = ic2.column_id AND c.name = c2.name"
        ")"
    )
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 存在重复的索引 {result.IndexName}，请考虑删除或合并重复的索引。")

    # Rule 25: 检查索引的填充因子
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, i.fill_factor "
                   "FROM sys.indexes i "
                   "WHERE i.fill_factor NOT BETWEEN 70 AND 90")  # 填充因子建议在70-90之间
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的填充因子为 {result.fill_factor}，可能不是最佳设置。")

    # Rule 26: 检查索引的页面密度
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_page_space_used_in_percent "
        "FROM sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') ps "
        "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
        "WHERE ps.avg_page_space_used_in_percent < 80")  # 页面密度低于80%可能是一个问题
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的页面密度低于80%，可能导致存储浪费。")

    # Rule 27: 检查禁用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.is_disabled = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 存在被禁用的索引 {result.IndexName}，请考虑启用或删除该索引。")

    # Rule 28: 检查只在索引中有的列，而在表中没有的列
    cursor.execute("SELECT OBJECT_NAME(ic.object_id) AS TableName, c1.name "
                   "FROM sys.index_columns ic JOIN sys.columns c1 ON c1.object_id = ic.object_id AND c1.column_id = ic.column_id "
                   "LEFT JOIN sys.columns c2 ON ic.object_id = c2.object_id AND ic.column_id = c2.column_id "
                   "WHERE c2.name IS NULL")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 在表 {result.TableName} 的索引中存在列 {result.name}，但在表中没有此列。")

    # Rule 29: 检查未使用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id AND i.index_id = us.index_id "
                   "WHERE us.user_seeks = 0 AND us.user_scans = 0 AND us.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 似乎从未被使用过。考虑删除此索引以节省存储和维护成本。")

    # Rule 30: 检查索引的深度
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.index_depth "
                   "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'DETAILED') ps "
                   "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
                   "WHERE ps.index_depth > 3")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的深度为 {result.index_depth}，可能导致查询性能下降。")

    # # Rule 31: 检查索引是否为在线操作
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
    #                "FROM sys.indexes i "
    #                "WHERE i.allow_online = 0")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 的索引 {result.IndexName} 不允许在线操作。考虑更改此设置以减少维护期间的停机时间。")

    # Rule 32: 检查索引的页数
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.page_count "
                   "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps "
                   "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
                   "WHERE ps.page_count < 1000")  # 假设少于1000页的索引可能不是最佳选择
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 只有 {result.page_count} 页，可能不是最佳索引选择。")

    # Rule 33: 检查索引的读写比率
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, us.user_updates, us.user_seeks + us.user_scans + us.user_lookups AS Reads "
        "FROM sys.dm_db_index_usage_stats us "
        "JOIN sys.indexes i ON i.object_id = us.object_id AND i.index_id = us.index_id "
        "WHERE us.user_updates > (us.user_seeks + us.user_scans + us.user_lookups) * 10")  # 假设更新比读多10倍的索引可能不是最佳选择
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的读写比率可能不是最佳选择，因为其被更新的次数远远超过读取的次数。")

    # Rule 34: 检查过多的非聚集索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, COUNT(*) AS NonClusteredIndexCount "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'NONCLUSTERED' "
                   "GROUP BY OBJECT_NAME(i.object_id) "
                   "HAVING COUNT(*) > 5")  # 假设一个表上有超过5个非聚集索引可能是个问题
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上有 {result.NonClusteredIndexCount} 个非聚集索引，可能导致写操作的性能下降。")

    # 规则 35: 检查是否有过多的包含列
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS IncludedColumnCount "
                   "FROM sys.indexes i "
                   "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
                   "WHERE ic.is_included_column = 1 "
                   "GROUP BY OBJECT_NAME(i.object_id), i.name "
                   "HAVING COUNT(*) > 5")  # 假设一个索引包含超过5个列可能不是最佳设计
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 包含 {result.IncludedColumnCount} 个列，可能导致存储和I/O开销增加。")

    # 规则 36: 检查非聚集索引的键列数
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS KeyColumnCount "
    #                "FROM sys.indexes i "
    #                "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
    #                "WHERE i.type_desc = 'NONCLUSTERED' AND ic.is_included_column = 0 "
    #                "GROUP BY OBJECT_NAME(i.object_id), i.name "
    #                "HAVING COUNT(*) > 5")  # 假设非聚集索引的键列数超过5可能导致性能下降
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 的非聚集索引 {result.IndexName} 的键列数为 {result.KeyColumnCount}，可能导致查询性能下降。")

    # 规则 37: 检查索引的空间利用率
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_page_space_used_in_percent "
        "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps "
        "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
        "WHERE ps.avg_page_space_used_in_percent < 70")  # 假设索引的空间利用率低于70%可能不是最佳选择
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的空间利用率只有 {result.avg_page_space_used_in_percent}%，可能导致存储空间浪费。")

    # # 规则 38: 检查索引的行数
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.row_count "
    #                "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps "
    #                "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
    #                "WHERE ps.row_count < 1000")  # 假设索引的行数少于1000可能不是最佳选择
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 表 {result.TableName} 的索引 {result.IndexName} 只有 {result.row_count} 行，可能不是最佳索引选择。")

    # 规则 39: 检查索引的填充因子
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.avg_fragmentation_in_percent "
                   "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps "
                   "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
                   "WHERE ps.avg_fragmentation_in_percent > 30")  # 假设索引的填充因子超过30%可能导致性能下降
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的填充因子为 {result.avg_fragmentation_in_percent}%，可能导致查询性能下降。")

    # 规则 40: 检查索引的总体大小
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, ps.page_count * 8 / 1024 AS IndexSizeMB "
        "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ps "
        "JOIN sys.indexes i ON i.object_id = ps.object_id AND i.index_id = ps.index_id "
        "WHERE ps.page_count * 8 / 1024 > 1000")  # 假设索引的总体大小超过1GB可能导致存储和I/O开销增加
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 的索引 {result.IndexName} 的大小为 {result.IndexSizeMB} MB，可能导致存储和I/O开销增加。")

    # 规则 41: 检查未使用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.dm_db_index_usage_stats u "
                   "JOIN sys.indexes i ON i.object_id = u.object_id AND i.index_id = u.index_id "
                   "WHERE u.user_seeks = 0 AND u.user_scans = 0 AND u.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 从未被使用过，考虑删除此索引以减少维护成本。")

    # 规则 42: 检查禁用的索引
    cursor.execute(
        "SELECT OBJECT_NAME(object_id) AS TableName, name AS IndexName FROM sys.indexes WHERE is_disabled = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 已被禁用，考虑启用或删除此索引。")

    # 规则 43: 检查存在多个非聚集索引的表
    cursor.execute("SELECT OBJECT_NAME(object_id) AS TableName, COUNT(*) AS NonClusteredIndexCount "
                   "FROM sys.indexes "
                   "WHERE type_desc = 'NONCLUSTERED' "
                   "GROUP BY object_id HAVING COUNT(*) > 5")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上存在 {result.NonClusteredIndexCount} 个非聚集索引，可能导致写操作性能下降。")

    # # 规则 44: 检查索引的深度
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.depth "
    #                "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s "
    #                "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
    #                "WHERE s.depth > 4")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的深度为 {result.depth}，可能导致读操作性能下降。")

    # 规则 45: 检查索引是否有过多的前导列
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, COUNT(*) AS LeadingColumnsCount "
    #                "FROM sys.index_columns c "
    #                "JOIN sys.indexes i ON i.object_id = c.object_id AND i.index_id = c.index_id "
    #                "WHERE key_ordinal > 0 AND key_ordinal < 4 "  # key_ordinal < 4 表示列在索引中的位置
    #                "GROUP BY i.object_id, i.name "
    #                "HAVING COUNT(*) > 3")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.LeadingColumnsCount} 个前导列，可能导致查询性能下降。")

    # 规则 46: 检查过大的索引键大小
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.avg_record_size_in_bytes "
                   "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s "
                   "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE s.avg_record_size_in_bytes > 900")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的平均记录大小为 {result.avg_record_size_in_bytes} 字节，可能导致查询性能下降。")

    # # 规则 47: 检查含有大量重复值的索引
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.duplicate_key_count "
    #                "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') s "
    #                "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
    #                "WHERE s.duplicate_key_count > 1000")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.duplicate_key_count} 个重复键，可能导致查询性能下降。")

    # 规则 48: 检查过期的索引维护计划
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.last_system_update "
                   "FROM sys.dm_db_index_usage_stats s "
                   "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE DATEDIFF(DAY, s.last_system_update, GETDATE()) > 180")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 已有超过180天未进行系统维护，请检查索引维护计划。")

    # # 规则 49: 检查导致大量页分裂的索引
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_splits_sec "
    #                "FROM sys.dm_db_index_operational_stats(NULL, NULL, NULL, NULL) s "
    #                "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
    #                "WHERE s.page_splits_sec > 10")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 导致大量页分裂，可能需要优化填充因子或考虑重新组织索引。")

    # # 规则 50: 检查导致锁争用的索引
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.lock_wait_count "
    #                "FROM sys.dm_db_index_operational_stats(NULL, NULL, NULL, NULL) s "
    #                "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
    #                "WHERE s.lock_wait_count > 10")
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 导致锁争用，可能需要调整事务或考虑优化索引设计。")

    # 规则 51: 检查过长的索引名称
    cursor.execute("SELECT OBJECT_NAME(object_id) AS TableName, name AS IndexName "
                   "FROM sys.indexes "
                   "WHERE LEN(name) > 50")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引名称 {result.IndexName} 过长，可能导致管理困难。")

    # # 规则 52: 检查使用了不建议的数据类型的索引
    # cursor.execute(
    #     "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName, t.name AS DataType "
    #     "FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #     "JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
    #     "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #     "JOIN sys.types t ON t.user_type_id = c.user_type_id "
    #     "WHERE t.name IN ('text', 'ntext', 'image')")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 使用了不建议的数据类型 {result.DataType}，可能影响性能。")

    # 规则 53: 检查是否存在非唯一的聚集索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'CLUSTERED' AND i.is_unique = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的聚集索引 {result.IndexName} 是非唯一的，可能影响查询性能。")

    # 规则 54: 检查使用GUID作为主键的表的索引
    # GUID作为主键可能会导致碎片化和插入性能问题。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "JOIN sys.columns c ON c.object_id = i.object_id AND c.column_id = i.index_id "
                   "WHERE c.system_type_id = 36 AND i.is_primary_key = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 使用GUID作为主键的索引 {result.IndexName}，可能导致性能问题。")

    # 规则 55: 检查没有任何读取操作的索引
    # 如果索引从未用于查询，但经常更新，可能导致不必要的开销。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.dm_db_index_usage_stats s "
                   "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 没有任何读取操作，可能是多余的。")

    # # 规则 56: 检查索引的列是否有大量重复的值
    # # 高度重复的索引列可能导致索引效率低下。
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName "
    #                "FROM sys.indexes i "
    #                "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
    #                "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #                "WHERE c.system_type_id IN (SELECT type_id FROM sys.types WHERE is_replicated = 1)")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的列 {result.ColumnName} 有大量重复值，可能不适合作为索引。")

    # 规则 57: 检查有大量DELETE操作的表的索引碎片情况
    # 频繁的DELETE操作可能导致索引碎片化。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.dm_db_index_usage_stats s "
                   "JOIN sys.indexes i ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups) * 10")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 更新次数远多于读取次数，可能需要重新评估。")

    # 规则 58: 检查索引的总体大小
    # 过大的索引可能导致存储和I/O开销增加。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'NONCLUSTERED' AND INDEXPROPERTY(i.object_id, i.name, 'IndexKeySize') > 900")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 的键大小过大，超过了900字节的限制。")

    # 规则 59: 检查索引是否在文件组上有适当的放置
    # 为了提供最佳性能，索引通常应该放在与其相关表相同的文件组上。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, f.name AS FileGroupName "
                   "FROM sys.indexes i "
                   "JOIN sys.filegroups f ON f.data_space_id = i.data_space_id "
                   "WHERE f.name != 'PRIMARY'")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 被放置在文件组 {result.FileGroupName} 而不是在PRIMARY文件组上。")

    # 规则 60: 检查是否存在没有WHERE子句的索引
    # 这些索引可能不提供查询性能优势，但会增加写操作的开销。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE NOT EXISTS (SELECT 1 FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0)")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 没有WHERE子句，可能导致不必要的写操作开销。")

    # 规则 61: 检查是否存在禁用的索引
    # 禁用的索引不会被查询使用，但仍占用存储空间。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.is_disabled = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在已被禁用的索引 {result.IndexName}，考虑是否删除。")

    # 规则 62: 检查是否存在重复的索引
    # 两个或更多的索引如果覆盖相同的列，可能导致冗余和不必要的开销。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "GROUP BY OBJECT_NAME(i.object_id), i.name "
                   "HAVING COUNT(*) > 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在重复的索引 {result.IndexName}，考虑合并或删除其中的某些索引。")

    # 规则 63: 检查过大的索引键列
    # SQL Server 对索引键列有900字节的限制。
    # cursor.execute(
    #     "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(c.max_length) AS TotalKeySize "
    #     "FROM sys.indexes i "
    #     "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
    #     "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #     "GROUP BY OBJECT_NAME(i.object_id), i.name "
    #     "HAVING SUM(c.max_length) > 900")
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的键大小超过900字节的限制。")

    # 规则 64: 检查包含大量NULL值的列的索引
    # 索引中的NULL值可能不提供查询性能优势。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName "
                   "FROM sys.indexes i "
                   "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
                   "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
                   "WHERE c.is_nullable = 1")

    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含可以包含NULL值的列 {result.ColumnName}，可能不提供查询性能优势。")

    # 规则 65: 检查是否存在仅覆盖一列的非聚集索引
    # 在某些情况下，可以考虑将它们合并到其他索引中。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'NONCLUSTERED' AND i.is_unique = 0 "
                   "GROUP BY OBJECT_NAME(i.object_id), i.name "
                   "HAVING COUNT(*) = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在仅覆盖一列的非聚集索引 {result.IndexName}，考虑是否合并到其他索引。")

    # 规则 66: 检查是否存在未使用的索引
    # 未使用的索引不仅占用存储空间，而且会增加写操作的开销。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "LEFT JOIN sys.dm_db_index_usage_stats s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上存在未使用的索引 {result.IndexName}，考虑删除。")

    # 规则 67: 检查非聚集索引的深度
    # 非聚集索引的深度过深可能影响查询性能。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.index_depth "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE i.type_desc = 'NONCLUSTERED' AND s.index_depth > 3")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 的深度为 {result.index_depth}，可能影响查询性能。")

    # 规则 68: 检查非聚集索引的页数
    # 如果非聚集索引的页数过少，可能不值得保留。
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE i.type_desc = 'NONCLUSTERED' AND s.page_count < 10")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 仅有 {result.page_count} 页，考虑是否删除。")

    # 规则 69: 检查索引的平均碎片化情况
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.avg_fragmentation_in_percent "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE s.avg_fragmentation_in_percent > 30")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的碎片化率为 {result.avg_fragmentation_in_percent}%，考虑重新组织或重建。")

    # 规则 70: 检查索引的填充因子
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, i.fill_factor "
                   "FROM sys.indexes i "
                   "WHERE i.fill_factor NOT BETWEEN 70 AND 90")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 的填充因子为 {result.fill_factor}%，可能需要调整。")

    # 规则 71: 检查索引的列数
    cursor.execute("""
        SELECT OBJECT_NAME(i.object_id) AS TableName,
               i.name AS IndexName,
               COUNT(c.column_id) AS ColumnCount
        FROM sys.indexes i
        JOIN sys.index_columns c ON c.object_id = i.object_id AND c.index_id = i.index_id
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(c.column_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.ColumnCount} 列，考虑简化索引结构。")

    # 规则 72: 检查索引的页分配情况
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE s.page_count < 10")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 仅占用了 {result.page_count} 页，考虑是否真的需要此索引。")

    # 规则 73: 检查是否有过多的非聚集索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, COUNT(i.index_id) AS NonClusteredIndexCount "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'NONCLUSTERED' "
                   "GROUP BY OBJECT_NAME(i.object_id) "
                   "HAVING COUNT(i.index_id) > 5")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上有 {result.NonClusteredIndexCount} 个非聚集索引，可能过多。")

    # 规则 74: 检查过小的索引页数
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, s.page_count "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_physical_stats (NULL, NULL, NULL, NULL, 'SAMPLED') s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE s.page_count < 5")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 仅有 {result.page_count} 页，考虑是否删除。")

    # 规则 75: 检查过大的索引数据大小
    cursor.execute(
        "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(p.used_page_count * 8) AS IndexSizeKB "
        "FROM sys.indexes i "
        "JOIN sys.allocation_units a ON a.container_id = i.index_id "
        "JOIN sys.dm_db_partition_stats p ON p.object_id = i.object_id "
        "WHERE a.type = 2 "  # IN_ROW_DATA
        "GROUP BY OBJECT_NAME(i.object_id), i.name "
        "HAVING SUM(p.used_page_count * 8) > 50000")  # 假设索引大于50MB可能被认为是大索引
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 大小为 {result.IndexSizeKB} KB，可能过大。")

    # # 规则 76: 检查重复的索引
    # cursor.execute("SELECT ind1.name AS Index1, ind2.name AS Index2, OBJECT_NAME(ind1.object_id) AS TableName "
    #                "FROM sys.indexes ind1 "
    #                "JOIN sys.indexes ind2 ON ind1.object_id = ind2.object_id "
    #                "WHERE ind1.index_id < ind2.index_id "
    #                "AND EXISTS (SELECT 1 FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id1 "
    #                "JOIN sys.index_columns ic2 ON ic1.column_id = ic2.column_id "
    #                "WHERE ic1.index_id = ind1.index_id AND ic2.index_id = ind2.index_id "
    #                "GROUP BY ic1.index_id, ic2.index_id "
    #                "HAVING COUNT(ic1.column_id) = COUNT(ic2.column_id))")
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 在表 {result.TableName} 上，索引 {result.Index1} 和 {result.Index2} 可能是重复的。")

    # 规则 77: 检查包含大文本列的索引
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, c.name AS ColumnName "
    #                "FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #                "JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
    #                "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
    #                "WHERE c.max_length > 4000")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含大文本列 {result.ColumnName}，可能导致性能下降。")

    # 规则 78: 检查未使用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "JOIN sys.dm_db_index_usage_stats s ON s.object_id = i.object_id AND s.index_id = i.index_id "
                   "WHERE s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 似乎从未被使用过，考虑是否删除。")

    # 规则 79: 检查独立的非聚集索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'NONCLUSTERED' AND i.is_unique = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的独立非聚集索引 {result.IndexName} 可能不需要设置为唯一。")

    # 规则 80: 检查是否有不必要的列包含在索引中
    cursor.execute("""
        SELECT OBJECT_NAME(i.object_id) AS TableName, 
               i.name AS IndexName, 
               c.name AS ColumnName
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        WHERE ic.key_ordinal = 0
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 包含不必要的列 {result.ColumnName}。")

    # 规则 81: 检查非聚集索引是否有过多的列
    cursor.execute("""
        SELECT OBJECT_NAME(i.object_id) AS TableName, 
               i.name AS IndexName, 
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i
        JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        WHERE i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的非聚集索引 {result.IndexName} 有过多的列，列数为 {result.ColumnCount}。")

    # 规则 82: 检查重复的索引名
    cursor.execute("SELECT i.name AS IndexName, COUNT(*) AS Count "
                   "FROM sys.indexes i "
                   "GROUP BY i.name "
                   "HAVING COUNT(*) > 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引名称 {result.IndexName} 在不同的表上重复使用了 {result.Count} 次。")

    # 规则 83: 检查是否有超出建议长度的索引名称
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, LEN(i.name) AS Length "
                   "FROM sys.indexes i "
                   "WHERE LEN(i.name) > 50")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 名称长度为 {result.Length}，超过建议的长度。")

    # 规则 84: 检查是否有使用保留关键字的索引名称
    # 假设我们有一个保留关键字列表
    # reserved_keywords = ['INDEX', 'TABLE', 'COLUMN']
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
    #                "FROM sys.indexes i")
    # results = cursor.fetchall()
    # for result in results:
    #     if result.IndexName.upper() in reserved_keywords:
    #         print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 使用了保留关键字。")

    # 规则 85: 检查是否有未使用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id "
                   "WHERE s.user_seeks = 0 AND s.user_scans = 0 AND s.user_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 未被使用，考虑删除它。")

    # 规则 86: 检查是否有不支持ONLINE操作的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.type_desc = 'XML' OR i.type_desc = 'SPATIAL'")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 不支持ONLINE操作。")

    # 规则 87: 检查禁用的索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "WHERE i.is_disabled = 1")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 被禁用，考虑重新启用或删除它。")

    # 规则 88: 检查索引平均碎片化率
    cursor.execute("SELECT OBJECT_NAME(a.object_id) AS TableName, a.index_id, avg_fragmentation_in_percent "
                   "FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, NULL) a "
                   "WHERE avg_fragmentation_in_percent > 30")
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 表 {result.TableName} 上的索引ID {result.index_id} 的碎片化率为 {result.avg_fragmentation_in_percent}%，考虑进行重建或重组。")

    # 规则 89: 检查是否有过多的分区
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, p.partition_number "
    #                "FROM sys.indexes i "
    #                "JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id "
    #                "GROUP BY OBJECT_NAME(i.object_id), i.name "
    #                "HAVING COUNT(p.partition_number) > 10")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 有 {result.partition_number} 个分区，可能导致查询性能下降。")

    # 规则 90: 检查是否有超过建议大小的索引
    # cursor.execute(
    #     "SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName, SUM(p.used_page_count) * 8 / 1024 AS IndexSizeMB "
    #     "FROM sys.indexes i "
    #     "JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id "
    #     "GROUP BY OBJECT_NAME(i.object_id), i.name "
    #     "HAVING SUM(p.used_page_count) * 8 / 1024 > 1000")  # 1GB的建议阈值
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 大小为 {result.IndexSizeMB}MB，超过建议的大小。")

    # 规则 91: 检查非聚集索引的键列数是否过多
    cursor.execute("""
        SELECT OBJECT_NAME(i.object_id) AS TableName, 
               i.name AS IndexName, 
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 5
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 非聚集索引 {result.IndexName} 在表 {result.TableName} 上的键列数为 {result.ColumnCount}，超过建议的数量。")

    # 规则 92: 检查是否有重复的索引名称
    cursor.execute("SELECT i.name AS IndexName, COUNT(*) AS Duplicates "
                   "FROM sys.indexes i "
                   "GROUP BY i.name "
                   "HAVING COUNT(*) > 1")
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
    cursor.execute("SELECT OBJECT_NAME(ic.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
                   "WHERE i.is_primary_key = 0 AND EXISTS ("
                   "  SELECT 1 FROM sys.indexes pi "
                   "  JOIN sys.index_columns pic ON pi.object_id = pic.object_id AND pi.index_id = pic.index_id "
                   "  WHERE pi.is_primary_key = 1 AND i.object_id = pi.object_id AND "
                   "  ic.column_id = pic.column_id AND ic.key_ordinal = pic.key_ordinal)")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的索引 {result.IndexName} 与主键重复，可能是冗余的。")

    # 规则 95: 检查是否有未绑定的外键
    cursor.execute("SELECT OBJECT_NAME(fk.parent_object_id) AS TableName, fk.name AS ForeignKeyName "
                   "FROM sys.foreign_keys fk "
                   "LEFT JOIN sys.indexes i ON fk.referenced_object_id = i.object_id AND fk.key_index_id = i.index_id "
                   "WHERE i.index_id IS NULL")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 表 {result.TableName} 上的外键 {result.ForeignKeyName} 未绑定到任何索引。")

    # 规则 96: 检查是否有重复的索引结构
    # cursor.execute("SELECT i1.name AS IndexName1, i2.name AS IndexName2, OBJECT_NAME(i1.object_id) AS TableName "
    #                "FROM sys.indexes i1 "
    #                "JOIN sys.indexes i2 ON i1.object_id = i2.object_id "
    #                "WHERE i1.index_id < i2.index_id AND "
    #                "EXISTS (SELECT 1 FROM sys.index_columns ic JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id1 "
    #                "JOIN sys.index_columns ic2 ON ic1.object_id = ic2.object_id AND ic1.column_id = ic2.column_id "
    #                "WHERE ic1.index_id = i1.index_id AND ic2.index_id = i2.index_id)")
    # results = cursor.fetchall()
    # for result in results:
    #     print(
    #         f"警告: 在表 {result.TableName} 上，索引 {result.IndexName1} 和 {result.IndexName2} 的结构相似，可能存在冗余。")

    # 规则 97: 检查是否有非簇集索引的键列数过多
    cursor.execute("""
        SELECT OBJECT_NAME(i.object_id) AS TableName, 
               i.name AS IndexName, 
               COUNT(ic.column_id) AS ColumnCount
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE i.type_desc = 'NONCLUSTERED'
        GROUP BY OBJECT_NAME(i.object_id), i.name
        HAVING COUNT(ic.column_id) > 7
    """)
    results = cursor.fetchall()
    for result in results:
        print(
            f"警告: 非簇集索引 {result.IndexName} 在表 {result.TableName} 上的键列数为 {result.ColumnCount}，超出建议的数量。")

    # 规则 98: 检查索引是否只包含了部分列，而没有包括所有需要的列
    # cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
    #                "FROM sys.indexes i "
    #                "WHERE i.is_included_column = 0 AND i.is_key_column = 0")
    # results = cursor.fetchall()
    # for result in results:
    #     print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上可能没有包括所有需要的列。")

    # 规则 99: 检查是否有大量的未使用的索引
    cursor.execute("SELECT OBJECT_NAME(ius.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.dm_db_index_usage_stats ius "
                   "JOIN sys.indexes i ON ius.object_id = i.object_id AND ius.index_id = i.index_id "
                   "WHERE ius.user_seeks + ius.user_scans + ius.user_lookups = 0 AND ius.system_seeks + ius.system_scans + ius.system_lookups = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上未被使用，考虑删除它以减少存储和维护开销。")

    # 规则 100: 检查是否有冷数据被索引
    cursor.execute("SELECT OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName "
                   "FROM sys.indexes i "
                   "JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id "
                   "WHERE p.rows = 0")
    results = cursor.fetchall()
    for result in results:
        print(f"警告: 索引 {result.IndexName} 在表 {result.TableName} 上只索引了冷数据，考虑重新评估该索引的需求。")

    cursor.close()


    return issues if issues is not None else []
