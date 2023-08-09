import pyodbc
import xml.etree.ElementTree as ET
import sqlparse

def get_execution_plan(conn, query):
    """
    获取SQL查询的执行计划。
    """
    # 使用 "SET SHOWPLAN_XML ON" 命令获取XML格式的执行计划
    set_showplan_cmd = "SET SHOWPLAN_XML ON;"
    cursor = conn.cursor()
    cursor.execute(set_showplan_cmd)

    # 将查询的SELECT部分替换为SELECT TOP 0
    modified_query = query.replace("SELECT", "SELECT TOP 0", 1)

    try:
        cursor.execute(modified_query)
    except pyodbc.Error as e:
        print(f"错误: {e}")
        if "列名" in str(e) or "无效" in str(e) or "语法" in str(e):
            print("提示：请检查SQL语句的语法、表结构和列名。")
        else:
            print("提示：可能是权限问题或其他数据库配置问题。")
        return None

    # 获取执行计划
    plan = cursor.fetchone()[0]
    cursor.close()

    # 在获取计划后重置SHOWPLAN_XML为OFF
    set_showplan_off_cmd = "SET SHOWPLAN_XML OFF;"
    cursor = conn.cursor()
    cursor.execute(set_showplan_off_cmd)
    cursor.close()

    return plan

def audit_execution_plan(plan_xml):
    """
    审计获取的执行计划，并返回任何潜在的性能问题。
    """
    try:
        root = ET.fromstring(plan_xml)
    except Exception as e:
        print(f"解析执行计划时出错: {e}")
        return
    ''''
    已校验规则1,4,5,6,103，
    '''
    print("开始进行执行计划审计...")
    # 规则 1: 检查全表扫描 √
    table_scans = root.findall(".//*[@PhysicalOp='Table Scan']")
    if table_scans:
        print("警告: 查询中存在全表扫描，可能影响性能。")

    # 规则 2: 检查缺失的索引
    missing_indexes = root.findall(".//MissingIndex")
    if missing_indexes:
        print("提示: 查询可能受益于添加以下索引:")
        for index in missing_indexes:
            columns = ', '.join([col.get('Name') for col in index.findall(".//ColumnGroup/Column")])
            print(f"表: {index.get('Table')} - 列: {columns}")

    # 规则 3: 检查预估的行数与实际行数的偏差
    rel_ops = root.findall(".//RelOp")
    discrepancies = [ro for ro in rel_ops if float(ro.get('EstimateRows', '0')) != float(ro.get('ActualRows', '0'))]
    if discrepancies:
        print("警告: 预估的行数与实际行数有较大偏差，可能需要更新统计信息。")

    # 规则 4: 检查并行查询 √
    parallel_queries = [element for element in root.iter() if
                        element.tag.endswith('RelOp') and element.get('Parallel') == '1']
    if parallel_queries:
        print("警告: 查询并行执行，可能导致资源争用。")

    # 规则 5: 检查排序操作 √
    sort_ops = root.findall(".//*[@PhysicalOp='Sort']")
    if sort_ops:
        print("警告: 查询中存在排序操作，可能影响性能。")

    # 规则 6: 检查哈希匹配 √
    # 注意: 在 SQL Server 的 XML 执行计划中，哈希匹配可能被表示为 PhysicalOp 属性值为 'Hash Match' 的元素
    hash_matches = root.findall(".//*[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在哈希匹配，可能需要大量内存。")

    # 规则 7: 检查昂贵的操作
    high_cost_ops = root.findall(".//RelOp[@EstimatedTotalSubtreeCost>10]")
    if high_cost_ops:
        print("警告: 查询中存在昂贵的操作，考虑优化查询。")

    # 规则 8: 检查子查询
    subqueries = root.findall(".//RelOp[@Action='Subquery']")
    if subqueries:
        print("警告: 子查询可能不如连接效率。")

    # 规则 9: 检查索引扫描
    index_scans = root.findall(".//*[@PhysicalOp='Index Scan']")
    if index_scans:
        print("警告: 查询中存在索引扫描，可能影响性能。")

    # 规则 10: 检查内存或磁盘溢出
    spills = root.findall(".//SpillToTempDb")
    if spills:
        print("警告: 查询中存在内存或磁盘溢出，可能影响性能。")

    # 规则 11: 检查嵌套循环
    nested_loops = root.findall(".//NestedLoop")
    if nested_loops:
        print("警告: 查询中存在嵌套循环，可能在大数据集上效率低下。")

    # 规则 12: 检查是否使用了RIDGE JOIN
    ridge_joins = root.findall(".//RidgeJoin")
    if ridge_joins:
        print("警告: 查询中使用了RIDGE JOIN，可能影响性能。")

    # 规则 13: 检查非聚集索引扫描
    non_clustered_indexes = [index_scan for index_scan in root.findall(".//IndexScan") if
                             index_scan.get("Clustered") == "false"]
    if non_clustered_indexes:
        print("警告: 存在非聚集索引扫描，这可能会导致性能下降。考虑使用聚集索引或重新评估查询设计。")

    # 规则 14: 检查转换或隐式转换
    conversions = root.findall(".//Convert")
    if conversions:
        print("警告: 查询中存在数据类型转换或隐式转换，可能会阻止索引的使用。")

    # 规则 15: 检查大量的数据移动
    data_movements = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if data_movements:
        print("警告: 查询中存在大量的数据移动，如数据溢出到磁盘。")

    # 规则 16: 检查任何警告
    warnings = root.findall(".//Warnings")
    if warnings:
        print("警告: 查询中存在警告，例如关于过时的统计信息。")

    # 规则 17: 检查连接的顺序
    wrong_order_joins = root.findall(".//RelOp[@LogicalOp='Inner Join'][@PhysicalOp!='Nested Loops']")
    if wrong_order_joins:
        print("警告: 连接的顺序可能不正确，可能影响性能。")

    # 规则 18: 检查并行操作限制
    parallelism_restrictions = root.findall(".//QueryPlan[@NonParallelPlanReason='MaxDOPSetToOne']")
    if parallelism_restrictions:
        print("警告: 查询被限制为单线程执行，可能影响性能。")

    # 规则 19: 检查过多的物理读取
    excessive_physical_reads = root.findall(".//RelOp[PhysicalReads>1000]")
    if excessive_physical_reads:
        print("警告: 过多的物理读取可能意味着缺少索引或统计信息过时。")

    # 规则 20: 检查大量的逻辑读取
    excessive_logical_reads = root.findall(".//RelOp[LogicalReads>1000]")
    if excessive_logical_reads:
        print("警告: 过多的逻辑读取可能影响性能。")

    # 规则 21: 检查使用表变量
    table_vars = root.findall(".//TableValuedFunction")
    if table_vars:
        print("警告: 使用表变量可能导致不准确的统计信息。")

    # 规则 22: 检查使用OPTION (FORCE ORDER)
    force_orders = root.findall(".//Hint[@Type='FORCE ORDER']")
    if force_orders:
        print("警告: 使用OPTION (FORCE ORDER)可能导致非最佳的查询计划。")

    # 规则 23: 检查使用OPTION (OPTIMIZE FOR UNKNOWN)
    optimize_for_unknowns = root.findall(".//Hint[@Type='OPTIMIZE FOR UNKNOWN']")
    if optimize_for_unknowns:
        print("警告: 使用OPTION (OPTIMIZE FOR UNKNOWN)可能导致非最佳的查询计划。")

    # 规则 24: 检查缺少统计信息
    missing_stats = root.findall(".//MissingStatistics")
    if missing_stats:
        print("警告: 执行计划中缺少统计信息。")

    # 规则 25: 检查过早的物化
    early_materializations = root.findall(".//EarlyMaterialization")
    if early_materializations:
        print("警告: 过早的物化可能导致不必要的I/O。")

    # 规则 26: 检查多余的索引扫描
    redundant_index_scans = root.findall(".//IndexScan[@Lookup='true']")
    if redundant_index_scans:
        print("警告: 多余的索引扫描可能影响性能。")

    # 规则 27: 检查没有被使用的索引
    unused_indexes = root.findall(".//UnusedIndex")
    if unused_indexes:
        print("警告: 查询中存在没有被使用的索引。")

    # 规则 28: 检查没有被推送的谓词
    unpushed_predicates = root.findall(".//UnpushedPredicate")
    if unpushed_predicates:
        print("警告: 谓词没有被推送到存储引擎，可能导致额外的I/O。")

    # 规则 29: 检查不支持的物理操作符
    unsupported_physical_ops = root.findall(".//UnsupportedPhysicalOp")
    if unsupported_physical_ops:
        print("警告: 执行计划中使用了不支持的物理操作符。")

    # 规则 30: 检查执行计划中的超时
    timeouts = root.findall(".//Timeout")
    if timeouts:
        print("警告: 执行计划中存在超时。")

    # 规则 31: 检查重复的谓词
    redundant_predicates = root.findall(".//RedundantPredicate")
    if redundant_predicates:
        print("警告: 查询中存在重复的谓词，可能影响性能。")

    # 规则 32: 检查不必要的远程查询
    unnecessary_remote_queries = root.findall(".//RemoteQuery[@Unnecessary='true']")
    if unnecessary_remote_queries:
        print("警告: 执行计划中存在不必要的远程查询。")

    # 规则 33: 检查不必要的列
    unnecessary_columns = root.findall(".//OutputList/ColumnReference[@Unnecessary='true']")
    if unnecessary_columns:
        print("警告: 查询返回了不必要的列，可能影响性能。")

    # 规则 34: 检查低效的数据类型转换
    inefficient_conversions = root.findall(".//Convert[@Implicit='false']")
    if inefficient_conversions:
        print("警告: 查询中存在低效的数据类型转换。")

    # 规则 35: 检查分区切换
    partition_switches = root.findall(".//PartitionSwitch")
    if partition_switches:
        print("警告: 执行计划中存在分区切换，可能导致额外的I/O。")

    # 规则 36: 检查低效的TOP操作符
    inefficient_tops = root.findall(".//Top[@Percent='false'][@WithTies='true']")
    if inefficient_tops:
        print("警告: 查询中使用了低效的TOP操作符。")

    # 规则 37: 检查未使用的表
    unused_tables = [relop for relop in root.findall(".//RelOp") if relop.get("EstimateRows") == "0"]
    if unused_tables:
        print("警告: 查询中存在未使用的表。这可能是查询设计不当的结果。")

    # 规则 38: 检查不必要的DISTINCT
    unnecessary_distincts = root.findall(".//Distinct")
    if unnecessary_distincts:
        print("警告: 查询中使用了不必要的DISTINCT，可能影响性能。")

    # 规则 39: 检查不必要的ORDER BY
    unnecessary_order_bys = root.findall(".//OrderBy")
    if unnecessary_order_bys:
        print("警告: 查询中使用了不必要的ORDER BY，可能影响性能。")

    # 规则 40: 检查低效的数据排序
    inefficient_data_orders = root.findall(".//Sort[@PhysicalOp='Parallelism']")
    if inefficient_data_orders:
        print("警告: 查询中存在低效的数据排序，可能影响性能。")

    # 规则 41: 检查不必要的数据合并
    unnecessary_data_merges = root.findall(".//MergeJoin[@ManyToMany='true']")
    if unnecessary_data_merges:
        print("警告: 查询中存在不必要的数据合并，可能影响性能。")

    # 规则 42: 检查不必要的数据串联
    unnecessary_data_concats = root.findall(".//Concatenation[@Unordered='true']")
    if unnecessary_data_concats:
        print("警告: 查询中存在不必要的数据串联，可能影响性能。")

    # 规则 43: 检查不必要的数据分割
    unnecessary_data_splits = root.findall(".//Split")
    if unnecessary_data_splits:
        print("警告: 查询中存在不必要的数据分割，可能影响性能。")

    # 规则 44: 检查低效的数据压缩
    inefficient_data_compressions = root.findall(".//ComputeScalar[@Define='Compression']")
    if inefficient_data_compressions:
        print("警告: 查询中存在低效的数据压缩，可能影响性能。")

    # 规则 45: 检查低效的数据解压缩
    inefficient_data_decompressions = root.findall(".//ComputeScalar[@Define='Decompression']")
    if inefficient_data_decompressions:
        print("警告: 查询中存在低效的数据解压缩，可能影响性能。")

    # 规则 46: 检查数据溢出
    data_overflows = root.findall(".//RelOp[@EstimateRebinds>0]")
    if data_overflows:
        print("警告: 执行计划中存在数据溢出，可能导致额外的I/O。")

    # 规则 47: 检查使用窗口函数
    window_functions = root.findall(".//WindowFunction")
    if window_functions:
        print("警告: 查询中使用了窗口函数，可能影响性能。")

    # 规则 48: 检查昂贵的子查询操作
    subquery_ops = root.findall(".//Subquery")
    if subquery_ops:
        print("警告: 查询中存在昂贵的子查询操作，可能影响性能。")

    # 规则 49: 检查过多的嵌套查询
    nested_queries = root.findall(".//NestedLoop")
    if nested_queries:
        print("警告: 查询中存在过多的嵌套查询，可能影响性能。")

    # 规则 50: 检查昂贵的递归CTE操作
    recursive_cte_ops = root.findall(".//RecursiveCTE")
    if recursive_cte_ops:
        print("警告: 查询中存在昂贵的递归CTE操作，可能影响性能。")

    # 规则 51: 检查昂贵的全文搜索操作
    fulltext_search_ops = root.findall(".//FullTextSearch")
    if fulltext_search_ops:
        print("警告: 查询中存在昂贵的全文搜索操作，可能影响性能。")

    # 规则 52: 检查表变量没有统计信息
    table_variable_stats = root.findall(".//TableVariableWithoutStats")
    if table_variable_stats:
        print("警告: 表变量没有统计信息，可能影响查询优化器的决策。")

    # 规则 53: 检查分区视图
    partitioned_views = root.findall(".//PartitionedView")
    if partitioned_views:
        print("警告: 分区视图可能导致性能问题。")

    # 规则 54: 检查不必要的自连接
    self_joins = root.findall(".//Join[@SelfJoin='true']")
    if self_joins:
        print("警告: 查询中存在不必要的自连接，可能影响性能。")

    # 规则 55: 检查索引与数据的物理分离
    index_data_disparities = root.findall(".//IndexScan[@PhysicalOp='Remote']")
    if index_data_disparities:
        print("警告: 索引与其数据在物理上是分开的，可能导致额外的I/O。")

    # 规则 56: 检查低效的外连接
    inefficient_outer_joins = root.findall(".//OuterJoin[@PhysicalOp='Hash']")
    if inefficient_outer_joins:
        print("警告: 使用哈希操作的外连接可能不如其他类型的连接效率。")

    # 规则 57: 检查未解决的查询提示
    unresolved_query_hints = root.findall(".//UnresolvedHint")
    if unresolved_query_hints:
        print("警告: 查询中存在未解决的查询提示。")

    # 规则 58: 检查使用了过时的查询提示
    deprecated_query_hints = root.findall(".//DeprecatedHint")
    if deprecated_query_hints:
        print("警告: 查询中使用了过时的查询提示。")

    # 规则 59: 检查昂贵的动态SQL操作
    dynamic_sql_ops = root.findall(".//DynamicSQL")
    if dynamic_sql_ops:
        print("警告: 查询中存在昂贵的动态SQL操作，可能影响性能。")

    # 规则 60: 检查昂贵的递归查询
    recursive_queries = root.findall(".//RecursiveQuery")
    if recursive_queries:
        print("警告: 查询中存在昂贵的递归查询，可能影响性能。")

    # 规则 61: 检查未使用的表别名
    unused_table_aliases = root.findall(".//UnusedAlias")
    if unused_table_aliases:
        print("警告: 查询中存在未使用的表别名，可能导致查询难以理解。")

    # 规则 62: 检查昂贵的列存储扫描
    columnstore_scans = root.findall(".//ColumnstoreScan")
    if columnstore_scans:
        print("警告: 查询中存在昂贵的列存储扫描，可能影响性能。")

    # 规则 63: 检查昂贵的列存储索引操作
    columnstore_index_ops = root.findall(".//ColumnstoreIndex")
    if columnstore_index_ops:
        print("警告: 查询中存在昂贵的列存储索引操作，可能影响性能。")

    # 规则 64: 检查使用了大量的内存的操作
    high_memory_ops = root.findall(".//RelOp[@Memory>5000]")  # Arbitrary threshold
    if high_memory_ops:
        print("警告: 查询中存在使用了大量内存的操作，可能影响性能。")

    # 规则 65: 检查使用了大量的CPU的操作
    high_cpu_ops = root.findall(".//RelOp[@CPU>1000]")  # Arbitrary threshold
    if high_cpu_ops:
        print("警告: 查询中存在使用了大量CPU的操作，可能影响性能。")

    # 规则 66: 检查低效的数据聚合
    inefficient_aggregations = root.findall(".//Aggregate[@Strategy='Hash']")
    if inefficient_aggregations:
        print("警告: 查询中存在低效的数据聚合，可能影响性能。")

    # 规则 67: 检查在大数据集上的嵌套循环
    large_data_set_loops = root.findall(".//NestedLoop[@LargeDataSet='true']")
    if large_data_set_loops:
        print("警告: 查询在大数据集上使用嵌套循环，可能影响性能。")

    # 规则 68: 检查不必要的数据复制
    data_copies = root.findall(".//Copy")
    if data_copies:
        print("警告: 查询中存在不必要的数据复制，可能影响性能。")

    # 规则 69: 检查昂贵的数据插入
    expensive_inserts = root.findall(".//Insert")
    if expensive_inserts:
        print("警告: 查询中存在昂贵的数据插入操作，可能影响性能。")

    # 规则 70: 检查昂贵的数据更新
    expensive_updates = root.findall(".//Update")
    if expensive_updates:
        print("警告: 查询中存在昂贵的数据更新操作，可能影响性能。")

    # 规则 71: 检查昂贵的数据删除
    expensive_deletes = root.findall(".//Delete")
    if expensive_deletes:
        print("警告: 查询中存在昂贵的数据删除操作，可能影响性能。")

    # 规则 72: 检查昂贵的数据合并
    expensive_merges = root.findall(".//Merge")
    if expensive_merges:
        print("警告: 查询中存在昂贵的数据合并操作，可能影响性能。")

    # 规则 73: 检查不必要的数据转换
    unnecessary_conversions = root.findall(".//Convert")
    if unnecessary_conversions:
        print("警告: 查询中存在不必要的数据转换，可能影响性能。")

    # 规则 74: 检查数据转换的错误
    conversion_errors = root.findall(".//Convert[@Error='true']")
    if conversion_errors:
        print("警告: 查询中的数据转换存在错误。")

    # 规则 75: 检查不必要的数据连接
    unnecessary_data_links = root.findall(".//DataLink")
    if unnecessary_data_links:
        print("警告: 查询中存在不必要的数据连接，可能影响性能。")

    # 规则 76: 检查不必要的数据流
    unnecessary_data_streams = root.findall(".//Stream")
    if unnecessary_data_streams:
        print("警告: 查询中存在不必要的数据流，可能影响性能。")

    # 规则 77: 检查数据流的错误
    stream_errors = root.findall(".//Stream[@Error='true']")
    if stream_errors:
        print("警告: 查询中的数据流存在错误。")

    # 规则 78: 检查昂贵的数据分区操作
    expensive_partitions = root.findall(".//Partition")
    if expensive_partitions:
        print("警告: 查询中存在昂贵的数据分区操作，可能影响性能。")

    # 规则 79: 检查昂贵的数据压缩操作
    expensive_compressions = root.findall(".//Compression")
    if expensive_compressions:
        print("警告: 查询中存在昂贵的数据压缩操作，可能影响性能。")

    # 规则 80: 检查昂贵的数据解压缩操作
    expensive_decompressions = root.findall(".//Decompression")
    if expensive_decompressions:
        print("警告: 查询中存在昂贵的数据解压缩操作，可能影响性能。")

    # 规则 81: 检查昂贵的数据分发操作
    expensive_distributions = root.findall(".//Distribution")
    if expensive_distributions:
        print("警告: 查询中存在昂贵的数据分发操作，可能影响性能。")

    # 规则 82: 检查不平衡的数据分发
    unbalanced_distributions = root.findall(".//Distribution[@Balance='false']")
    if unbalanced_distributions:
        print("警告: 数据分发不平衡，可能导致资源浪费。")

    # # 规则 83: 检查查询的I/O成本
    # # 注意: 这需要具体的阈值和数据库的实际I/O成本数据，此处只是一个示例
    # io_cost = float(root.find(".//QueryPlan").get('TotalSubtreeCost', '0'))
    # if io_cost > 50:  # 假设阈值为50，实际值需要根据具体情况确定
    #     print("警告: 查询的I/O成本异常高，考虑优化查询或相关配置。")
    # 规则 83: 检查查询的I/O成本
    # 注意: 这需要具体的阈值和数据库的实际I/O成本数据，此处只是一个示例
    query_plan_element = root.find(".//QueryPlan")
    if query_plan_element is not None:
        io_cost = float(query_plan_element.get('TotalSubtreeCost', '0'))
        if io_cost > 50:  # 假设阈值为50，实际值需要根据具体情况确定
            print("警告: 查询的I/O成本异常高，考虑优化查询或相关配置。")

    # 规则 84: 检查不必要的数据分隔
    unnecessary_data_separations = root.findall(".//Separation")
    if unnecessary_data_separations:
        print("警告: 查询中存在不必要的数据分隔，可能影响性能。")

    # 规则 85: 检查昂贵的数据分隔操作
    expensive_separations = root.findall(".//Separation")
    if expensive_separations:
        print("警告: 查询中存在昂贵的数据分隔操作，可能影响性能。")

    # 规则 86: 检查数据阻塞
    data_blockages = root.findall(".//Blockage")
    if data_blockages:
        print("警告: 数据阻塞可能导致性能问题。")

    # 规则 87: 检查数据锁
    data_locks = root.findall(".//Lock")
    if data_locks:
        print("警告: 数据锁可能导致性能问题。")

    # 规则 88: 检查数据死锁
    data_deadlocks = root.findall(".//Deadlock")
    if data_deadlocks:
        print("警告: 数据死锁可能导致查询失败。")

    # 规则 89: 检查低效的数据缓存
    inefficient_data_caching = root.findall(".//Cache[@Efficiency='low']")
    if inefficient_data_caching:
        print("警告: 低效的数据缓存可能导致性能问题。")

    # 规则 90: 检查不必要的数据重复
    unnecessary_data_replications = root.findall(".//Replication")
    if unnecessary_data_replications:
        print("警告: 查询中存在不必要的数据重复，可能影响性能。")

    # 规则 91: 检查昂贵的数据重复操作
    expensive_replications = root.findall(".//Replication")
    if expensive_replications:
        print("警告: 查询中存在昂贵的数据重复操作，可能影响性能。")

    # 规则 92: 检查数据负载不平衡
    data_load_imbalances = root.findall(".//Load[@Balance='false']")
    if data_load_imbalances:
        print("警告: 数据负载不平衡，可能导致资源浪费。")

    # 规则 93: 检查数据溢出
    data_spills = root.findall(".//Spill")
    if data_spills:
        print("警告: 数据溢出可能导致性能问题。")

    # 规则 94: 检查数据泄漏
    data_leaks = root.findall(".//Leak")
    if data_leaks:
        print("警告: 数据泄漏可能导致安全问题。")

    # 规则 95: 检查数据冲突
    data_conflicts = root.findall(".//Conflict")
    if data_conflicts:
        print("警告: 数据冲突可能导致查询失败。")

    # 规则 96: 检查使用非sargable操作
    non_sargable_ops = root.findall(".//NonSargable")
    if non_sargable_ops:
        print("警告: 查询中存在非sargable操作，可能影响性能。")

    # 规则 97: 检查数据分布不均
    data_distribution_imbalances = root.findall(".//Distribution[@Even='false']")
    if data_distribution_imbalances:
        print("警告: 数据分布不均，可能导致资源浪费。")

    # 规则 98: 检查数据碎片
    data_fragments = root.findall(".//Fragmentation")
    if data_fragments:
        print("警告: 数据碎片可能导致性能问题。")

    # 规则 99: 检查数据冗余
    data_redundancies = root.findall(".//Redundancy")
    if data_redundancies:
        print("警告: 数据冗余可能导致资源浪费。")

    # 规则 100: 检查大量的UNION操作
    excessive_unions = root.findall(".//Union")
    if excessive_unions:
        print("警告: 查询中存在大量的UNION操作，可能影响性能。")

    # 规则 101: 检查表扫描操作
    table_scans = root.findall(".//TableScan")
    if table_scans:
        print("警告: 查询中存在表扫描操作，可能影响性能。考虑添加适当的索引。")

    # 规则 102: 检查索引扫描
    index_scans = root.findall(".//IndexScan")
    if index_scans:
        print("警告: 查询中存在索引扫描，而不是索引查找。考虑修改查询或优化索引。")

    # 规则 103: 检查高代价操作 √
    high_cost_ops = [op for op in root.findall(".//*") if float(op.get('EstimatedTotalSubtreeCost', 0)) > 10.0]
    if high_cost_ops:
        print("警告: 查询中存在高代价的操作，可能影响性能。")

    # 规则 104: 检查RID查找
    rid_lookups = root.findall(".//RIDLookup")
    if rid_lookups:
        print("警告: 查询中存在RID查找，这可能是因为缺少覆盖索引。考虑优化索引。")

    # 规则 105: 检查大型哈希聚合
    large_hash_aggregates = root.findall(".//HashMatch[@AggregateType='Hash']")
    if large_hash_aggregates:
        print("警告: 查询中存在大型哈希聚合操作，可能导致大量的内存使用。")

    # 规则 106: 检查并行操作中的数据流不均衡
    parallel_imbalance = root.findall(".//Parallelism[@Reason='DataFlow']")
    if parallel_imbalance:
        print("警告: 查询中的并行操作存在数据流不均衡，可能导致性能下降。")

    # 规则 107: 检查计算密集型操作
    compute_intensive = [op for op in root.findall(".//*") if float(op.get('ComputeScalar', 0)) > 0.2]
    if compute_intensive:
        print("警告: 查询中存在计算密集型操作，可能影响性能。")

    # 规则 108: 检查数据移动操作
    data_movement = root.findall(".//Spool")
    if data_movement:
        print("警告: 查询中存在数据移动操作，可能影响性能。")

    # 规则 109: 检查外部表操作
    external_table_ops = root.findall(".//RemoteQuery")
    if external_table_ops:
        print("警告: 查询中存在外部表操作，可能影响性能。")

    # 规则 110: 检查是否存在"Hash Match"
    hash_match_operations = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_match_operations:
        print("警告: 查询中存在'Hash Match'操作，这可能表示没有找到合适的索引。考虑优化查询或添加合适的索引。")

    # 规则 111: 检查哈希连接
    hash_joins = root.findall(".//HashMatch[@JoinType='Inner']")
    if hash_joins:
        print("警告: 查询中存在哈希连接，可能需要大量内存。考虑优化连接策略。")

    # 规则 112: 检查"Missing Index"提示
    missing_index_warnings = root.findall(".//MissingIndex")
    if missing_index_warnings:
        print("警告: 执行计划中有'Missing Index'提示，考虑添加建议的索引来提高性能。")

    # 规则 113: 检查键查找
    key_lookups = root.findall(".//KeyLookup")
    if key_lookups:
        print("警告: 查询中存在键查找操作，可能导致性能问题。考虑使用覆盖索引。")

    # 规则 114: 检查递归操作
    recursive_ops = root.findall(".//Recursive")
    if recursive_ops:
        print("警告: 查询中存在递归操作，可能影响性能。")

    # 规则 115: 检查并行操作是否因为资源争夺被阻塞
    parallel_blocked = [op for op in root.findall(".//Parallelism") if op.get('Blocked', 0) == '1']
    if parallel_blocked:
        print("警告: 查询中的并行操作被阻塞，可能是因为资源争夺。")

    # 规则 116: 检查转换操作
    convert_ops = root.findall(".//Convert")
    if convert_ops:
        print("警告: 查询中存在数据类型转换操作，可能影响性能。确保查询中使用的数据类型匹配表结构的数据类型。")

    # 规则 117: 检查流水线操作
    stream_aggregate = root.findall(".//StreamAggregate")
    if stream_aggregate:
        print("警告: 查询中存在流水线聚合操作，可能需要优化。")

    # 规则 118: 检查内存溢出到磁盘的操作
    spill_to_tempdb = [op for op in root.findall(".//*") if float(op.get('EstimatedSpillLevel', 0)) > 0]
    if spill_to_tempdb:
        print("警告: 查询中的某些操作可能导致内存溢出到tempdb，可能影响性能。")

    # 规则 119: 检查未使用的统计信息
    unused_stats = root.findall(".//StatisticsNotUsed")
    if unused_stats:
        print("警告: 查询中存在未使用的统计信息，考虑更新或删除不必要的统计信息。")

    # 规则 120: 检查非最优的位图操作
    bitmap_ops = root.findall(".//Bitmap")
    if bitmap_ops:
        print("警告: 查询中存在位图操作，可能影响性能。考虑优化相关的连接或筛选条件。")

    # 规则 121: 检查高成本的远程查询
    remote_query = root.findall(".//RemoteQuery")
    if remote_query:
        print("警告: 查询中存在高成本的远程查询操作，可能影响性能。考虑优化远程查询或将数据本地化。")

    # 规则 122: 检查表变量操作
    table_var_ops = root.findall(".//TableValuedFunction")
    if table_var_ops:
        print("警告: 查询中存在表变量操作，可能导致性能下降。考虑使用临时表替代表变量。")

    # 规则 123: 检查大量的RID查找（堆查找）
    rid_lookups = root.findall(".//RIDLookup")
    if rid_lookups:
        print("警告: 查询中存在大量的RID查找操作，这是堆查找，可能导致性能问题。考虑使用聚集索引。")

    # 规则 124: 检查Left Outer Join操作，可能意味着查询中有左外连接
    left_outer_join_operations = root.findall(".//RelOp[@PhysicalOp='Left Outer Join']")
    if left_outer_join_operations:
        print("警告: 查询中存在Left Outer Join操作，考虑是否可以优化连接策略。")

    # 规则 125: 检查序列投影操作
    sequence_project = root.findall(".//SequenceProject")
    if sequence_project:
        print("警告: 查询中存在序列投影操作，可能影响性能。")

    # 规则 126: 检查窗口函数操作
    window_aggregates = root.findall(".//WindowAggregate")
    if window_aggregates:
        print("警告: 查询中存在窗口函数操作，可能影响性能。考虑优化窗口函数或相关查询。")

    # 规则 127: 检查顶部操作
    top_ops = root.findall(".//Top")
    if top_ops:
        print("警告: 查询中存在TOP操作，可能导致性能下降。确保只检索所需的记录数。")

    # 规则 128: 检查嵌套循环连接中的异步操作
    async_nested_loops = root.findall(".//NestedLoops[@IsAsync='True']")
    if async_nested_loops:
        print("警告: 查询中的嵌套循环连接存在异步操作，可能导致性能问题。考虑优化连接策略。")

    # 规则 129: 检查分配给查询的过多内存
    high_memory_grants = [op for op in root.findall(".//MemoryGrant") if
                          float(op.get('SerialRequiredMemory', 0)) > 1048576]
    if high_memory_grants:
        print("警告: 查询被分配了过多的内存，可能导致其他查询资源争夺。考虑优化查询以减少内存使用。")

    # 规则 130: 检查表的扫描操作而不是索引的扫描
    full_table_scans = root.findall(".//TableScan")
    if full_table_scans:
        print("警告: 查询中存在全表扫描操作，可能影响性能。考虑使用或优化索引以减少全表扫描。")

    # 规则 131: 检查哈希递归操作
    hash_recursive = root.findall(".//Hash[@Recursive='True']")
    if hash_recursive:
        print("警告: 查询中存在哈希递归操作，可能影响性能。考虑优化相关的连接策略。")

    # 规则 132: 检查高度并行的操作
    highly_parallel_ops = [op for op in root.findall(".//RelOp") if int(op.get('Parallel', 0)) > 8]
    if highly_parallel_ops:
        print("警告: 查询中存在高度并行的操作，可能导致资源争夺。考虑调整查询或并行度设置。")

    # 规则 133: 检查流水线函数调用
    streaming_udfs = root.findall(".//StreamingUDF")
    if streaming_udfs:
        print("警告: 查询中存在流水线UDF调用，可能导致性能下降。考虑优化或避免使用流水线UDF。")

    # 规则 134: 检查高开销的UDF调用
    high_cost_udfs = [op for op in root.findall(".//UDF") if float(op.get('EstimateTotalSubtreeCost', 0)) > 10]
    if high_cost_udfs:
        print("警告: 查询中存在高开销的UDF调用，可能影响性能。考虑优化或避免使用这些UDF。")

    # 规则 135: 检查大量的外部表操作
    external_table_ops = root.findall(".//ExternalTable")
    if external_table_ops:
        print("警告: 查询中存在大量的外部表操作，可能导致性能问题。考虑将数据本地化或优化外部查询。")

    # 规则 136: 检查不优化的子查询
    unoptimized_subqueries = root.findall(".//UnoptimizedSubquery")
    if unoptimized_subqueries:
        print("警告: 查询中存在不优化的子查询，可能导致性能问题。考虑重写子查询或将其转化为连接操作。")

    # 规则 137: 检查高开销的动态SQL操作
    dynamic_sql_ops = [op for op in root.findall(".//DynamicSQL") if float(op.get('EstimateTotalSubtreeCost', 0)) > 10]
    if dynamic_sql_ops:
        print("警告: 查询中存在高开销的动态SQL操作，可能影响性能。考虑优化或避免使用动态SQL。")

    # 规则 138: 检查数据移动操作
    data_movement_ops = root.findall(".//DataMovement")
    if data_movement_ops:
        print("警告: 查询中存在数据移动操作，可能导致性能下降。考虑优化查询或数据分布策略。")

    # 规则 139: 检查列存储索引扫描的不优化操作
    non_optimized_columnstore = root.findall(".//ColumnStoreIndexScan[@Optimized='False']")
    if non_optimized_columnstore:
        print("警告: 查询中的列存储索引扫描未被优化，可能影响性能。考虑优化查询或列存储索引设置。")

    # 规则 140: 检查排序操作中的高内存使用
    high_mem_sorts = [op for op in root.findall(".//Sort") if float(op.get('MemoryFractions', 0)) > 0.5]
    if high_mem_sorts:
        print("警告: 查询中的排序操作使用了大量内存，可能导致资源争夺。考虑优化查询或调整资源分配。")

    # 规则 141: 检查 Bitmap 过滤器操作
    bitmap_filters = root.findall(".//Bitmap")
    if bitmap_filters:
        print(
            "警告: 查询中存在 Bitmap 过滤器操作。虽然这些操作有时可以提高性能，但在某些情况下它们可能导致性能下降。考虑对相关查询进行优化。")

    # 规则 142: 检查计算标量操作
    compute_scalars = root.findall(".//ComputeScalar")
    if compute_scalars:
        print("警告: 查询中存在大量的计算标量操作，可能导致CPU开销增加。考虑优化相关的标量计算或将其移到应用程序中进行。")

    # 规则 143: 检查嵌套循环连接
    nested_loops = root.findall(".//NestedLoops")
    if nested_loops:
        print("警告: 查询中存在嵌套循环连接，这可能在大数据集上效率较低。考虑优化连接策略或确保相关列已经进行了索引。")

    # 规则 144: 检查远程查询
    remote_queries = root.findall(".//RemoteQuery")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，可能导致网络开销增加。考虑将数据本地化或优化远程查询。")

    # 规则 145: 检查表变量操作
    table_vars = root.findall(".//TableVariable")
    if table_vars:
        print("警告: 查询中使用了表变量，这可能在某些情况下效率较低。考虑使用临时表或优化表变量使用。")

    # 规则 146: 检查 TVF (表值函数) 扫描
    tvf_scans = root.findall(".//TableValuedFunction")
    if tvf_scans:
        print("警告: 查询中存在表值函数(TVF)扫描，可能导致性能下降。考虑优化 TVF 或使用其他方法重写查询。")

    # 规则 147: 检查 Sort 操作中的警告
    sort_warnings = [op for op in root.findall(".//Sort") if op.get('WithAbortOption', 'False') == 'True']
    if sort_warnings:
        print("警告: 查询中的排序操作存在潜在的中止选项，可能导致查询提前结束。确保为排序操作提供足够的资源或优化查询。")

    # 规则 148: 检查 Spool 操作
    spool_ops = root.findall(".//Spool")
    if spool_ops:
        print("警告: 查询中存在 Spool 操作，可能导致磁盘开销增加。考虑优化查询以减少或消除 Spool 操作。")

    # 规则 149: 检查 Window 函数操作
    window_funcs = root.findall(".//Window")
    if window_funcs:
        print("警告: 查询中使用了窗口函数，可能导致性能下降。考虑优化窗口函数的使用或重写查询。")

    # 规则 150: 检查交叉应用操作
    cross_app_ops = root.findall(".//CrossApp")
    if cross_app_ops:
        print("警告: 查询中存在跨应用操作，可能导致性能和数据一致性问题。考虑将数据移动到同一应用或优化跨应用查询。")

    # 规则 151: 检查哈希匹配操作
    hash_matches = root.findall(".//HashMatch")
    if hash_matches:
        print("警告: 查询中存在哈希匹配操作，可能导致内存开销增加。考虑优化连接策略或确保相关列已经进行了索引。")

    # 规则 152: 检查高代价的流操作
    high_cost_streams = [op for op in root.findall(".//RelOp") if float(op.get('EstimatedTotalSubtreeCost', 0)) > 10]
    if high_cost_streams:
        print("警告: 查询中存在高代价的流操作，可能导致性能下降。仔细检查这些操作并考虑进行优化。")

    # 规则 153: 检查非平行查询操作
    non_parallel_ops = root.findall(".//NonParallelPlanReason")
    if non_parallel_ops:
        print("警告: 查询未能并行执行。考虑优化查询或检查服务器设置以支持并行处理。")

    # 规则 154: 检查大型数据移动操作
    data_movement_ops = root.findall(".//DataMovement")
    if data_movement_ops:
        print("警告: 查询中存在大型数据移动操作，可能导致网络或磁盘开销增加。考虑优化查询或数据库结构。")

    # 规则 155: 检查大型删除操作
    delete_ops = [op for op in root.findall(".//Delete") if int(op.get('RowCount', 0)) > 10000]
    if delete_ops:
        print("警告: 查询中执行了大量的删除操作，可能导致性能下降或锁定问题。考虑分批进行删除或优化删除策略。")

    # 规则 156: 检查多表连接操作
    multi_table_joins = root.findall(".//Join[@PhysicalOp='MultiTableJoin']")
    if multi_table_joins:
        print("警告: 查询中存在多表连接操作，可能导致性能下降。考虑重写查询或优化连接策略。")

    # 规则 157: 检查列存储索引的效率
    columnstore_scans = root.findall(".//ColumnStoreIndexScan")
    if columnstore_scans:
        print("警告: 查询中存在列存储索引扫描，但可能没有充分利用列存储的优势。考虑优化查询或检查列存储索引的设计。")

    # 规则 158: 检查排序操作的内存开销
    sort_memory_issues = [op for op in root.findall(".//Sort") if float(op.get('MemoryFraction', 0)) > 0.5]
    if sort_memory_issues:
        print("警告: 查询中的排序操作使用了大量的内存。考虑优化排序操作或增加查询的内存配额。")

    # 规则 159: 检查潜在的死锁操作
    potential_deadlocks = root.findall(".//Deadlock")
    if potential_deadlocks:
        print("警告: 查询中存在可能导致死锁的操作。考虑重写查询或调整事务隔离级别。")

    # 规则 160: 检查全文搜索操作的效率
    fulltext_searches = root.findall(".//FullTextSearch")
    if fulltext_searches:
        print("警告: 查询中使用了全文搜索，但可能没有充分优化。考虑检查全文索引或优化全文查询。")

    # 规则 161: 检查嵌套循环连接操作
    nested_loops = root.findall(".//NestedLoops")
    if nested_loops:
        print("警告: 查询中存在嵌套循环连接操作，可能会影响大数据集的性能。")

    # 规则 162: 检查哈希匹配连接操作
    hash_matches = root.findall(".//HashMatch")
    if hash_matches:
        print("警告: 查询中存在哈希匹配连接操作，可能会导致额外的I/O和CPU负担。")

    # 规则 163: 检查表变量的使用
    table_vars = root.findall(".//TableValuedFunction")
    if table_vars:
        print("警告: 查询中使用了表变量，可能会影响性能，尤其是在大数据集上。")

    # 规则 164: 检查RID Lookup操作
    rid_lookup = root.findall(".//RIDLookup")
    if rid_lookup:
        print("警告: 查询中存在RID Lookup操作，这通常意味着缺少聚集索引。")

    # 规则 165: 检查Filter操作
    filters = root.findall(".//Filter")
    if filters:
        print("警告: 查询中存在Filter操作，可能会导致查询性能下降。")

    # 规则 166: 检查并行操作
    parallel_ops = root.findall(".//Parallelism")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能会导致资源争用和性能下降。")

    # 规则 167: 检查Sort操作
    sort_ops = root.findall(".//Sort")
    if sort_ops:
        print("警告: 查询中存在Sort操作，大量的排序可能会消耗大量的CPU和内存。")

    # 规则 168: 检查Compute Scalar操作
    compute_scalar_ops = root.findall(".//ComputeScalar")
    if compute_scalar_ops:
        print("警告: 查询中存在Compute Scalar操作，可能会导致额外的计算开销。")

    # 规则 169: 检查Sequence Project操作
    sequence_project_ops = root.findall(".//SequenceProject")
    if sequence_project_ops:
        print("警告: 查询中存在Sequence Project操作，可能会影响查询性能。")

    # 规则 170: 检查Stream Aggregate操作
    stream_aggregate_ops = root.findall(".//StreamAggregate")
    if stream_aggregate_ops:
        print("警告: 查询中存在Stream Aggregate操作，可能会导致I/O和CPU的额外负担。")

    # 规则 171: 检查Convert操作
    convert_ops = root.findall(".//Convert")
    if convert_ops:
        print("警告: 查询中存在数据类型转换操作，可能会导致性能下降。")

    # 规则 172: 检查Constant Scan操作
    constant_scan_ops = root.findall(".//ConstantScan")
    if constant_scan_ops:
        print("警告: 查询中存在Constant Scan操作，可能影响查询性能。")

    # 规则 173: 检查外部表连接
    external_table_joins = root.findall(".//RemoteQuery")
    if external_table_joins:
        print("警告: 查询涉及外部表连接，可能导致性能问题。")

    # 规则 174: 检查Sparse Column操作
    sparse_column_ops = root.findall(".//SparseColumnOperator")
    if sparse_column_ops:
        print("警告: 查询中使用了稀疏列，这可能会影响性能。")

    # 规则 175: 检查TOP操作
    top_ops = root.findall(".//Top")
    if top_ops:
        print("警告: 查询中使用了TOP操作，可能导致性能问题，尤其是当未与ORDER BY结合使用时。")

    # 规则 176: 检查UDF (用户定义函数) 的使用
    udf_ops = root.findall(".//UserDefinedFunction")
    if udf_ops:
        print("警告: 查询中使用了用户定义的函数，这可能导致性能问题。")

    # 规则 177: 检查Window Aggregate操作
    window_aggregate_ops = root.findall(".//WindowAggregate")
    if window_aggregate_ops:
        print("警告: 查询中使用了窗口聚合函数，可能导致性能问题。")

    # 规则 178: 检查XML运算操作
    xml_ops = root.findall(".//XmlReader")
    if xml_ops:
        print("警告: 查询中存在XML操作，可能会影响性能。")

    # 规则 179: 检查全文索引查询
    fulltext_query = root.findall(".//Contains")
    if fulltext_query:
        print("警告: 查询中使用了全文索引查询，可能导致性能问题。")

    # 规则 180: 检查动态SQL操作
    dynamic_sql_ops = root.findall(".//Dynamic")
    if dynamic_sql_ops:
        print("警告: 查询中存在动态SQL操作，可能导致性能和安全问题。")

    # 规则 181: 检查Sort操作，可能意味着查询正在对数据进行排序。
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_operations:
        print("警告: 查询中存在Sort操作，可能意味着查询正在对数据进行排序。考虑优化排序策略或使用索引。")

    # 规则 182: 检查悬挂的外部连接
    unmatched_outer_joins = root.findall(".//UnmatchedOuterJoin")
    if unmatched_outer_joins:
        print("警告: 查询中存在悬挂的外部连接，可能导致性能问题。")

    # 规则 183: 检查表值函数
    table_valued_function = root.findall(".//TableValuedFunction")
    if table_valued_function:
        print("警告: 查询中使用了表值函数，可能导致性能问题。")

    # 规则 184: 检查列存储索引扫描
    column_store_scan = root.findall(".//ColumnStoreIndexScan")
    if column_store_scan:
        print("警告: 查询中使用了列存储索引扫描，可能导致性能问题。")

    # 规则 185: 检查列存储索引查找
    column_store_seek = root.findall(".//ColumnStoreIndexSeek")
    if column_store_seek:
        print("警告: 查询中使用了列存储索引查找，可能导致性能问题。")

    # 规则 186: 检查列存储哈希匹配
    column_store_hash = root.findall(".//ColumnStoreHashJoin")
    if column_store_hash:
        print("警告: 查询中使用了列存储哈希匹配，可能导致性能问题。")

    # 规则 187: 检查非优化的嵌套循环
    non_optimized_loops = root.findall(".//NestedLoops")
    if non_optimized_loops:
        print("警告: 查询中存在非优化的嵌套循环，可能导致性能问题。")

    # 规则 188: 检查递归查询
    recursive_cte = root.findall(".//RecursiveCTE")
    if recursive_cte:
        print("警告: 查询中使用了递归公共表达式，可能导致性能问题。")

    # 规则 189: 检查是否存在"Hash Match"
    hash_match_operations = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_match_operations:
        print("警告: 查询中存在'Hash Match'操作，这可能表示没有找到合适的索引。考虑优化查询或添加合适的索引。")

    # 规则 190: 检查非参数化查询
    non_param_queries = root.findall(".//NonParameterizedQuery")
    if non_param_queries:
        print("警告: 查询中存在非参数化查询，可能导致性能问题和SQL注入风险。")

    # 规则 191: 检查顺序扫描
    seq_scans = root.findall(".//SequenceProject")
    if seq_scans:
        print("警告: 查询中存在顺序扫描，可能导致性能问题。")

    # 规则 192: 检查是否存在"Table Scan"
    table_scan_operations = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    if table_scan_operations:
        print("警告: 查询中存在'Table Scan'操作，这通常比'Index Scan'慢。考虑优化查询或添加合适的索引。")

    # 规则 193: 检查空连接
    null_joins = root.findall(".//NullIf")
    if null_joins:
        print("警告: 查询中使用了空连接，可能导致性能问题。")

    # 规则 194: 检查使用不等于操作
    not_equals_ops = root.findall(".//NotEquals")
    if not_equals_ops:
        print("警告: 查询中使用了不等于操作，可能导致性能问题。")

    # 规则 195: 检查大型插入
    bulk_inserts = root.findall(".//BulkInsert")
    if bulk_inserts:
        print("警告: 查询中存在大型插入操作，可能导致性能问题。")

    # 规则 196: 检查大型更新
    bulk_updates = root.findall(".//BulkUpdate")
    if bulk_updates:
        print("警告: 查询中存在大型更新操作，可能导致性能问题。")

    # 规则 197: 检查硬编码值
    hardcoded_vals = root.findall(".//ConstantScan")
    if hardcoded_vals:
        print("警告: 查询中存在硬编码的值，可能导致性能问题和可维护性问题。")

    # 规则 198: 检查复杂的视图嵌套
    nested_views = root.findall(".//View")
    if len(nested_views) > 2:
        print("警告: 查询中存在过多的视图嵌套，可能导致性能问题。")

    # 规则 199: 检查不必要的计算
    unnecessary_computations = root.findall(".//ComputeScalar")
    if unnecessary_computations:
        print("警告: 查询中存在不必要的计算，可能导致性能问题。")

    # 规则 200: 检查大量的嵌套子查询
    nested_subqueries = [op for op in root.findall(".//Subquery") if int(op.get('NestedLevel', 0)) > 5]
    if nested_subqueries:
        print("警告: 查询中存在大量的嵌套子查询，可能导致性能下降。考虑将部分子查询改写为连接或临时表。")


    # 规则 201: 检查Hash Match操作，可能意味着查询需要优化
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash Match操作，可能需要进一步优化。")

    # 规则 202: 检查RID Lookup操作，可能意味着需要更好的索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，考虑优化相关索引。")

    # 规则 203: 检查Nested Loops Join，当数据量大时可能不高效
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if nested_loops:
        print("警告: 查询中存在Nested Loops操作，可能需要进一步优化。")

    # 规则 204: 检查大量的Sort操作，可能影响性能
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_operations:
        print("警告: 查询中存在多个Sort操作，可能影响性能。")

    # 规则 205: 检查Parallelism操作，可能意味着查询可以进一步优化
    parallelism_operations = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if parallelism_operations:
        print("警告: 查询中存在Parallelism操作，考虑进一步优化查询。")

    # 规则 206: 检查Filter操作，可能意味着查询条件不高效
    filter_operations = root.findall(".//RelOp[@PhysicalOp='Filter']")
    if filter_operations:
        print("警告: 查询中存在Filter操作，可能需要调整查询条件。")

    # 规则 207: 检查Compute Scalar操作，可能影响性能
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalars:
        print("警告: 查询中存在Compute Scalar操作，可能影响性能。")

    # 规则 208: 检查非优化的Bitmap操作
    non_optimized_bitmaps = root.findall(".//RelOp[@PhysicalOp='Bitmap']")
    if non_optimized_bitmaps:
        print("警告: 查询中存在非优化的Bitmap操作，考虑进一步优化查询。")

    # 规则 209: 检查Sequence Project操作，可能意味着查询需要优化
    sequence_projects = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_projects:
        print("警告: 查询中存在Sequence Project操作，可能需要进一步优化。")

    # 规则 210: 检查流水线操作，可能意味着查询中的某些部分不高效
    stream_aggregate = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_aggregate:
        print("警告: 查询中存在Stream Aggregate操作，可能需要进一步优化。")

    # ... [继续上述的代码]

    # 规则 211: 检查存在的递归操作，可能影响性能
    recursive_operations = root.findall(".//RelOp[@PhysicalOp='Recursive Union']")
    if recursive_operations:
        print("警告: 查询中存在递归操作，可能影响性能。")

    # 规则 212: 检查Hash Team操作，可能意味着需要更大的内存
    hash_teams = root.findall(".//RelOp[@PhysicalOp='Hash Team']")
    if hash_teams:
        print("警告: 查询中存在Hash Team操作，考虑增加可用内存或优化查询。")

    # 规则 213: 检查存在的动态索引操作，可能影响性能
    dynamic_indexes = root.findall(".//RelOp[@PhysicalOp='Dynamic Index']")
    if dynamic_indexes:
        print("警告: 查询中存在动态索引操作，可能影响性能。")

    # 规则 214: 检查存在的动态排序操作，可能影响性能
    dynamic_sorts = root.findall(".//RelOp[@PhysicalOp='Dynamic Sort']")
    if dynamic_sorts:
        print("警告: 查询中存在动态排序操作，可能影响性能。")

    # 规则 215: 检查存在的Bitmap Heap操作，可能意味着查询需要优化
    bitmap_heaps = root.findall(".//RelOp[@PhysicalOp='Bitmap Heap']")
    if bitmap_heaps:
        print("警告: 查询中存在Bitmap Heap操作，考虑进一步优化查询。")

    # 规则 216: 检查存在的远程查询操作，可能意味着跨服务器查询不高效
    remote_queries = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，考虑优化跨服务器查询。")

    # 规则 217: 检查存在的流水线排序操作，可能影响性能
    stream_sorts = root.findall(".//RelOp[@PhysicalOp='Stream Sort']")
    if stream_sorts:
        print("警告: 查询中存在Stream Sort操作，可能影响性能。")

    # 规则 218: 检查存在的窗口聚合操作，可能意味着查询需要优化
    window_aggregates = root.findall(".//RelOp[@PhysicalOp='Window Aggregate']")
    if window_aggregates:
        print("警告: 查询中存在窗口聚合操作，考虑进一步优化查询。")

    # 规则 219: 检查存在的列存储索引扫描，可能意味着列存储索引需要优化
    columnstore_index_scans = root.findall(".//RelOp[@PhysicalOp='Columnstore Index Scan']")
    if columnstore_index_scans:
        print("警告: 查询中存在列存储索引扫描操作，考虑优化列存储索引。")

    # 规则 220: 检查存在的分区操作，可能影响性能
    partition_operations = root.findall(".//RelOp[@PhysicalOp='Partition']")
    if partition_operations:
        print("警告: 查询中存在分区操作，可能影响性能。")

    # 规则 221: 检查存在的Hash匹配操作，这可能意味着连接不够高效
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash匹配操作，考虑使用其他连接策略如Merge或Loop。")

    # 规则 222: 检查并行操作，可能意味着查询可以进一步优化以避免并行处理
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能意味着查询需要进一步优化。")

    # 规则 223: 检查存在的Compute Scalar操作，这可能意味着有计算操作可以在查询中优化
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalars:
        print("警告: 查询中存在Compute Scalar操作，考虑是否有计算可以优化。")

    # 规则 224: 检查存在的顺序扫描，可能意味着缺少索引
    sequence_scans = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_scans:
        print("警告: 查询中存在顺序扫描，考虑添加适当的索引。")

    # 规则 225: 检查存在的Table Spool操作，可能影响性能
    table_spools = root.findall(".//RelOp[@PhysicalOp='Table Spool']")
    if table_spools:
        print("警告: 查询中存在Table Spool操作，可能影响性能。")

    # 规则 226: 检查存在的RID Lookup操作，可能意味着需要一个聚集索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，考虑添加一个聚集索引。")

    # 规则 227: 检查存在的Top操作，可能意味着查询返回大量数据
    top_ops = root.findall(".//RelOp[@PhysicalOp='Top']")
    if top_ops:
        print("警告: 查询中存在Top操作，考虑是否真的需要返回那么多数据。")

    # 规则 228: 检查存在的Key Lookup操作，可能意味着非聚集索引缺失某些列
    key_lookups = root.findall(".//RelOp[@PhysicalOp='Key Lookup']")
    if key_lookups:
        print("警告: 查询中存在Key Lookup操作，考虑将查找的列包括在非聚集索引中。")

    # 规则 229: 检查存在的Nested Loops操作，可能意味着连接不够高效
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if nested_loops:
        print("警告: 查询中存在Nested Loops操作，考虑优化查询或使用其他连接策略。")

    # 规则 230: 检查存在的Bitmap操作，这可能影响性能
    bitmaps = root.findall(".//RelOp[@PhysicalOp='Bitmap Create']")
    if bitmaps:
        print("警告: 查询中存在Bitmap操作，可能影响性能。")

    # 规则 231: 检查存在的流操作，它可能表示数据排序并可能影响性能
    stream_ops = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_ops:
        print("警告: 查询中存在Stream Aggregate操作，可能表示数据排序并影响性能。")

    # 规则 232: 检查Sort操作，它可能导致性能下降
    sort_ops = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_ops:
        print("警告: 查询中存在Sort操作，考虑优化查询以减少或消除排序。")

    # 规则 233: 检查存在的Remote Query操作，可能意味着跨服务器查询，这可能影响性能
    remote_queries = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if remote_queries:
        print("警告: 查询中存在Remote Query操作，跨服务器查询可能影响性能。")

    # 规则 234: 检查Filter操作，尤其是高成本的Filter，这可能影响性能
    high_cost_filters = [op for op in root.findall(".//RelOp[@PhysicalOp='Filter']") if
                         float(op.attrib['EstimatedTotalSubtreeCost']) > 1.0]
    if high_cost_filters:
        print("警告: 查询中存在高成本的Filter操作，考虑优化查询条件。")

    # 规则 235: 检查存在的Constant Scan操作，这可能意味着查询中有不必要的常数扫描
    constant_scans = root.findall(".//RelOp[@PhysicalOp='Constant Scan']")
    if constant_scans:
        print("警告: 查询中存在Constant Scan操作，考虑优化查询以避免不必要的常数扫描。")

    # 规则 236: 检查存在的Dynamic Index Seek操作，这可能意味着索引未被完全利用
    dynamic_seeks = root.findall(".//RelOp[@PhysicalOp='Dynamic Index Seek']")
    if dynamic_seeks:
        print("警告: 查询中存在Dynamic Index Seek操作，考虑优化索引以提高其效率。")

    # 规则 237: 检查存在的Bitmap Heap Scan，这可能意味着需要一个索引来改善性能
    bitmap_heap_scans = root.findall(".//RelOp[@PhysicalOp='Bitmap Heap Scan']")
    if bitmap_heap_scans:
        print("警告: 查询中存在Bitmap Heap Scan操作，考虑添加索引以改善性能。")

    # 规则 238: 检查存在的动态序列扫描，可能表示查询中有动态生成的序列
    dynamic_sequence_scans = root.findall(".//RelOp[@PhysicalOp='Dynamic Sequence Project']")
    if dynamic_sequence_scans:
        print("警告: 查询中存在Dynamic Sequence Project操作，可能影响性能。")

    # 规则 239: 检查存在的列存储索引扫描，这可能意味着列存储索引未被完全利用
    columnstore_scans = root.findall(".//RelOp[@PhysicalOp='Columnstore Index Scan']")
    if columnstore_scans:
        print("警告: 查询中存在Columnstore Index Scan操作，考虑优化查询以更好地利用列存储索引。")

    # 规则 240: 检查存在的外部表扫描，这可能意味着查询正在从外部数据源检索数据
    external_table_scans = root.findall(".//RelOp[@PhysicalOp='External Table Scan']")
    if external_table_scans:
        print("警告: 查询中存在External Table Scan操作，访问外部数据源可能影响性能。")

    # 规则 241: 检查大量的嵌套循环连接操作
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if len(nested_loops) > 5:
        print("警告: 查询中存在大量的Nested Loops操作，可能影响性能。考虑优化查询或索引。")

    # 规则 242: 检查Hash Match操作，它可能导致内存中的数据溢出到磁盘
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash Match操作。这可能导致内存中的数据溢出到磁盘，影响性能。")

    # 规则 243: 检查递归CTE，它可能导致性能问题
    recursive_ctes = root.findall(".//RelOp[@LogicalOp='Recursive Union']")
    if recursive_ctes:
        print("警告: 查询中使用了递归CTE，可能导致性能问题。")

    # 规则 244: 检查RID Lookup操作，它可能表示需要聚集索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，可能需要聚集索引来改善性能。")

    # 规则 245: 检查Adaptive Join操作，可能影响性能
    adaptive_joins = root.findall(".//RelOp[@PhysicalOp='Adaptive Join']")
    if adaptive_joins:
        print("警告: 查询中存在Adaptive Join操作，可能影响性能。")

    # 规则 246: 检查并行操作，它们可能导致线程竞争和性能下降
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能导致线程竞争和性能下降。")

    # 规则 247: 检查表变量操作，它们可能没有统计信息并导致性能问题
    table_vars = root.findall(".//RelOp[@PhysicalOp='Table-valued function']")
    if table_vars:
        print("警告: 查询中使用了表变量，它们可能没有统计信息并导致性能问题。")

    # 规则 248: 检查Compute Scalar操作，大量的Compute Scalar可能影响性能
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if len(compute_scalars) > 5:
        print("警告: 查询中存在大量的Compute Scalar操作，可能影响性能。")

    # 规则 249: 检查非SARGable操作，如函数在WHERE子句中的列上
    non_sargable = root.findall(".//ScalarOperator[Function]")
    if non_sargable:
        print("警告: 查询中存在非SARGable操作，可能影响性能。")

    # 规则 250: 检查大量的Spool操作，可能影响性能
    spool_ops = root.findall(".//RelOp[@PhysicalOp='Spool']")
    if len(spool_ops) > 3:
        print("警告: 查询中存在大量的Spool操作，可能影响性能。")

    # 规则 251: 检查Sort操作，因为它们可能导致内存中的数据溢出到磁盘
    sort_ops = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_ops:
        print("警告: 查询中存在Sort操作，这可能导致内存中的数据溢出到磁盘，影响性能。")

    # 规则 252: 检查存在的外部表操作，这可能表示跨数据库或远程查询
    external_tables = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if external_tables:
        print("警告: 查询中存在远程查询操作，可能影响性能。考虑将数据本地化。")

    # 规则 253: 检查Bitmap操作，因为它们可能导致CPU使用率增加
    bitmap_ops = root.findall(".//RelOp[@PhysicalOp='Bitmap']")
    if bitmap_ops:
        print("警告: 查询中存在Bitmap操作，这可能导致CPU使用率增加。")

    # 规则 254: 检查存在的流聚合，因为在大数据集上可能不高效
    stream_aggregates = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_aggregates:
        print("警告: 查询中存在流聚合操作，这在大数据集上可能不高效。")

    # 规则 255: 检查存在的窗口聚合，因为它们可能影响性能
    window_aggs = root.findall(".//RelOp[@PhysicalOp='Window Aggregate']")
    if window_aggs:
        print("警告: 查询中存在窗口聚合操作，这可能影响性能。")

    # 规则 256: 检查存在的序列投影，它们可能导致内存压力
    sequence_projections = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_projections:
        print("警告: 查询中存在序列投影操作，这可能导致内存压力。")

    # 规则 257: 检查高成本的操作，因为它们可能是性能瓶颈
    high_cost_ops = [op for op in root.findall(".//RelOp") if float(op.get('EstimatedTotalSubtreeCost', '0')) > 50]
    if high_cost_ops:
        print("警告: 查询中存在高成本的操作，可能是性能瓶颈。")

    # 规则 258: 检查存在的哈希匹配部分连接，因为它们可能导致内存中的数据溢出到磁盘
    hash_partial_joins = root.findall(".//RelOp[@PhysicalOp='Partial Hash Match']")
    if hash_partial_joins:
        print("警告: 查询中存在哈希匹配部分连接，这可能导致内存中的数据溢出到磁盘。")

    # 规则 259: 检查存在的懒惰溢出，因为这可能表示内存压力
    lazy_spools = root.findall(".//RelOp[@PhysicalOp='Lazy Spool']")
    if lazy_spools:
        print("警告: 查询中存在懒惰溢出操作，这可能表示内存压力。")

    # 规则 260: 检查存在的非优化的嵌套循环，因为它们可能是性能瓶颈
    non_opt_loops = root.findall(".//RelOp[@PhysicalOp='Non-Optimized Nested Loops']")
    if non_opt_loops:
        print("警告: 查询中存在非优化的嵌套循环操作，这可能是性能瓶颈。")

    # 规则 261: 检查是否存在过多的计算列
    computed_columns = root.findall(".//ComputeScalar")
    if len(computed_columns) > 5:
        print("警告: 查询中存在过多的计算列，可能导致CPU负担增加。")

    # 规则 262: 检查是否使用了全文搜索
    full_text_search = root.findall(".//Contains")
    if full_text_search:
        print("警告: 查询使用了全文搜索，可能影响性能。确保全文搜索已正确配置并优化。")

    # 规则 263: 检查是否存在并行操作，但并行度过低
    parallel_ops = [op for op in root.findall(".//RelOp") if "Parallel" in op.get('PhysicalOp', '')]
    if parallel_ops and int(root.get('DegreeOfParallelism', 1)) < 2:
        print("警告: 查询中存在并行操作，但并行度过低。考虑提高并行度。")

    # 规则 264: 检查是否存在非平衡的并行操作
    non_balanced_parallel_ops = [op for op in parallel_ops if op.get('NonParallelPlanReason') == 'NonParallelizable']
    if non_balanced_parallel_ops:
        print("警告: 查询中存在非平衡的并行操作，可能导致资源未被充分利用。")

    # 规则 265: 检查是否存在过多的UDF调用
    udf_calls = root.findall(".//UDF")
    if len(udf_calls) > 3:
        print("警告: 查询中存在过多的UDF调用，可能导致性能下降。")

    # 规则 266: 检查是否存在非SARGable操作，导致索引未能有效使用
    non_sargable_ops = root.findall(".//Filter[@NonSargable]")
    if non_sargable_ops:
        print("警告: 查询中存在非SARGable操作，可能导致索引未能有效使用。")

    # 规则 267: 检查是否有因为数据类型不匹配导致的隐式转换
    implicit_conversions = root.findall(".//Convert[@Implicit]")
    if implicit_conversions:
        print("警告: 查询中存在隐式数据类型转换，可能导致性能下降。")

    # 规则 268: 检查是否存在过大的数据移动操作
    large_data_movement_ops = [op for op in root.findall(".//RelOp") if
                               float(op.get('EstimatedDataSize', '0')) > 1000000]
    if large_data_movement_ops:
        print("警告: 查询中存在大量数据移动操作，可能导致性能瓶颈。")

    # 规则 269: 检查是否有太多的内存授予操作，可能导致内存压力
    high_memory_grants = [op for op in root.findall(".//MemoryGrant") if float(op.get('RequestedMemory', '0')) > 10000]
    if high_memory_grants:
        print("警告: 查询请求了大量的内存，可能导致内存压力。")

    # 规则 270: 检查是否存在多次读取同一表的操作
    multiple_table_reads = {}
    for op in root.findall(".//RelOp"):
        table_name = op.find(".//Object[@Table]")
        if table_name is not None:
            table_name = table_name.get('Table', '')
            multiple_table_reads[table_name] = multiple_table_reads.get(table_name, 0) + 1

    tables_read_multiple_times = [table for table, count in multiple_table_reads.items() if count > 1]
    if tables_read_multiple_times:
        print(f"警告: 表 {', '.join(tables_read_multiple_times)} 在查询中被多次读取，可能导致性能下降。")

    # 规则 271: 检查是否存在过大的哈希匹配
    large_hash_matches = [op for op in root.findall(".//HashMatch") if
                          float(op.get('EstimatedDataSize', '0')) > 1000000]
    if large_hash_matches:
        print("警告: 查询中存在大型的哈希匹配操作，可能导致性能下降。")

    # 规则 272: 检查是否存在高开销的嵌套循环
    expensive_loops = [op for op in root.findall(".//NestedLoops") if
                       float(op.get('EstimatedTotalSubtreeCost', '0')) > 5.0]
    if expensive_loops:
        print("警告: 查询中存在高开销的嵌套循环操作，考虑优化相关逻辑。")

    # 规则 273: 检查是否使用了远程查询
    remote_queries = root.findall(".//RemoteQuery")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，可能导致性能延迟。")

    # 规则 274: 检查是否存在不必要的物理操作，如排序
    redundant_sorts = root.findall(".//Sort[@IsExplicitlyForced]")
    if redundant_sorts:
        print("警告: 查询中存在不必要的排序操作，考虑移除冗余的ORDER BY子句。")

    # 规则 275: 检查是否有大量数据的插入操作
    large_inserts = [op for op in root.findall(".//Insert") if float(op.get('EstimatedDataSize', '0')) > 500000]
    if large_inserts:
        print("警告: 查询中存在大量数据的插入操作，可能导致性能瓶颈。")

    # 规则 276: 检查是否有大量数据的更新操作
    large_updates = [op for op in root.findall(".//Update") if float(op.get('EstimatedDataSize', '0')) > 500000]
    if large_updates:
        print("警告: 查询中存在大量数据的更新操作，可能导致性能瓶颈。")

    # 规则 277: 检查查询是否有回退操作
    rollbacks = root.findall(".//Rollback")
    if rollbacks:
        print("警告: 查询中存在回退操作，可能导致性能下降和数据不一致。")

    # 规则 278: 检查是否存在大量的动态SQL执行
    dynamic_sql_ops = [op for op in root.findall(".//DynamicSQL") if float(op.get('EstimatedDataSize', '0')) > 100000]
    if dynamic_sql_ops:
        print("警告: 查询中存在大量的动态SQL执行，可能导致安全风险和性能问题。")

    # 规则 279: 检查查询是否过于复杂，包含过多的连接和子查询
    highly_complex_queries = [op for op in root.findall(".//Query") if
                              len(op.findall(".//Join")) > 10 or len(op.findall(".//Subquery")) > 5]
    if highly_complex_queries:
        print("警告: 查询可能过于复杂，考虑分解或重新设计查询逻辑。")

    # 规则 280: 检查是否有过多的列被选择，可能导致不必要的数据传输
    over_selected_columns = [op for op in root.findall(".//OutputList") if len(op.findall(".//ColumnReference")) > 20]
    if over_selected_columns:
        print("警告: 查询中选择了过多的列，考虑只选择必要的列以提高性能。")

    # 规则 281: 检查表的广播操作
    broadcast_ops = root.findall(".//Broadcast")
    if broadcast_ops:
        print("警告: 查询中存在表的广播操作，这可能会导致性能问题，特别是在大数据集上。")

    # 规则 282: 检查是否有在内存中的大型表扫描操作
    in_memory_table_scans = [op for op in root.findall(".//TableScan") if op.get('Storage') == 'MemoryOptimized']
    if in_memory_table_scans:
        print("警告: 查询中存在内存中的大型表扫描，考虑优化索引或查询以避免全表扫描。")

    # # 规则 283: 检查是否有过多的自连接
    rel_ops = root.findall(".//RelOp")
    self_joins = [
        op for op in rel_ops if op.find('.//Join') is not None and
                                op.find(".//Object").get('Database') == op.find(".//Object[2]").get('Database') and
                                op.find(".//Object").get('Schema') == op.find(".//Object[2]").get('Schema') and
                                op.find(".//Object").get('Table') == op.find(".//Object[2]").get('Table')
    ]

    if self_joins:
        print("警告: 查询中存在过多的自连接，可能导致性能下降。")

    # 规则 284: 检查是否有使用悲观锁定
    pessimistic_locks = root.findall(".//PessimisticLock")
    if pessimistic_locks:
        print("警告: 查询中使用了悲观锁定，可能导致其他查询被阻塞。")

    # 规则 285: 检查是否存在高代价的远程查询
    expensive_remote_queries = [op for op in root.findall(".//RemoteQuery") if
                                float(op.get('EstimatedTotalSubtreeCost', '0')) > 5.0]
    if expensive_remote_queries:
        print("警告: 查询中存在高代价的远程查询，考虑优化远程查询或将数据本地化。")

    # 规则 286: 检查是否有大量的数据压缩和解压缩操作
    data_compression_ops = root.findall(".//DataCompression")
    if data_compression_ops:
        print("警告: 查询中存在大量的数据压缩和解压缩操作，这可能会导致性能下降。")

    # 规则 287: 检查是否使用了过时的或不建议使用的操作符
    deprecated_ops = root.findall(".//DeprecatedOperator")
    if deprecated_ops:
        print("警告: 查询中使用了过时的或不建议使用的操作符，考虑更新查询。")

    # 规则 288: 检查是否有大量的数据转换操作
    data_conversion_ops = root.findall(".//Convert")
    if data_conversion_ops:
        print("警告: 查询中存在大量的数据转换操作，这可能会导致性能下降。")

    # 规则 289: 检查是否存在大量的行到列的转换
    pivot_ops = root.findall(".//Pivot")
    if pivot_ops:
        print("警告: 查询中存在大量的行到列的转换，可能导致性能问题。")

    # 规则 290: 检查是否有不必要的数据复制操作
    copy_ops = root.findall(".//Copy")
    if copy_ops:
        print("警告: 查询中存在不必要的数据复制操作，考虑优化查询逻辑。")

    # 规则 291: 检查是否存在对大表的 Nested Loops
    large_nested_loops = [op for op in root.findall(".//NestedLoops") if
                          float(op.get('EstimatedRowCount', '0')) > 100000]
    if large_nested_loops:
        print("警告: 查询中存在对大表的 Nested Loops 操作，这可能会导致性能问题。考虑优化查询或使用其他的连接策略。")

    # 规则 292: 检查是否有不必要的 ORDER BY 操作
    unnecessary_order_by = [op for op in root.findall(".//Sort") if op.find(".//TopSort") is not None]
    if unnecessary_order_by:
        print("警告: 查询中存在不必要的 ORDER BY 操作，这可能会导致性能下降。考虑移除或优化排序操作。")

    # 规则 293: 检查是否存在对外部资源的访问，如链接的服务器
    external_access = root.findall(".//RemoteQuery")
    if external_access:
        print("警告: 查询中存在对外部资源的访问，这可能会导致性能问题。考虑将外部数据本地化或优化远程查询。")

    # 规则 294: 检查是否有过度的并行操作，可能导致资源争用
    excessive_parallelism = [op for op in root.findall(".//Parallelism") if
                             float(op.get('EstimatedTotalSubtreeCost', '0')) < 1.0]
    if excessive_parallelism:
        print("警告: 查询中存在过度的并行操作，这可能会导致资源争用和性能问题。考虑减少并行度或优化查询逻辑。")

    # 规则 295: 检查是否存在大量的空值检查操作
    null_checks = root.findall(".//IsNull")
    if null_checks:
        print("警告: 查询中存在大量的空值检查操作，这可能会导致性能下降。考虑优化查询逻辑或使用其他方法处理空值。")

    # 规则 296: 检查是否有大量的数据分区操作，可能导致数据碎片和性能问题
    partition_ops = root.findall(".//PartitionRange")
    if partition_ops:
        print("警告: 查询中存在大量的数据分区操作，这可能会导致数据碎片和性能问题。考虑优化数据分区策略或查询逻辑。")

    # 规则 297: 检查是否存在不必要的数据聚合操作
    unnecessary_aggregation = [op for op in root.findall(".//Aggregate") if op.get('GroupBy') is None]
    if unnecessary_aggregation:
        print("警告: 查询中存在不必要的数据聚合操作，这可能会导致性能下降。考虑移除或优化聚合操作。")

    # 规则 298: 检查是否有大量的数据合并操作，可能导致性能问题
    merge_ops = root.findall(".//Merge")
    if merge_ops:
        print("警告: 查询中存在大量的数据合并操作，这可能会导致性能问题。考虑优化数据合并策略或查询逻辑。")

    # 规则 299: 检查是否存在大量的动态数据操作
    dynamic_ops = root.findall(".//Dynamic")
    if dynamic_ops:
        print("警告: 查询中存在大量的动态数据操作，这可能会导致性能问题。考虑优化动态数据策略或查询逻辑。")

    # 规则 300: 检查是否有对非优化视图的访问
    non_optimized_views = [op for op in root.findall(".//View") if op.get('Optimized') == 'false']
    if non_optimized_views:
        print("警告: 查询中存在对非优化视图的访问，这可能会导致性能问题。考虑优化视图或查询逻辑。")

    # 规则 301: 检查Top操作，可能意味着查询不高效
    top_operations = root.findall(".//RelOp[@PhysicalOp='Top']")
    if top_operations:
        print("警告: 查询中存在Top操作，考虑进一步优化查询。")

    # 规则 302: 检查Sort操作，可能意味着查询不高效
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_operations:
        print("警告: 查询中存在Sort操作，考虑是否可以通过索引或其他方式优化。")

    # 规则 303: 检查Hash Match操作，可能意味着查询需要大量的内存
    hash_match_operations = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_match_operations:
        print("警告: 查询中存在Hash Match操作，这可能需要大量的内存。考虑优化查询或增加内存。")

    # 规则 304: 检查Clustered Index Scan和Table Scan操作，可能意味着查询不高效
    # 分解查询为两个单独的查询
    clustered_index_scans = root.findall(".//RelOp[@PhysicalOp='Clustered Index Scan']")
    table_scans = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    # 合并结果
    scan_operations = clustered_index_scans + table_scans
    if scan_operations:
        print("警告: 查询中存在Clustered Index Scan或Table Scan操作，可能导致性能下降。考虑是否可以优化索引策略。")

    # 规则 305: 检查是否存在扫描操作而不是查找操作，这可能意味着缺失索引
    # 分解查询为两个单独的查询
    clustered_index_scans_305 = root.findall(".//RelOp[@PhysicalOp='Clustered Index Scan']")
    table_scans_305 = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    # 合并结果
    scan_operations_305 = clustered_index_scans_305 + table_scans_305
    if scan_operations_305:
        print("警告: 查询中使用了扫描操作而不是查找操作，可能意味着缺少适当的索引。")

    # 规则 306: 检查Parallelism操作，可能意味着查询需要多个线程并行执行
    parallel_operations = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if parallel_operations:
        print("警告: 查询中存在Parallelism操作，这可能意味着查询需要多个线程并行执行，可能导致线程争用。")

    # 规则 307: 检查Compute Scalar操作，可能意味着查询中有复杂的计算
    compute_scalar_operations = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalar_operations:
        print("警告: 查询中存在Compute Scalar操作，考虑是否可以简化查询中的计算。")

    # 规则 308: 检查Nested Loops操作，这可能表示查询中有循环连接。
    nested_loops_operations = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if nested_loops_operations:
        print("警告: 查询中存在Nested Loops操作，这可能表示查询中有循环连接。考虑优化连接策略或使用索引。")

    # 规则 309: 检查Hash Match操作，这可能表示查询中有哈希连接。
    hash_match_operations = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_match_operations:
        print("警告: 查询中存在Hash Match操作，这可能表示查询中有哈希连接。考虑优化连接策略或使用索引。")

    # 规则 310: 检查Merge Join操作，这可能表示查询中有合并连接。
    merge_join_operations = root.findall(".//RelOp[@PhysicalOp='Merge Join']")
    if merge_join_operations:
        print("警告: 查询中存在Merge Join操作，这可能表示查询中有合并连接。考虑优化连接策略或使用索引。")

    # 规则 311: 检查Filter操作，可能意味着查询的过滤条件不高效
    filter_operations = root.findall(".//RelOp[@PhysicalOp='Filter']")
    if filter_operations:
        print("警告: 查询中存在Filter操作，考虑优化过滤条件或使用索引。")

    # 规则 312: 检查Constant Scan操作，这可能意味着查询中存在硬编码的常量值
    constant_scan_operations = root.findall(".//RelOp[@PhysicalOp='Constant Scan']")
    if constant_scan_operations:
        print("警告: 查询中存在Constant Scan操作，考虑避免使用硬编码的常量值。")

    # 规则 313: 检查Stream Aggregate操作，可能意味着查询中有聚合操作
    stream_aggregate_operations = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_aggregate_operations:
        print("警告: 查询中存在Stream Aggregate操作，考虑优化聚合或使用索引。")

    # 规则 314: 检查Compute Scalar操作，这可能表示查询正在计算某些值。
    compute_scalar_operations = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalar_operations:
        print("警告: 查询中存在Compute Scalar操作，这可能表示查询正在计算某些值。考虑优化查询或使用计算列。")

    # 规则 315: 检查Filter操作，可能意味着查询在获取数据后进行过滤。
    filter_operations = root.findall(".//RelOp[@PhysicalOp='Filter']")
    if filter_operations:
        print("警告: 查询中存在Filter操作，可能意味着查询在获取数据后进行过滤。考虑在数据获取前进行过滤，或优化查询条件。")

    # 规则 316: 检查Constant Scan操作，这可能表示查询从一个常量集合中获取数据。
    constant_scan_operations = root.findall(".//RelOp[@PhysicalOp='Constant Scan']")
    if constant_scan_operations:
        print("警告: 查询中存在Constant Scan操作，这可能表示查询从一个常量集合中获取数据。")

    # 规则 317: 检查Sequence Project操作，可能与窗口函数或其他排序操作有关。
    sequence_project_operations = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_project_operations:
        print("警告: 查询中存在Sequence Project操作，可能与窗口函数或其他排序操作有关。考虑优化查询。")

    # 规则 318: 检查Segment操作，可能与窗口函数或分段操作有关。
    segment_operations = root.findall(".//RelOp[@PhysicalOp='Segment']")
    if segment_operations:
        print("警告: 查询中存在Segment操作，可能与窗口函数或分段操作有关。考虑优化查询。")

    # 规则 319: 检查Assert操作，可能意味着查询正在验证某些条件。
    assert_operations = root.findall(".//RelOp[@PhysicalOp='Assert']")
    if assert_operations:
        print("警告: 查询中存在Assert操作，可能意味着查询正在验证某些条件。考虑优化查询或验证条件。")

    # 规则 320: 检查Sort操作，可能意味着查询正在排序数据
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_operations:
        print("警告: 查询中存在Sort操作，可能意味着查询正在排序数据。考虑优化或使用索引来避免排序。")

    # 规则 321: 检查Table Scan操作，可能意味着查询正在全表扫描
    table_scan_operations = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    if table_scan_operations:
        print("警告: 查询中存在Table Scan操作，可能意味着查询正在全表扫描。考虑优化或使用索引。")

    # 规则 322: 检查Table Spool操作，可能意味着查询正在使用临时表
    table_spool_operations = root.findall(".//RelOp[@PhysicalOp='Table Spool']")
    if table_spool_operations:
        print("警告: 查询中存在Table Spool操作，可能意味着查询正在使用临时表。考虑优化或避免使用临时表。")

    # 规则 323: 检查Table-valued function操作，可能影响性能
    tvf_operations = root.findall(".//RelOp[@PhysicalOp='Table-valued function']")
    if tvf_operations:
        print("警告: 查询中存在Table-valued function操作，可能影响性能。考虑优化或避免使用表值函数。")

    # 规则 324: 检查Union操作，可能意味着查询正在合并结果集
    union_operations = root.findall(".//RelOp[@PhysicalOp='Union']")
    if union_operations:
        print("警告: 查询中存在Union操作，可能意味着查询正在合并结果集。考虑优化或避免使用Union。")

    # 规则 325: 检查Update操作，可能意味着查询正在更新数据
    update_operations = root.findall(".//RelOp[@PhysicalOp='Update']")
    if update_operations:
        print("警告: 查询中存在Update操作，可能意味着查询正在更新数据。")

    # 规则 326: 检查Adaptive Join操作，可能意味着查询优化器在运行时选择了最佳的连接策略
    adaptive_join_operations = root.findall(".//RelOp[@PhysicalOp='Adaptive Join']")
    if adaptive_join_operations:
        print("警告: 查询中存在Adaptive Join操作，这可能意味着查询优化器在运行时选择了最佳的连接策略。")

    # 规则 327: 检查Bulk Insert操作，可能与插入大量数据有关
    bulk_insert_operations = root.findall(".//RelOp[@PhysicalOp='Bulk Insert']")
    if bulk_insert_operations:
        print("警告: 查询中存在Bulk Insert操作，考虑优化大量数据插入策略。")

    # 规则 328: 检查Columnstore Index Scan操作，可能与列存储索引扫描有关
    columnstore_index_scan_operations = root.findall(".//RelOp[@PhysicalOp='Columnstore Index Scan']")
    if columnstore_index_scan_operations:
        print("警告: 查询中存在Columnstore Index Scan操作，考虑优化列存储索引策略。")

    # 规则 329: 检查Concatenation操作，可能与多个数据集合连接有关
    concatenation_operations = root.findall(".//RelOp[@PhysicalOp='Concatenation']")
    if concatenation_operations:
        print("警告: 查询中存在Concatenation操作，考虑优化数据集合连接策略。")

    # 规则 330: 检查Full Outer Join操作，可能意味着查询中有全外连接
    full_outer_join_operations = root.findall(".//RelOp[@PhysicalOp='Full Outer Join']")
    if full_outer_join_operations:
        print("警告: 查询中存在Full Outer Join操作，考虑是否可以优化连接策略。")

    # 规则 331: 检查Parallelism操作，可能意味着查询试图并行执行，但可能受到某些限制
    parallelism_operations = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if parallelism_operations:
        print(
            "警告: 查询中存在Parallelism操作，这可能意味着查询试图并行执行，但可能受到某些限制。考虑优化并行策略或增加资源。")
    # 规则 332: 检查Bitmap操作，可能与哈希连接或某些索引扫描操作有关。
    bitmap_operations = root.findall(".//RelOp[@PhysicalOp='Bitmap']")
    if bitmap_operations:
        print("警告: 查询中存在Bitmap操作，可能与哈希连接或某些索引扫描操作有关。考虑优化连接策略或查询结构。")

    # 规则 333: 检查Table Spool操作，这可能表示查询正在缓存某些结果以供稍后使用。
    table_spool_operations = root.findall(".//RelOp[@PhysicalOp='Table Spool']")
    if table_spool_operations:
        print("警告: 查询中存在Table Spool操作，这可能表示查询正在缓存某些结果以供稍后使用。考虑优化查询或内存配置。")

    # 规则 334: 检查Window Spool操作，可能与窗口函数有关。
    window_spool_operations = root.findall(".//RelOp[@PhysicalOp='Window Spool']")
    if window_spool_operations:
        print("警告: 查询中存在Window Spool操作，可能与窗口函数有关。考虑优化窗口函数或查询结构。")

    # 规则 335: 检查计算与过滤的顺序
    # 如果在执行计算操作之前没有进行数据过滤，可能存在不必要的计算开销。
    compute_scalars = root.findall(".//ComputeScalar")
    if compute_scalars:
        for compute in compute_scalars:
            if not any(filter_op.tag == "Filter" for filter_op in compute.getparent()):
                print("警告: 在执行计算操作之前没有进行数据过滤，可能存在不必要的计算开销。")

    # 规则 336: 检查嵌套的存储过程或函数调用
    # 如果查询中有多个嵌套的存储过程或函数的调用，这可能是一个优化点。
    udf_calls = root.findall(".//UDF")
    if len(udf_calls) > 1:
        print("警告: 查询中存在多个嵌套或重复的存储过程或函数调用，考虑优化查询结构。")

    # 规则 337:在进行计算之前没有进行数据过滤:
    compute_scalars_337 = root.findall(".//ComputeScalar")
    filtered_compute_scalars_337 = [cs for cs in compute_scalars_337 if
                                    cs.find("preceding-sibling::Filter") is not None]
    if filtered_compute_scalars_337:
        print("警告: 在进行计算之前没有进行数据过滤。考虑先过滤数据以提高性能。")

    # 规则 338: 检查多次扫描大表：
    large_table_scans = root.findall(".//IndexScan[@Table='LargeTableName']")
    if len(large_table_scans) > 1:
        print("警告: 同一个大表被多次扫描。考虑优化查询。")

    # 规则 339: 检查同一个表的多次聚合:
    table_aggregates = root.findall(".//Aggregate[@Table='TableName']")
    if len(table_aggregates) > 1:
       print("警告: 同一个表有多次聚合操作。可能存在冗余。")

    # 规则 340:检查多层嵌套子查询:
    nested_subqueries = root.findall(".//Subquery[@Nested='True']")
    if len(nested_subqueries) > 2:
       print("警告: 存在多层嵌套子查询。考虑简化查询。")

    # 规则 341：检查多索引使用:
    index_scans = root.findall(".//IndexScan")
    if len(set([i.get('Index') for i in index_scans])) > 3:
       print("警告: 查询使用了多个不同的索引。考虑优化索引策略。")

    # 规则 342: 连接多大表无索引:
    large_table_joins = root.findall(".//Join[@Table='LargeTableName']")
    if large_table_joins and not index_scans:
       print("警告: 多个大表连接没有适当的索引支持。")

    # 规则 343: 检查数据流:
    filters_343 = root.findall(".//Filter")
    filtered_filters_343 = [f for f in filters_343 if f.find("following-sibling::Operation") is not None]
    if filtered_filters_343:
        print("警告: 先进行操作然后再过滤数据。考虑先过滤数据。")

    # 规则 344: 检查排序操作之后的数据过滤:
    filters_344 = root.findall(".//Filter")
    filtered_filters_344 = [f for f in filters_344 if f.find("preceding-sibling::Sort") is not None]
    if filtered_filters_344:
        print("警告: 先进行排序然后再过滤数据。考虑优化查询。")

    # 规则 345: 检查不必要的数据转换:
    conversions = root.findall(".//Convert[@Unnecessary='True']")
    if conversions:
       print("警告: 查询中存在不必要的数据转换。")

    # 规则 346: 检查是否有过多的物理I / O操作:
    physical_io_ops = root.findall(".//RelOp[@PhysicalIO='High']")
    if physical_io_ops:
       print("警告: 存在高物理I/O操作。考虑优化存储或查询。")

    #规则 347: 检查是否有大量的内存操作:
    memory_heavy_ops = root.findall(".//RelOp[@MemoryUsage='High']")
    if memory_heavy_ops:
       print("警告: 存在高内存使用操作。考虑优化查询或增加资源。")

    #复合型规则
    # 复合规则 1 : 检查复杂的表扫描与不同的过滤条件
    tables_scanned = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    filtered_tables = set()
    for table in tables_scanned:
        filters = table.findall(".//Filter")
        if filters:
            table_name = table.find(".//Object").get('Table')
            filtered_tables.add(table_name)
    if len(filtered_tables) > 1:
        print(
            f"警告: 查询中存在多次扫描的表{', '.join(filtered_tables)}，并且使用了不同的过滤条件，可能表示查询可以进一步优化。")

    # 复合规则 2: 检查缺乏索引的连接操作
    joins = []
    join_types = ['Nested Loops', 'Hash Match', 'Merge Join']
    for jt in join_types:
        joins.extend(root.findall(f".//RelOp[@PhysicalOp='{jt}']"))

    for join in joins:
        join_cols = join.findall(".//ColumnReference")
        if join_cols:
            for col in join_cols:
                if not col.get('IsIndexed'):
                    print(f"警告: 连接操作中的字段{col.get('Column')}没有被索引，可能导致性能问题。")

    # 复合规则 3: 检查排序后的连接
    sorts = root.findall(".//RelOp[@PhysicalOp='Sort']")
    for sort in sorts:
        order_by_cols = sort.findall(".//OrderBy")
        next_op = sort.getnext()
        if next_op.tag == 'RelOp' and next_op.get('PhysicalOp') in ['Nested Loops', 'Hash Match', 'Merge Join']:
            join_cols = next_op.findall(".//ColumnReference")
            if not any(col.get('Column') for col in join_cols if
                       col.get('Column') in [ob.get('Column') for ob in order_by_cols]):
                print(
                    "警告: 在一个排序操作之后直接跟随一个连接操作，而排序的字段并不是连接的字段，可能意味着排序可以在查询的后期进行以提高效率。")

    # 复合规则 4：嵌套循环连接后跟排序操作:
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops Join']")
    for nl in nested_loops:
        next_sibling = nl.getnext()  # 获取下一个相邻元素
        if next_sibling is not None and next_sibling.get('PhysicalOp') == 'Sort':
            print("警告: 嵌套循环连接后直接跟排序操作。考虑优化连接或排序策略。")

    # 复合规则 5：同一个表的多次不同方式连接:
    table_name = 'TableName'
    # 获取所有连接操作
    joins = root.findall(".//RelOp[@Table='{}']".format(table_name))
    join_types = set([join.get("PhysicalOp") for join in joins])

    # 检查是否有多于一种连接方式
    if any(jtype in join_types for jtype in ['Nested Loops Join', 'Hash Match Join', 'Merge Join']):
        if len(join_types) > 1:
            print(f"警告: 表 {table_name} 被多次以不同方式连接。考虑统一连接策略。")

    # 复合规则 6: 使用了多个索引但没有聚合操作:
    index_scans = root.findall(".//IndexScan")
    distinct_indexes = set([i.get('Index') for i in index_scans])
    aggregates = root.findall(".//Aggregate")

    if len(distinct_indexes) > 2 and not aggregates:
        print("警告: 查询使用了多个索引但没有聚合操作。可能存在索引冗余。")

    # 复合规则 7: 检查多表连接的执行顺序
    hash_joins = root.findall(".//RelOp[@PhysicalOp='Hash Join']")
    merge_joins = root.findall(".//RelOp[@PhysicalOp='Merge Join']")
    joins = hash_joins + merge_joins

    for join in joins:
        tables_in_join = [table.get('Table') for table in join.findall(".//TableScan")]
        if 'LargeTable1' in tables_in_join and 'SmallTable1' in tables_in_join:
            print("警告: 考虑更改连接顺序以优化性能。")

    # 复合规则 8: 检查多个相似的索引扫描或查找
    index_scans = root.findall(".//RelOp[@PhysicalOp='Index Scan']")
    index_seeks = root.findall(".//RelOp[@PhysicalOp='Index Seek']")
    all_index_operations = index_scans + index_seeks
    if len(all_index_operations) > 2:  # 假设相同的索引操作出现超过2次
        print("警告: 查询中存在多个相似的索引操作，考虑重写查询或优化索引。")

    # 复合规则 9: 检查多个大表的交叉连接
    cross_joins = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    cross_joins = [join for join in cross_joins if join.get('LogicalOp') == 'Cross Join']

    if cross_joins:
        # 检查这些交叉连接操作是否涉及到大表
        large_tables_involved = [join for join in cross_joins if int(join.find('.//EstimatedRows').text) > 1000000]
        if large_tables_involved:
            print("警告: 查询中存在多个大表的交叉连接，可能导致性能问题。")

    # 复合规则 10: 检查多阶段聚合
    aggregates = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    aggregates = [agg for agg in aggregates if agg.get('LogicalOp') == 'Aggregate']

    if len(aggregates) > 1:
        print("警告: 查询中存在多阶段聚合操作，可能导致性能问题。")

    # 复合规则 11: 检查同一表的多个索引扫描
    index_scans = root.findall(".//RelOp[@PhysicalOp='Index Scan']")
    tables_with_multiple_scans = set()
    for scan in index_scans:
        table_name = scan.find(".//Object[@Table]").get('Table')
        if table_name in tables_with_multiple_scans:
            print(f"警告: 表 {table_name} 被多次扫描，可能导致性能问题。")
        else:
            tables_with_multiple_scans.add(table_name)

    # 复合规则 12: 高开销操作与低选择性过滤器的组合
    high_cost_ops_table_scan = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    high_cost_ops_index_scan = root.findall(".//RelOp[@PhysicalOp='Clustered Index Scan']")
    high_cost_ops = high_cost_ops_table_scan + high_cost_ops_index_scan

    if high_cost_ops:
        low_selectivity_filters = [f for f in root.findall(".//Filter") if
                                   float(f.get('Selectivity', '1')) < 0.1]  # 假设过滤选择性小于10%为低选择性
        if low_selectivity_filters:
            print(
                "警告: 查询中存在高开销操作与低选择性过滤器的组合。考虑优化查询。提示: 确保你的查询中没有隐式转换，它可能会影响性能。")

    # 复合规则 13: 多次使用相同的昂贵子查询
    expensive_subqueries = root.findall(".//Subquery[@Cost>0.5]")  # 假设成本大于0.5为昂贵子查询
    if len(expensive_subqueries) > 1:
        print("警告: 查询中多次引用了相同的、计算成本高的子查询。考虑使用CTE或临时表优化。")

    # 复合规则 14: 排序操作后的缺失索引
    sort_ops = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_ops:
        post_sort_ops_without_index = sort_ops[0].findall(".//RelOp[not(@Index)]")
        if post_sort_ops_without_index:
            print("警告: 在排序操作之后进行了需要索引的操作，但相关字段没有索引。考虑添加索引。")

    # 复合规则 15: 过度的并行操作
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    if len(parallel_ops) > 64:  # 假设CPU有64个核心
        print("警告: 查询中有过多的并行操作。考虑调整并行策略。")

    # 复合规则 16: 检查排序和连接的顺序是否优化
    sort_elements = root.findall(".//RelOp[@PhysicalOp='Sort']")

    for sort_element in sort_elements:
        next_sibling = sort_element.getnext()  # 获取下一个同级元素
        if next_sibling is not None and next_sibling.get('PhysicalOp') in ['Merge Join', 'Hash Match Join']:
            print("警告: 排序和连接的顺序可能未优化。考虑调整查询。")
            break

    # 复合规则 17: 检查是否有多个全表扫描
    full_table_scans = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    if len(full_table_scans) > 1:
        print("警告: 查询中存在多个全表扫描，可能导致性能下降。考虑添加索引。")

    # 复合规则 18: 检查是否有多个远程查询
    remote_queries = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if len(remote_queries) > 2:
        print("警告: 查询中存在多个远程查询操作，可能导致网络开销增加。考虑优化远程查询。")

    # 复合规则 19: 检查多个计算与过滤的顺序
    compute_elements = root.findall(".//ComputeScalar")

    # 为每个ComputeScalar元素找到后续的Filter元素
    compute_followed_by_filters = [compute for compute in compute_elements if
                                   compute.getnext() is not None and compute.getnext().tag == 'Filter']

    if len(compute_followed_by_filters) > 2:
        print("警告: 查询中存在多个计算后跟过滤的操作。考虑优化计算与过滤的顺序。")

    # 复合规则 20: 检查多个分区操作与索引扫描的组合
    partition_operations = root.findall(".//RelOp[@PhysicalOp='Partition']")
    index_scans_after_partition = [p.findall(".//IndexScan") for p in partition_operations]
    if any(index_scans_after_partition):
        print("警告: 查询中存在多个分区操作与索引扫描的组合。考虑优化分区策略。")

    # 复合规则 21: 检查多个嵌套子查询的使用
    def find_nested_subqueries(element, depth=0):
        """
        递归方法来查找嵌套子查询。

        参数:
        - element: 当前正在检查的XML元素。
        - depth: 当前元素的深度，默认为0。

        返回:
        - 嵌套子查询的列表。
        """
        # 如果当前元素是子查询并且深度大于0，则返回该元素
        if element.tag == "Subquery" and depth > 0:
            return [element]

        nested_subqueries_found = []
        # 递归查找所有子元素
        for child in element:
            nested_subqueries_found.extend(find_nested_subqueries(child, depth + 1))

        return nested_subqueries_found

    # 使用上述函数来查找嵌套子查询
    nested_subqueries = find_nested_subqueries(root)
    if len(nested_subqueries) > 2:
        print("警告: 查询中存在多个嵌套子查询的使用。考虑优化子查询结构。")

    # 复合规则 22: 当查询同时包含多个高成本操作时警告
    high_cost_operations = root.findall(".//RelOp[@EstimatedTotalSubtreeCost>10]")  # 假设10为高成本阈值
    if len(high_cost_operations) > 2:
        print("警告: 查询中存在多个高成本操作。")

    # 复合规则 23: 当查询中存在多次对同一表的访问但使用了不同的索引时警告
    tables_with_multiple_indexes = set()
    index_scans = root.findall(".//IndexScan")
    for i in index_scans:
        table = i.get('Table')
        index = i.get('Index')
        if table in tables_with_multiple_indexes:
            print(f"警告: 表 {table} 被多次访问并使用了不同的索引 {index}。")
        tables_with_multiple_indexes.add(table)

    # 复合规则 24: 当查询有多个排序操作，并且它们不是在查询的末尾进行时警告
    sorts = root.findall(".//Sort")
    for sort in sorts:
        following_nodes = sort.findall(".//following-sibling::RelOp")
        if following_nodes:
            print("警告: 查询中存在一个排序操作，后面还有其他操作。")

    # 复合规则 25: 当查询在没有过滤条件的情况下对多个大表进行连接时警告
    def joins_without_filter(element):
        """
        递归方法来查找没有过滤条件的连接操作。

        参数:
        - element: 当前正在检查的XML元素。

        返回:
        - 没有过滤条件的连接操作的列表。
        """
        join_ops = ['Nested Loops Join', 'Merge Join', 'Hash Match Join']
        if element.tag == "RelOp" and element.get('PhysicalOp') in join_ops:
            # 查找Filter子元素
            filter_child = element.find(".//Filter")
            if filter_child is None:
                return [element]

        joins_found = []
        # 递归查找所有子元素
        for child in element:
            joins_found.extend(joins_without_filter(child))

        return joins_found

    # 使用上述函数来查找没有过滤条件的连接操作
    joins_without_filters = joins_without_filter(root)
    if len(joins_without_filters) > 1:
        print("警告: 查询中存在多个连接操作但没有相应的过滤条件。")

    # 复合规则 26: 当查询使用了多个全表扫描操作时警告
    full_table_scans = root.findall(".//RelOp[@PhysicalOp='Table Scan']")
    if len(full_table_scans) > 1:
        print("警告: 查询中使用了多个全表扫描操作。")

    # 复合规则 27: 当一个查询内部存在多次对同一存储过程或函数的调用时警告
    udf_calls = root.findall(".//RelOp[@NodeType='UDF']")
    stored_proc_calls = root.findall(".//RelOp[@NodeType='StoredProc']")

    proc_or_func_calls = udf_calls + stored_proc_calls

    if len(proc_or_func_calls) > 1:
        print("警告: 查询内部存在多次对同一存储过程或函数的调用。")

    # 复合规则 28: 当查询中有多个并行操作，但CPU利用率低时警告
    parallel_operations = root.findall(".//RelOp[@Parallel='1']")
    if len(parallel_operations) > 2:  # 假设存在超过2个并行操作
        print("警告: 查询中存在多个并行操作，可能导致CPU资源未充分利用。")

    # 复合规则 29: 当查询中存在多个递归操作时警告
    recursive_operations = root.findall(".//RelOp[@NodeType='Recursive']")
    if recursive_operations:
        print("警告: 查询中存在多个递归操作，可能导致性能问题。")

    # 复合规则 30: 当查询中存在多个嵌套子查询时警告
    nested_subqueries = root.findall(".//SubQuery")
    if len(nested_subqueries) > 2:  # 假设存在超过2个嵌套子查询
        print("警告: 查询中存在多个嵌套子查询，可能导致性能问题。")

    # 复合规则 31: 当查询中存在多个重复的连接条件时警告
    join_conditions = {}

    nested_loops_joins = root.findall(".//RelOp[@PhysicalOp='Nested Loops Join']")
    merge_joins = root.findall(".//RelOp[@PhysicalOp='Merge Join']")
    hash_match_joins = root.findall(".//RelOp[@PhysicalOp='Hash Match Join']")

    joins = nested_loops_joins + merge_joins + hash_match_joins

    for join in joins:
        condition = join.find(".//Predicate").text  # 假设这样可以获得连接条件
        if condition in join_conditions:
            print(f"警告: 查询中存在重复的连接条件: {condition}。")
        else:
            join_conditions[condition] = True

    # 复合规则 32: 当一个大表与多个小表进行连接时警告
    big_table = 'BigTableName'
    nested_loops_joins = root.findall(f".//RelOp[@Table='{big_table}'][@PhysicalOp='Nested Loops Join']")
    merge_joins = root.findall(f".//RelOp[@Table='{big_table}'][@PhysicalOp='Merge Join']")
    hash_match_joins = root.findall(f".//RelOp[@Table='{big_table}'][@PhysicalOp='Hash Match Join']")

    big_table_joins = nested_loops_joins + merge_joins + hash_match_joins

    if len(big_table_joins) > 2:
        print(f"警告: 大表 {big_table} 与多个小表进行了连接。")

    # 复合规则 33: 检查是否有多个昂贵的排序操作
    expensive_sorts = root.findall(".//Sort[@EstimatedTotalSubtreeCost>5]")  # 假设5为昂贵的阈值
    if len(expensive_sorts) > 1:
        print("警告: 查询中存在多个昂贵的排序操作。")

    # 复合规则 34: 当有多个子查询在同一个级别时警告
    all_subqueries = root.findall(".//SubQuery")
    direct_subqueries = [sq for sq in all_subqueries if not sq.findall(".//SubQuery")]

    if len(direct_subqueries) > 2:  # 假设直接子查询超过2个
        print("警告: 查询中在同一级别存在多个子查询。")

    # 复合规则 35: 当有多个CTE（公共表达式）在查询中时警告
    ctes = root.findall(".//CommonTableExpression")
    if len(ctes) > 2:  # 假设有超过2个CTE
        print("警告: 查询中存在多个公共表达式（CTE）。")

    # 复合规则 36: 当一个查询中既有并行操作又有串行操作时警告  #规则之前可以考虑进一步把考虑写的更详细。
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    serial_ops = root.findall(".//RelOp[@Parallel='0']")
    if parallel_ops and serial_ops:
        print("警告: 查询中同时存在并行和串行操作。混合使用可能导致不稳定的查询性能。考虑检查并行设置和适当调整查询。")

    # 复合规则 37: 检查是否存在多个非聚集索引扫描
    index_scans = root.findall(".//RelOp[@PhysicalOp='Index Scan']")
    non_clustered_index_scans = [scan for scan in index_scans if 'CLUSTERED' not in scan.get('Index', '')]

    if len(non_clustered_index_scans) > 1:
        print(
            "警告: 查询中存在多个非聚集索引扫描。可能存在重叠的索引列，这可能导致性能下降和资源浪费。考虑合并或删除冗余索引。")

    # 复合规则 38: 检查是否使用了LIKE操作符与通配符开始的字符串
    scalar_ops = root.findall(".//ScalarOperator")
    like_wildcard_scans = [op for op in scalar_ops if 'LIKE [%' in (op.text or '')]

    if like_wildcard_scans:
        print("警告: 查询中使用了LIKE操作符与通配符开始的字符串，这会导致全索引扫描，影响查询效率。考虑避免使用通配符开头的LIKE模式。")

    # 复合规则 39: 检查查询是否涉及大量数据的排序
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    large_sort_operations = [op for op in sort_operations if float(op.get('EstimatedRows', 0)) > 10000]

    if large_sort_operations:
        print("警告: 查询中存在大量数据的排序操作，这可能导致大量内存使用和性能下降。考虑优化查询或使用索引来帮助排序。")


    # 复合规则 40: 检查查询中是否使用了计算列
    computed_columns = []
    for defined_value in root.findall(".//DefinedValue"):
        scalar_operator = defined_value.find(".//ScalarOperator")
        if scalar_operator is not None and 'COMPUTE SCALAR' in scalar_operator.text:
            computed_columns.append(defined_value)

    if computed_columns:
        print("警告: 查询中使用了计算列，每次查询时都会重新计算，可能导致性能下降。考虑预先计算或使用持久化计算列。")


    # 复合规则 41: 检查查询中是否过多地使用了OR操作符
    or_operations_count = 0

    # 寻找所有ScalarOperator元素
    scalar_operators = root.findall(".//ScalarOperator")
    for operator in scalar_operators:
        # 如果元素的文本包含'OR'，则增加计数
        if 'OR' in operator.text:
            or_operations_count += 1

    if or_operations_count > 2:
        print(
            "警告: 查询中过多地使用了OR操作符，这可能导致查询计划不优化和全表扫描。考虑将OR条件分解为多个简单查询并使用UNION。")

    # 复合规则 42: 检查查询中是否使用了NOT IN或NOT EXISTS
    found_not_in_or_exists = False

    # 寻找所有ScalarOperator元素
    scalar_operators = root.findall(".//ScalarOperator")
    for operator in scalar_operators:
        if 'NOT IN' in operator.text or 'NOT EXISTS' in operator.text:
            found_not_in_or_exists = True
            break

    if found_not_in_or_exists:
        print("警告: 使用NOT IN或NOT EXISTS可能导致全表扫描，影响性能。考虑使用左连接或其他方法替代。")

    print("执行计划审计完成。")




















