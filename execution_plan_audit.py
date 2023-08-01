import pyodbc
import xml.etree.ElementTree as ET

def get_execution_plan(conn, query):
    # 使用 "SET SHOWPLAN_XML ON" 命令获取XML格式的执行计划
    set_showplan_cmd = "SET SHOWPLAN_XML ON;"
    cursor = conn.cursor()
    cursor.execute(set_showplan_cmd)
    cursor.execute(query)
    
    # 获取执行计划
    plan = cursor.fetchone()[0]
    cursor.close()

    # 在获取计划后重置SHOWPLAN_XML为OFF
    set_showplan_off_cmd = "SET SHOWPLAN_XML OFF;"
    cursor = conn.cursor()
    cursor.execute(set_showplan_off_cmd)
    cursor.close()
    
    return plan


def audit_execution_plan(plan):
    # 解析XML格式的执行计划
    root = ET.fromstring(plan)
    # Rule 1: 检查全表扫描
    full_scans = root.findall(".//TableScan[not(Index)]")
    if full_scans:
        print("警告: 查询中存在全表扫描，可能影响性能。")

    # Rule 2: 检查缺失的索引
    missing_indexes = root.findall(".//MissingIndex")
    if missing_indexes:
        print("提示: 查询可能受益于添加以下索引:")
        for index in missing_indexes:
            columns = ', '.join([col.get('Name') for col in index.findall(".//ColumnGroup/Column")])
            print(f"表: {index.get('Table')} - 列: {columns}")

    # Rule 3: 检查预估的行数与实际行数的偏差
    discrepancies = root.findall(".//RelOp[EstimatedRows!=ActualRows]")
    if discrepancies:
        print("警告: 预估的行数与实际行数有较大偏差，可能需要更新统计信息。")

    # Rule 4: 检查并行查询
    parallel_queries = root.findall(".//QueryPlan[DegreeOfParallelism>1]")
    if parallel_queries:
        print("警告: 查询并行执行，可能导致资源争用。")

    # Rule 5: 检查排序操作
    sort_ops = root.findall(".//Sort")
    if sort_ops:
        print("警告: 查询中存在排序操作，可能影响性能。")

    # Rule 6: 检查哈希匹配
    hash_matches = root.findall(".//HashMatch")
    if hash_matches:
        print("警告: 查询中存在哈希匹配，可能需要大量内存。")

    # Rule 7: 检查昂贵的操作
    high_cost_ops = root.findall(".//RelOp[@EstimatedTotalSubtreeCost>10]")  # Arbitrary threshold
    if high_cost_ops:
        print("警告: 查询中存在昂贵的操作，考虑优化查询。")

    # Rule 8: 检查子查询
    subqueries = root.findall(".//Subquery")
    if subqueries:
        print("警告: 子查询可能不如连接效率。")

    # Rule 9: 检查悬挂的索引扫描
    hanging_index_scans = root.findall(".//IndexScan[not(SeekPredicates)]")
    if hanging_index_scans:
        print("警告: 存在悬挂的索引扫描，可能意味着查询条件没有被索引覆盖。")

    # Rule 10: 检查内存或磁盘溢出
    spills = root.findall(".//SpillToTempDb")
    if spills:
        print("警告: 查询中存在内存或磁盘溢出，可能影响性能。")

    # Rule 11: 检查嵌套循环
    nested_loops = root.findall(".//NestedLoop")
    if nested_loops:
        print("警告: 查询中存在嵌套循环，可能在大数据集上效率低下。")

    # Rule 12: 检查是否使用了RIDGE JOIN
    ridge_joins = root.findall(".//RidgeJoin")
    if ridge_joins:
        print("警告: 查询中使用了RIDGE JOIN，可能影响性能。")

    # Rule 13: 检查非聚集索引的使用
    non_clustered_indexes = root.findall(".//IndexScan[not(Clustered)]")
    if non_clustered_indexes:
        print("警告: 使用非聚集索引可能导致更多的磁盘I/O。")

    # Rule 14: 检查转换或隐式转换
    conversions = root.findall(".//Convert")
    if conversions:
        print("警告: 查询中存在数据类型转换或隐式转换，可能会阻止索引的使用。")

    # Rule 15: 检查大量的数据移动
    data_movements = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if data_movements:
        print("警告: 查询中存在大量的数据移动，如数据溢出到磁盘。")

    # Rule 16: 检查任何警告
    warnings = root.findall(".//Warnings")
    if warnings:
        print("警告: 查询中存在警告，例如关于过时的统计信息。")

    # Rule 17: 检查连接的顺序
    wrong_order_joins = root.findall(".//RelOp[@LogicalOp='Inner Join'][@PhysicalOp!='Nested Loops']")
    if wrong_order_joins:
        print("警告: 连接的顺序可能不正确，可能影响性能。")

    # Rule 18: 检查并行操作限制
    parallelism_restrictions = root.findall(".//QueryPlan[@NonParallelPlanReason='MaxDOPSetToOne']")
    if parallelism_restrictions:
        print("警告: 查询被限制为单线程执行，可能影响性能。")

    # Rule 19: 检查过多的物理读取
    excessive_physical_reads = root.findall(".//RelOp[PhysicalReads>1000]")
    if excessive_physical_reads:
        print("警告: 过多的物理读取可能意味着缺少索引或统计信息过时。")

    # Rule 20: 检查大量的逻辑读取
    excessive_logical_reads = root.findall(".//RelOp[LogicalReads>1000]")
    if excessive_logical_reads:
        print("警告: 过多的逻辑读取可能影响性能。")

    # Rule 21: 检查使用表变量
    table_vars = root.findall(".//TableValuedFunction")
    if table_vars:
        print("警告: 使用表变量可能导致不准确的统计信息。")

    # Rule 22: 检查使用OPTION (FORCE ORDER)
    force_orders = root.findall(".//Hint[@Type='FORCE ORDER']")
    if force_orders:
        print("警告: 使用OPTION (FORCE ORDER)可能导致非最佳的查询计划。")

    # Rule 23: 检查使用OPTION (OPTIMIZE FOR UNKNOWN)
    optimize_for_unknowns = root.findall(".//Hint[@Type='OPTIMIZE FOR UNKNOWN']")
    if optimize_for_unknowns:
        print("警告: 使用OPTION (OPTIMIZE FOR UNKNOWN)可能导致非最佳的查询计划。")

    # Rule 24: 检查缺少统计信息
    missing_stats = root.findall(".//MissingStatistics")
    if missing_stats:
        print("警告: 执行计划中缺少统计信息。")

    # Rule 25: 检查过早的物化
    early_materializations = root.findall(".//EarlyMaterialization")
    if early_materializations:
        print("警告: 过早的物化可能导致不必要的I/O。")

    # Rule 26: 检查多余的索引扫描
    redundant_index_scans = root.findall(".//IndexScan[@Lookup='true']")
    if redundant_index_scans:
        print("警告: 多余的索引扫描可能影响性能。")

    # Rule 27: 检查没有被使用的索引
    unused_indexes = root.findall(".//UnusedIndex")
    if unused_indexes:
        print("警告: 查询中存在没有被使用的索引。")

    # Rule 28: 检查没有被推送的谓词
    unpushed_predicates = root.findall(".//UnpushedPredicate")
    if unpushed_predicates:
        print("警告: 谓词没有被推送到存储引擎，可能导致额外的I/O。")

    # Rule 29: 检查不支持的物理操作符
    unsupported_physical_ops = root.findall(".//UnsupportedPhysicalOp")
    if unsupported_physical_ops:
        print("警告: 执行计划中使用了不支持的物理操作符。")

    # Rule 30: 检查执行计划中的超时
    timeouts = root.findall(".//Timeout")
    if timeouts:
        print("警告: 执行计划中存在超时。")

    # Rule 31: 检查重复的谓词
    redundant_predicates = root.findall(".//RedundantPredicate")
    if redundant_predicates:
        print("警告: 查询中存在重复的谓词，可能影响性能。")

    # Rule 32: 检查不必要的远程查询
    unnecessary_remote_queries = root.findall(".//RemoteQuery[@Unnecessary='true']")
    if unnecessary_remote_queries:
        print("警告: 执行计划中存在不必要的远程查询。")

    # Rule 33: 检查不必要的列
    unnecessary_columns = root.findall(".//OutputList/ColumnReference[@Unnecessary='true']")
    if unnecessary_columns:
        print("警告: 查询返回了不必要的列，可能影响性能。")

    # Rule 34: 检查低效的数据类型转换
    inefficient_conversions = root.findall(".//Convert[@Implicit='false']")
    if inefficient_conversions:
        print("警告: 查询中存在低效的数据类型转换。")

    # Rule 35: 检查分区切换
    partition_switches = root.findall(".//PartitionSwitch")
    if partition_switches:
        print("警告: 执行计划中存在分区切换，可能导致额外的I/O。")

    # Rule 36: 检查低效的TOP操作符
    inefficient_tops = root.findall(".//Top[@Percent='false'][@WithTies='true']")
    if inefficient_tops:
        print("警告: 查询中使用了低效的TOP操作符。")

    # Rule 37: 检查未使用的表
    unused_tables = root.findall(".//RelOp[@EstimateRows=0]")
    if unused_tables:
        print("警告: 查询中存在未使用的表，可能影响性能。")

    # Rule 38: 检查不必要的DISTINCT
    unnecessary_distincts = root.findall(".//Distinct")
    if unnecessary_distincts:
        print("警告: 查询中使用了不必要的DISTINCT，可能影响性能。")

    # Rule 39: 检查不必要的ORDER BY
    unnecessary_order_bys = root.findall(".//OrderBy")
    if unnecessary_order_bys:
        print("警告: 查询中使用了不必要的ORDER BY，可能影响性能。")

    # Rule 40: 检查低效的数据排序
    inefficient_data_orders = root.findall(".//Sort[@PhysicalOp='Parallelism']")
    if inefficient_data_orders:
        print("警告: 查询中存在低效的数据排序，可能影响性能。")

    # Rule 41: 检查不必要的数据合并
    unnecessary_data_merges = root.findall(".//MergeJoin[@ManyToMany='true']")
    if unnecessary_data_merges:
        print("警告: 查询中存在不必要的数据合并，可能影响性能。")

    # Rule 42: 检查不必要的数据串联
    unnecessary_data_concats = root.findall(".//Concatenation[@Unordered='true']")
    if unnecessary_data_concats:
        print("警告: 查询中存在不必要的数据串联，可能影响性能。")

    # Rule 43: 检查不必要的数据分割
    unnecessary_data_splits = root.findall(".//Split")
    if unnecessary_data_splits:
        print("警告: 查询中存在不必要的数据分割，可能影响性能。")

    # Rule 44: 检查低效的数据压缩
    inefficient_data_compressions = root.findall(".//ComputeScalar[@Define='Compression']")
    if inefficient_data_compressions:
        print("警告: 查询中存在低效的数据压缩，可能影响性能。")

    # Rule 45: 检查低效的数据解压缩
    inefficient_data_decompressions = root.findall(".//ComputeScalar[@Define='Decompression']")
    if inefficient_data_decompressions:
        print("警告: 查询中存在低效的数据解压缩，可能影响性能。")

    # Rule 46: 检查数据溢出
    data_overflows = root.findall(".//RelOp[@EstimateRebinds>0]")
    if data_overflows:
        print("警告: 执行计划中存在数据溢出，可能导致额外的I/O。")

    # Rule 47: 检查使用窗口函数
    window_functions = root.findall(".//WindowFunction")
    if window_functions:
        print("警告: 查询中使用了窗口函数，可能影响性能。")

    # Rule 48: 检查昂贵的子查询操作
    subquery_ops = root.findall(".//Subquery")
    if subquery_ops:
        print("警告: 查询中存在昂贵的子查询操作，可能影响性能。")

    # Rule 49: 检查过多的嵌套查询
    nested_queries = root.findall(".//NestedLoop")
    if nested_queries:
        print("警告: 查询中存在过多的嵌套查询，可能影响性能。")

    # Rule 50: 检查昂贵的递归CTE操作
    recursive_cte_ops = root.findall(".//RecursiveCTE")
    if recursive_cte_ops:
        print("警告: 查询中存在昂贵的递归CTE操作，可能影响性能。")

    # Rule 51: 检查昂贵的全文搜索操作
    fulltext_search_ops = root.findall(".//FullTextSearch")
    if fulltext_search_ops:
        print("警告: 查询中存在昂贵的全文搜索操作，可能影响性能。")

    # Rule 52: 检查表变量没有统计信息
    table_variable_stats = root.findall(".//TableVariableWithoutStats")
    if table_variable_stats:
        print("警告: 表变量没有统计信息，可能影响查询优化器的决策。")

    # Rule 53: 检查分区视图
    partitioned_views = root.findall(".//PartitionedView")
    if partitioned_views:
        print("警告: 分区视图可能导致性能问题。")

    # Rule 54: 检查不必要的自连接
    self_joins = root.findall(".//Join[@SelfJoin='true']")
    if self_joins:
        print("警告: 查询中存在不必要的自连接，可能影响性能。")

    # Rule 55: 检查索引与数据的物理分离
    index_data_disparities = root.findall(".//IndexScan[@PhysicalOp='Remote']")
    if index_data_disparities:
        print("警告: 索引与其数据在物理上是分开的，可能导致额外的I/O。")

    # Rule 56: 检查低效的外连接
    inefficient_outer_joins = root.findall(".//OuterJoin[@PhysicalOp='Hash']")
    if inefficient_outer_joins:
        print("警告: 使用哈希操作的外连接可能不如其他类型的连接效率。")

    # Rule 57: 检查未解决的查询提示
    unresolved_query_hints = root.findall(".//UnresolvedHint")
    if unresolved_query_hints:
        print("警告: 查询中存在未解决的查询提示。")

    # Rule 58: 检查使用了过时的查询提示
    deprecated_query_hints = root.findall(".//DeprecatedHint")
    if deprecated_query_hints:
        print("警告: 查询中使用了过时的查询提示。")

    # Rule 59: 检查昂贵的动态SQL操作
    dynamic_sql_ops = root.findall(".//DynamicSQL")
    if dynamic_sql_ops:
        print("警告: 查询中存在昂贵的动态SQL操作，可能影响性能。")

    # Rule 60: 检查昂贵的递归查询
    recursive_queries = root.findall(".//RecursiveQuery")
    if recursive_queries:
        print("警告: 查询中存在昂贵的递归查询，可能影响性能。")

    # Rule 61: 检查未使用的表别名
    unused_table_aliases = root.findall(".//UnusedAlias")
    if unused_table_aliases:
        print("警告: 查询中存在未使用的表别名，可能导致查询难以理解。")

    # Rule 62: 检查昂贵的列存储扫描
    columnstore_scans = root.findall(".//ColumnstoreScan")
    if columnstore_scans:
        print("警告: 查询中存在昂贵的列存储扫描，可能影响性能。")

    # Rule 63: 检查昂贵的列存储索引操作
    columnstore_index_ops = root.findall(".//ColumnstoreIndex")
    if columnstore_index_ops:
        print("警告: 查询中存在昂贵的列存储索引操作，可能影响性能。")

    # Rule 64: 检查使用了大量的内存的操作
    high_memory_ops = root.findall(".//RelOp[@Memory>5000]")  # Arbitrary threshold
    if high_memory_ops:
        print("警告: 查询中存在使用了大量内存的操作，可能影响性能。")

    # Rule 65: 检查使用了大量的CPU的操作
    high_cpu_ops = root.findall(".//RelOp[@CPU>1000]")  # Arbitrary threshold
    if high_cpu_ops:
        print("警告: 查询中存在使用了大量CPU的操作，可能影响性能。")

    # Rule 66: 检查低效的数据聚合
    inefficient_aggregations = root.findall(".//Aggregate[@Strategy='Hash']")
    if inefficient_aggregations:
        print("警告: 查询中存在低效的数据聚合，可能影响性能。")

    # Rule 67: 检查在大数据集上的嵌套循环
    large_data_set_loops = root.findall(".//NestedLoop[@LargeDataSet='true']")
    if large_data_set_loops:
        print("警告: 查询在大数据集上使用嵌套循环，可能影响性能。")

    # Rule 68: 检查不必要的数据复制
    data_copies = root.findall(".//Copy")
    if data_copies:
        print("警告: 查询中存在不必要的数据复制，可能影响性能。")

    # Rule 69: 检查昂贵的数据插入
    expensive_inserts = root.findall(".//Insert")
    if expensive_inserts:
        print("警告: 查询中存在昂贵的数据插入操作，可能影响性能。")

    # Rule 70: 检查昂贵的数据更新
    expensive_updates = root.findall(".//Update")
    if expensive_updates:
        print("警告: 查询中存在昂贵的数据更新操作，可能影响性能。")

    # Rule 71: 检查昂贵的数据删除
    expensive_deletes = root.findall(".//Delete")
    if expensive_deletes:
        print("警告: 查询中存在昂贵的数据删除操作，可能影响性能。")

    # Rule 72: 检查昂贵的数据合并
    expensive_merges = root.findall(".//Merge")
    if expensive_merges:
        print("警告: 查询中存在昂贵的数据合并操作，可能影响性能。")

    # Rule 73: 检查不必要的数据转换
    unnecessary_conversions = root.findall(".//Convert")
    if unnecessary_conversions:
        print("警告: 查询中存在不必要的数据转换，可能影响性能。")

    # Rule 74: 检查数据转换的错误
    conversion_errors = root.findall(".//Convert[@Error='true']")
    if conversion_errors:
        print("警告: 查询中的数据转换存在错误。")

    # Rule 75: 检查不必要的数据连接
    unnecessary_data_links = root.findall(".//DataLink")
    if unnecessary_data_links:
        print("警告: 查询中存在不必要的数据连接，可能影响性能。")

    # Rule 76: 检查不必要的数据流
    unnecessary_data_streams = root.findall(".//Stream")
    if unnecessary_data_streams:
        print("警告: 查询中存在不必要的数据流，可能影响性能。")

    # Rule 77: 检查数据流的错误
    stream_errors = root.findall(".//Stream[@Error='true']")
    if stream_errors:
        print("警告: 查询中的数据流存在错误。")

    # Rule 78: 检查昂贵的数据分区操作
    expensive_partitions = root.findall(".//Partition")
    if expensive_partitions:
        print("警告: 查询中存在昂贵的数据分区操作，可能影响性能。")

    # Rule 79: 检查昂贵的数据压缩操作
    expensive_compressions = root.findall(".//Compression")
    if expensive_compressions:
        print("警告: 查询中存在昂贵的数据压缩操作，可能影响性能。")

    # Rule 80: 检查昂贵的数据解压缩操作
    expensive_decompressions = root.findall(".//Decompression")
    if expensive_decompressions:
        print("警告: 查询中存在昂贵的数据解压缩操作，可能影响性能。")

    # Rule 81: 检查昂贵的数据分发操作
    expensive_distributions = root.findall(".//Distribution")
    if expensive_distributions:
        print("警告: 查询中存在昂贵的数据分发操作，可能影响性能。")

    # Rule 82: 检查不平衡的数据分发
    unbalanced_distributions = root.findall(".//Distribution[@Balance='false']")
    if unbalanced_distributions:
        print("警告: 数据分发不平衡，可能导致资源浪费。")

    # Rule 83: 检查不必要的数据合并
    unnecessary_data_fusions = root.findall(".//Fusion")
    if unnecessary_data_fusions:
        print("警告: 查询中存在不必要的数据合并，可能影响性能。")

    # Rule 84: 检查不必要的数据分隔
    unnecessary_data_separations = root.findall(".//Separation")
    if unnecessary_data_separations:
        print("警告: 查询中存在不必要的数据分隔，可能影响性能。")

    # Rule 85: 检查昂贵的数据分隔操作
    expensive_separations = root.findall(".//Separation")
    if expensive_separations:
        print("警告: 查询中存在昂贵的数据分隔操作，可能影响性能。")

    # Rule 86: 检查数据阻塞
    data_blockages = root.findall(".//Blockage")
    if data_blockages:
        print("警告: 数据阻塞可能导致性能问题。")

    # Rule 87: 检查数据锁
    data_locks = root.findall(".//Lock")
    if data_locks:
        print("警告: 数据锁可能导致性能问题。")

    # Rule 88: 检查数据死锁
    data_deadlocks = root.findall(".//Deadlock")
    if data_deadlocks:
        print("警告: 数据死锁可能导致查询失败。")

    # Rule 89: 检查低效的数据缓存
    inefficient_data_caching = root.findall(".//Cache[@Efficiency='low']")
    if inefficient_data_caching:
        print("警告: 低效的数据缓存可能导致性能问题。")

    # Rule 90: 检查不必要的数据重复
    unnecessary_data_replications = root.findall(".//Replication")
    if unnecessary_data_replications:
        print("警告: 查询中存在不必要的数据重复，可能影响性能。")

    # Rule 91: 检查昂贵的数据重复操作
    expensive_replications = root.findall(".//Replication")
    if expensive_replications:
        print("警告: 查询中存在昂贵的数据重复操作，可能影响性能。")

    # Rule 92: 检查数据负载不平衡
    data_load_imbalances = root.findall(".//Load[@Balance='false']")
    if data_load_imbalances:
        print("警告: 数据负载不平衡，可能导致资源浪费。")

    # Rule 93: 检查数据溢出
    data_spills = root.findall(".//Spill")
    if data_spills:
        print("警告: 数据溢出可能导致性能问题。")

    # Rule 94: 检查数据泄漏
    data_leaks = root.findall(".//Leak")
    if data_leaks:
        print("警告: 数据泄漏可能导致安全问题。")

    # Rule 95: 检查数据冲突
    data_conflicts = root.findall(".//Conflict")
    if data_conflicts:
        print("警告: 数据冲突可能导致查询失败。")

    # Rule 96: 检查使用非sargable操作
    non_sargable_ops = root.findall(".//NonSargable")
    if non_sargable_ops:
        print("警告: 查询中存在非sargable操作，可能影响性能。")

    # Rule 97: 检查数据分布不均
    data_distribution_imbalances = root.findall(".//Distribution[@Even='false']")
    if data_distribution_imbalances:
        print("警告: 数据分布不均，可能导致资源浪费。")

    # Rule 98: 检查数据碎片
    data_fragments = root.findall(".//Fragmentation")
    if data_fragments:
        print("警告: 数据碎片可能导致性能问题。")

    # Rule 99: 检查数据冗余
    data_redundancies = root.findall(".//Redundancy")
    if data_redundancies:
        print("警告: 数据冗余可能导致资源浪费。")

    # Rule 100: 检查大量的UNION操作
    excessive_unions = root.findall(".//Union")
    if excessive_unions:
        print("警告: 查询中存在大量的UNION操作，可能影响性能。")
