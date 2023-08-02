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

def audit_execution_plan(plan_xml):
    try:
        root = ET.fromstring(plan_xml)
    except Exception as e:
        print(f"解析执行计划时出错: {e}")
        return

    # 使用最基础的 XPath 查询来查找所有 TableScan
    table_scans = root.findall(".//TableScan")
    for ts in table_scans:
        # 使用 Python 来检查每个 TableScan 是否有 Index 子元素
        index_elements = [elem for elem in ts if elem.tag == 'Index']
        if not index_elements:
            print("警告: 查询包含全表扫描。考虑添加适当的索引来提高性能。")
            break


    # Rule 1: 检查全表扫描
    table_scans = root.findall(".//TableScan")
    full_scans = [ts for ts in table_scans if ts.find(".//Index") is None]
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
    rel_ops = root.findall(".//RelOp")
    discrepancies = [ro for ro in rel_ops if ro.get('EstimatedRows') != ro.get('ActualRows')]
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
    hanging_index_scans = [index_scan for index_scan in root.findall(".//IndexScan") if
                           index_scan.find("SeekPredicates") is None]
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

    # Rule 13: 检查非聚集索引扫描
    non_clustered_indexes = [index_scan for index_scan in root.findall(".//IndexScan") if
                             index_scan.get("Clustered") == "false"]
    if non_clustered_indexes:
        print("警告: 存在非聚集索引扫描，这可能会导致性能下降。考虑使用聚集索引或重新评估查询设计。")

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
    unused_tables = [relop for relop in root.findall(".//RelOp") if relop.get("EstimateRows") == "0"]
    if unused_tables:
        print("警告: 查询中存在未使用的表。这可能是查询设计不当的结果。")

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

    # Rule 101: 检查表扫描操作
    table_scans = root.findall(".//TableScan")
    if table_scans:
        print("警告: 查询中存在表扫描操作，可能影响性能。考虑添加适当的索引。")

    # Rule 102: 检查索引扫描
    index_scans = root.findall(".//IndexScan")
    if index_scans:
        print("警告: 查询中存在索引扫描，而不是索引查找。考虑修改查询或优化索引。")

    # Rule 103: 检查高代价操作
    high_cost_ops = [op for op in root.findall(".//*") if float(op.get('EstimatedTotalSubtreeCost', 0)) > 10.0]
    if high_cost_ops:
        print("警告: 查询中存在高代价的操作，可能影响性能。")

    # Rule 104: 检查RID查找
    rid_lookups = root.findall(".//RIDLookup")
    if rid_lookups:
        print("警告: 查询中存在RID查找，这可能是因为缺少覆盖索引。考虑优化索引。")

    # Rule 105: 检查大型哈希聚合
    large_hash_aggregates = root.findall(".//HashMatch[@AggregateType='Hash']")
    if large_hash_aggregates:
        print("警告: 查询中存在大型哈希聚合操作，可能导致大量的内存使用。")

    # Rule 106: 检查并行操作中的数据流不均衡
    parallel_imbalance = root.findall(".//Parallelism[@Reason='DataFlow']")
    if parallel_imbalance:
        print("警告: 查询中的并行操作存在数据流不均衡，可能导致性能下降。")

    # Rule 107: 检查计算密集型操作
    compute_intensive = [op for op in root.findall(".//*") if float(op.get('ComputeScalar', 0)) > 0.2]
    if compute_intensive:
        print("警告: 查询中存在计算密集型操作，可能影响性能。")

    # Rule 108: 检查数据移动操作
    data_movement = root.findall(".//Spool")
    if data_movement:
        print("警告: 查询中存在数据移动操作，可能影响性能。")

    # Rule 109: 检查外部表操作
    external_table_ops = root.findall(".//RemoteQuery")
    if external_table_ops:
        print("警告: 查询中存在外部表操作，可能影响性能。")

    # Rule 110: 检查嵌套循环连接
    nested_loops = root.findall(".//NestedLoops")
    if nested_loops:
        print("警告: 查询中存在嵌套循环连接，可能导致大量的行处理。考虑优化连接策略或使用适当的索引。")

    # Rule 111: 检查哈希连接
    hash_joins = root.findall(".//HashMatch[@JoinType='Inner']")
    if hash_joins:
        print("警告: 查询中存在哈希连接，可能需要大量内存。考虑优化连接策略。")

    # Rule 112: 检查排序操作
    sort_ops = root.findall(".//Sort")
    if sort_ops:
        print("警告: 查询中存在排序操作，可能影响性能。考虑优化查询或使用适当的索引。")

    # Rule 113: 检查键查找
    key_lookups = root.findall(".//KeyLookup")
    if key_lookups:
        print("警告: 查询中存在键查找操作，可能导致性能问题。考虑使用覆盖索引。")

    # Rule 114: 检查递归操作
    recursive_ops = root.findall(".//Recursive")
    if recursive_ops:
        print("警告: 查询中存在递归操作，可能影响性能。")

    # Rule 115: 检查并行操作是否因为资源争夺被阻塞
    parallel_blocked = [op for op in root.findall(".//Parallelism") if op.get('Blocked', 0) == '1']
    if parallel_blocked:
        print("警告: 查询中的并行操作被阻塞，可能是因为资源争夺。")

    # Rule 116: 检查转换操作
    convert_ops = root.findall(".//Convert")
    if convert_ops:
        print("警告: 查询中存在数据类型转换操作，可能影响性能。确保查询中使用的数据类型匹配表结构的数据类型。")

    # Rule 117: 检查流水线操作
    stream_aggregate = root.findall(".//StreamAggregate")
    if stream_aggregate:
        print("警告: 查询中存在流水线聚合操作，可能需要优化。")

    # Rule 118: 检查内存溢出到磁盘的操作
    spill_to_tempdb = [op for op in root.findall(".//*") if float(op.get('EstimatedSpillLevel', 0)) > 0]
    if spill_to_tempdb:
        print("警告: 查询中的某些操作可能导致内存溢出到tempdb，可能影响性能。")

    # Rule 119: 检查未使用的统计信息
    unused_stats = root.findall(".//StatisticsNotUsed")
    if unused_stats:
        print("警告: 查询中存在未使用的统计信息，考虑更新或删除不必要的统计信息。")

    # Rule 120: 检查非最优的位图操作
    bitmap_ops = root.findall(".//Bitmap")
    if bitmap_ops:
        print("警告: 查询中存在位图操作，可能影响性能。考虑优化相关的连接或筛选条件。")

    # Rule 121: 检查高成本的远程查询
    remote_query = root.findall(".//RemoteQuery")
    if remote_query:
        print("警告: 查询中存在高成本的远程查询操作，可能影响性能。考虑优化远程查询或将数据本地化。")

    # Rule 122: 检查表变量操作
    table_var_ops = root.findall(".//TableValuedFunction")
    if table_var_ops:
        print("警告: 查询中存在表变量操作，可能导致性能下降。考虑使用临时表替代表变量。")

    # Rule 123: 检查大量的RID查找（堆查找）
    rid_lookups = root.findall(".//RIDLookup")
    if rid_lookups:
        print("警告: 查询中存在大量的RID查找操作，这是堆查找，可能导致性能问题。考虑使用聚集索引。")

    # Rule 124: 检查计算标量操作
    compute_scalar_ops = root.findall(".//ComputeScalar")
    if compute_scalar_ops:
        print("警告: 查询中存在计算标量操作，可能影响性能。考虑将计算移至应用层或优化查询。")

    # Rule 125: 检查序列投影操作
    sequence_project = root.findall(".//SequenceProject")
    if sequence_project:
        print("警告: 查询中存在序列投影操作，可能影响性能。")

    # Rule 126: 检查窗口函数操作
    window_aggregates = root.findall(".//WindowAggregate")
    if window_aggregates:
        print("警告: 查询中存在窗口函数操作，可能影响性能。考虑优化窗口函数或相关查询。")

    # Rule 127: 检查顶部操作
    top_ops = root.findall(".//Top")
    if top_ops:
        print("警告: 查询中存在TOP操作，可能导致性能下降。确保只检索所需的记录数。")

    # Rule 128: 检查嵌套循环连接中的异步操作
    async_nested_loops = root.findall(".//NestedLoops[@IsAsync='True']")
    if async_nested_loops:
        print("警告: 查询中的嵌套循环连接存在异步操作，可能导致性能问题。考虑优化连接策略。")

    # Rule 129: 检查分配给查询的过多内存
    high_memory_grants = [op for op in root.findall(".//MemoryGrant") if
                          float(op.get('SerialRequiredMemory', 0)) > 1048576]
    if high_memory_grants:
        print("警告: 查询被分配了过多的内存，可能导致其他查询资源争夺。考虑优化查询以减少内存使用。")

    # Rule 130: 检查表的扫描操作而不是索引的扫描
    full_table_scans = root.findall(".//TableScan")
    if full_table_scans:
        print("警告: 查询中存在全表扫描操作，可能影响性能。考虑使用或优化索引以减少全表扫描。")

    # Rule 131: 检查哈希递归操作
    hash_recursive = root.findall(".//Hash[@Recursive='True']")
    if hash_recursive:
        print("警告: 查询中存在哈希递归操作，可能影响性能。考虑优化相关的连接策略。")

    # Rule 132: 检查高度并行的操作
    highly_parallel_ops = [op for op in root.findall(".//RelOp") if int(op.get('Parallel', 0)) > 8]
    if highly_parallel_ops:
        print("警告: 查询中存在高度并行的操作，可能导致资源争夺。考虑调整查询或并行度设置。")

    # Rule 133: 检查流水线函数调用
    streaming_udfs = root.findall(".//StreamingUDF")
    if streaming_udfs:
        print("警告: 查询中存在流水线UDF调用，可能导致性能下降。考虑优化或避免使用流水线UDF。")

    # Rule 134: 检查高开销的UDF调用
    high_cost_udfs = [op for op in root.findall(".//UDF") if float(op.get('EstimateTotalSubtreeCost', 0)) > 10]
    if high_cost_udfs:
        print("警告: 查询中存在高开销的UDF调用，可能影响性能。考虑优化或避免使用这些UDF。")

    # Rule 135: 检查大量的外部表操作
    external_table_ops = root.findall(".//ExternalTable")
    if external_table_ops:
        print("警告: 查询中存在大量的外部表操作，可能导致性能问题。考虑将数据本地化或优化外部查询。")

    # Rule 136: 检查不优化的子查询
    unoptimized_subqueries = root.findall(".//UnoptimizedSubquery")
    if unoptimized_subqueries:
        print("警告: 查询中存在不优化的子查询，可能导致性能问题。考虑重写子查询或将其转化为连接操作。")

    # Rule 137: 检查高开销的动态SQL操作
    dynamic_sql_ops = [op for op in root.findall(".//DynamicSQL") if float(op.get('EstimateTotalSubtreeCost', 0)) > 10]
    if dynamic_sql_ops:
        print("警告: 查询中存在高开销的动态SQL操作，可能影响性能。考虑优化或避免使用动态SQL。")

    # Rule 138: 检查数据移动操作
    data_movement_ops = root.findall(".//DataMovement")
    if data_movement_ops:
        print("警告: 查询中存在数据移动操作，可能导致性能下降。考虑优化查询或数据分布策略。")

    # Rule 139: 检查列存储索引扫描的不优化操作
    non_optimized_columnstore = root.findall(".//ColumnStoreIndexScan[@Optimized='False']")
    if non_optimized_columnstore:
        print("警告: 查询中的列存储索引扫描未被优化，可能影响性能。考虑优化查询或列存储索引设置。")

    # Rule 140: 检查排序操作中的高内存使用
    high_mem_sorts = [op for op in root.findall(".//Sort") if float(op.get('MemoryFractions', 0)) > 0.5]
    if high_mem_sorts:
        print("警告: 查询中的排序操作使用了大量内存，可能导致资源争夺。考虑优化查询或调整资源分配。")

    # Rule 141: 检查 Bitmap 过滤器操作
    bitmap_filters = root.findall(".//Bitmap")
    if bitmap_filters:
        print(
            "警告: 查询中存在 Bitmap 过滤器操作。虽然这些操作有时可以提高性能，但在某些情况下它们可能导致性能下降。考虑对相关查询进行优化。")

    # Rule 142: 检查计算标量操作
    compute_scalars = root.findall(".//ComputeScalar")
    if compute_scalars:
        print("警告: 查询中存在大量的计算标量操作，可能导致CPU开销增加。考虑优化相关的标量计算或将其移到应用程序中进行。")

    # Rule 143: 检查嵌套循环连接
    nested_loops = root.findall(".//NestedLoops")
    if nested_loops:
        print("警告: 查询中存在嵌套循环连接，这可能在大数据集上效率较低。考虑优化连接策略或确保相关列已经进行了索引。")

    # Rule 144: 检查远程查询
    remote_queries = root.findall(".//RemoteQuery")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，可能导致网络开销增加。考虑将数据本地化或优化远程查询。")

    # Rule 145: 检查表变量操作
    table_vars = root.findall(".//TableVariable")
    if table_vars:
        print("警告: 查询中使用了表变量，这可能在某些情况下效率较低。考虑使用临时表或优化表变量使用。")

    # Rule 146: 检查 TVF (表值函数) 扫描
    tvf_scans = root.findall(".//TableValuedFunction")
    if tvf_scans:
        print("警告: 查询中存在表值函数(TVF)扫描，可能导致性能下降。考虑优化 TVF 或使用其他方法重写查询。")

    # Rule 147: 检查 Sort 操作中的警告
    sort_warnings = [op for op in root.findall(".//Sort") if op.get('WithAbortOption', 'False') == 'True']
    if sort_warnings:
        print("警告: 查询中的排序操作存在潜在的中止选项，可能导致查询提前结束。确保为排序操作提供足够的资源或优化查询。")

    # Rule 148: 检查 Spool 操作
    spool_ops = root.findall(".//Spool")
    if spool_ops:
        print("警告: 查询中存在 Spool 操作，可能导致磁盘开销增加。考虑优化查询以减少或消除 Spool 操作。")

    # Rule 149: 检查 Window 函数操作
    window_funcs = root.findall(".//Window")
    if window_funcs:
        print("警告: 查询中使用了窗口函数，可能导致性能下降。考虑优化窗口函数的使用或重写查询。")

    # Rule 150: 检查交叉应用操作
    cross_app_ops = root.findall(".//CrossApp")
    if cross_app_ops:
        print("警告: 查询中存在跨应用操作，可能导致性能和数据一致性问题。考虑将数据移动到同一应用或优化跨应用查询。")

    # Rule 151: 检查哈希匹配操作
    hash_matches = root.findall(".//HashMatch")
    if hash_matches:
        print("警告: 查询中存在哈希匹配操作，可能导致内存开销增加。考虑优化连接策略或确保相关列已经进行了索引。")

    # Rule 152: 检查高代价的流操作
    high_cost_streams = [op for op in root.findall(".//RelOp") if float(op.get('EstimatedTotalSubtreeCost', 0)) > 10]
    if high_cost_streams:
        print("警告: 查询中存在高代价的流操作，可能导致性能下降。仔细检查这些操作并考虑进行优化。")

    # Rule 153: 检查非平行查询操作
    non_parallel_ops = root.findall(".//NonParallelPlanReason")
    if non_parallel_ops:
        print("警告: 查询未能并行执行。考虑优化查询或检查服务器设置以支持并行处理。")

    # Rule 154: 检查大型数据移动操作
    data_movement_ops = root.findall(".//DataMovement")
    if data_movement_ops:
        print("警告: 查询中存在大型数据移动操作，可能导致网络或磁盘开销增加。考虑优化查询或数据库结构。")

    # Rule 155: 检查大型删除操作
    delete_ops = [op for op in root.findall(".//Delete") if int(op.get('RowCount', 0)) > 10000]
    if delete_ops:
        print("警告: 查询中执行了大量的删除操作，可能导致性能下降或锁定问题。考虑分批进行删除或优化删除策略。")

    # Rule 156: 检查多表连接操作
    multi_table_joins = root.findall(".//Join[@PhysicalOp='MultiTableJoin']")
    if multi_table_joins:
        print("警告: 查询中存在多表连接操作，可能导致性能下降。考虑重写查询或优化连接策略。")

    # Rule 157: 检查列存储索引的效率
    columnstore_scans = root.findall(".//ColumnStoreIndexScan")
    if columnstore_scans:
        print("警告: 查询中存在列存储索引扫描，但可能没有充分利用列存储的优势。考虑优化查询或检查列存储索引的设计。")

    # Rule 158: 检查排序操作的内存开销
    sort_memory_issues = [op for op in root.findall(".//Sort") if float(op.get('MemoryFraction', 0)) > 0.5]
    if sort_memory_issues:
        print("警告: 查询中的排序操作使用了大量的内存。考虑优化排序操作或增加查询的内存配额。")

    # Rule 159: 检查潜在的死锁操作
    potential_deadlocks = root.findall(".//Deadlock")
    if potential_deadlocks:
        print("警告: 查询中存在可能导致死锁的操作。考虑重写查询或调整事务隔离级别。")

    # Rule 160: 检查全文搜索操作的效率
    fulltext_searches = root.findall(".//FullTextSearch")
    if fulltext_searches:
        print("警告: 查询中使用了全文搜索，但可能没有充分优化。考虑检查全文索引或优化全文查询。")

    # Rule 161: 检查嵌套循环连接操作
    nested_loops = root.findall(".//NestedLoops")
    if nested_loops:
        print("警告: 查询中存在嵌套循环连接操作，可能会影响大数据集的性能。")

    # Rule 162: 检查哈希匹配连接操作
    hash_matches = root.findall(".//HashMatch")
    if hash_matches:
        print("警告: 查询中存在哈希匹配连接操作，可能会导致额外的I/O和CPU负担。")

    # Rule 163: 检查表变量的使用
    table_vars = root.findall(".//TableValuedFunction")
    if table_vars:
        print("警告: 查询中使用了表变量，可能会影响性能，尤其是在大数据集上。")

    # Rule 164: 检查RID Lookup操作
    rid_lookup = root.findall(".//RIDLookup")
    if rid_lookup:
        print("警告: 查询中存在RID Lookup操作，这通常意味着缺少聚集索引。")

    # Rule 165: 检查Filter操作
    filters = root.findall(".//Filter")
    if filters:
        print("警告: 查询中存在Filter操作，可能会导致查询性能下降。")

    # Rule 166: 检查并行操作
    parallel_ops = root.findall(".//Parallelism")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能会导致资源争用和性能下降。")

    # Rule 167: 检查Sort操作
    sort_ops = root.findall(".//Sort")
    if sort_ops:
        print("警告: 查询中存在Sort操作，大量的排序可能会消耗大量的CPU和内存。")

    # Rule 168: 检查Compute Scalar操作
    compute_scalar_ops = root.findall(".//ComputeScalar")
    if compute_scalar_ops:
        print("警告: 查询中存在Compute Scalar操作，可能会导致额外的计算开销。")

    # Rule 169: 检查Sequence Project操作
    sequence_project_ops = root.findall(".//SequenceProject")
    if sequence_project_ops:
        print("警告: 查询中存在Sequence Project操作，可能会影响查询性能。")

    # Rule 170: 检查Stream Aggregate操作
    stream_aggregate_ops = root.findall(".//StreamAggregate")
    if stream_aggregate_ops:
        print("警告: 查询中存在Stream Aggregate操作，可能会导致I/O和CPU的额外负担。")

    # Rule 171: 检查Convert操作
    convert_ops = root.findall(".//Convert")
    if convert_ops:
        print("警告: 查询中存在数据类型转换操作，可能会导致性能下降。")

    # Rule 172: 检查Constant Scan操作
    constant_scan_ops = root.findall(".//ConstantScan")
    if constant_scan_ops:
        print("警告: 查询中存在Constant Scan操作，可能影响查询性能。")

    # Rule 173: 检查外部表连接
    external_table_joins = root.findall(".//RemoteQuery")
    if external_table_joins:
        print("警告: 查询涉及外部表连接，可能导致性能问题。")

    # Rule 174: 检查Sparse Column操作
    sparse_column_ops = root.findall(".//SparseColumnOperator")
    if sparse_column_ops:
        print("警告: 查询中使用了稀疏列，这可能会影响性能。")

    # Rule 175: 检查TOP操作
    top_ops = root.findall(".//Top")
    if top_ops:
        print("警告: 查询中使用了TOP操作，可能导致性能问题，尤其是当未与ORDER BY结合使用时。")

    # Rule 176: 检查UDF (用户定义函数) 的使用
    udf_ops = root.findall(".//UserDefinedFunction")
    if udf_ops:
        print("警告: 查询中使用了用户定义的函数，这可能导致性能问题。")

    # Rule 177: 检查Window Aggregate操作
    window_aggregate_ops = root.findall(".//WindowAggregate")
    if window_aggregate_ops:
        print("警告: 查询中使用了窗口聚合函数，可能导致性能问题。")

    # Rule 178: 检查XML运算操作
    xml_ops = root.findall(".//XmlReader")
    if xml_ops:
        print("警告: 查询中存在XML操作，可能会影响性能。")

    # Rule 179: 检查全文索引查询
    fulltext_query = root.findall(".//Contains")
    if fulltext_query:
        print("警告: 查询中使用了全文索引查询，可能导致性能问题。")

    # Rule 180: 检查动态SQL操作
    dynamic_sql_ops = root.findall(".//Dynamic")
    if dynamic_sql_ops:
        print("警告: 查询中存在动态SQL操作，可能导致性能和安全问题。")

    # Rule 181: 检查表变量操作
    table_variable_ops = root.findall(".//TableVariable")
    if table_variable_ops:
        print("警告: 查询中使用了表变量，可能导致性能问题。")

    # Rule 182: 检查悬挂的外部连接
    unmatched_outer_joins = root.findall(".//UnmatchedOuterJoin")
    if unmatched_outer_joins:
        print("警告: 查询中存在悬挂的外部连接，可能导致性能问题。")

    # Rule 183: 检查表值函数
    table_valued_function = root.findall(".//TableValuedFunction")
    if table_valued_function:
        print("警告: 查询中使用了表值函数，可能导致性能问题。")

    # Rule 184: 检查列存储索引扫描
    column_store_scan = root.findall(".//ColumnStoreIndexScan")
    if column_store_scan:
        print("警告: 查询中使用了列存储索引扫描，可能导致性能问题。")

    # Rule 185: 检查列存储索引查找
    column_store_seek = root.findall(".//ColumnStoreIndexSeek")
    if column_store_seek:
        print("警告: 查询中使用了列存储索引查找，可能导致性能问题。")

    # Rule 186: 检查列存储哈希匹配
    column_store_hash = root.findall(".//ColumnStoreHashJoin")
    if column_store_hash:
        print("警告: 查询中使用了列存储哈希匹配，可能导致性能问题。")

    # Rule 187: 检查非优化的嵌套循环
    non_optimized_loops = root.findall(".//NestedLoops")
    if non_optimized_loops:
        print("警告: 查询中存在非优化的嵌套循环，可能导致性能问题。")

    # Rule 188: 检查递归查询
    recursive_cte = root.findall(".//RecursiveCTE")
    if recursive_cte:
        print("警告: 查询中使用了递归公共表达式，可能导致性能问题。")

    # Rule 189: 检查远程查询
    remote_query = root.findall(".//RemoteQuery")
    if remote_query:
        print("警告: 查询中存在远程查询操作，可能导致性能问题。")

    # Rule 190: 检查非参数化查询
    non_param_queries = root.findall(".//NonParameterizedQuery")
    if non_param_queries:
        print("警告: 查询中存在非参数化查询，可能导致性能问题和SQL注入风险。")

    # Rule 191: 检查顺序扫描
    seq_scans = root.findall(".//SequenceProject")
    if seq_scans:
        print("警告: 查询中存在顺序扫描，可能导致性能问题。")

    # Rule 192: 检查排序操作
    sort_ops = root.findall(".//Sort")
    if sort_ops:
        print("警告: 查询中存在排序操作，可能导致性能问题。")

    # Rule 193: 检查空连接
    null_joins = root.findall(".//NullIf")
    if null_joins:
        print("警告: 查询中使用了空连接，可能导致性能问题。")

    # Rule 194: 检查使用不等于操作
    not_equals_ops = root.findall(".//NotEquals")
    if not_equals_ops:
        print("警告: 查询中使用了不等于操作，可能导致性能问题。")

    # Rule 195: 检查大型插入
    bulk_inserts = root.findall(".//BulkInsert")
    if bulk_inserts:
        print("警告: 查询中存在大型插入操作，可能导致性能问题。")

    # Rule 196: 检查大型更新
    bulk_updates = root.findall(".//BulkUpdate")
    if bulk_updates:
        print("警告: 查询中存在大型更新操作，可能导致性能问题。")

    # Rule 197: 检查硬编码值
    hardcoded_vals = root.findall(".//ConstantScan")
    if hardcoded_vals:
        print("警告: 查询中存在硬编码的值，可能导致性能问题和可维护性问题。")

    # Rule 198: 检查复杂的视图嵌套
    nested_views = root.findall(".//View")
    if len(nested_views) > 2:
        print("警告: 查询中存在过多的视图嵌套，可能导致性能问题。")

    # Rule 199: 检查不必要的计算
    unnecessary_computations = root.findall(".//ComputeScalar")
    if unnecessary_computations:
        print("警告: 查询中存在不必要的计算，可能导致性能问题。")

    # Rule 200: 检查大量的嵌套子查询
    nested_subqueries = [op for op in root.findall(".//Subquery") if int(op.get('NestedLevel', 0)) > 5]
    if nested_subqueries:
        print("警告: 查询中存在大量的嵌套子查询，可能导致性能下降。考虑将部分子查询改写为连接或临时表。")

    # ... [继续上一个文件中的代码]

    # Rule 201: 检查Hash Match操作，可能意味着查询需要优化
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash Match操作，可能需要进一步优化。")

    # Rule 202: 检查RID Lookup操作，可能意味着需要更好的索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，考虑优化相关索引。")

    # Rule 203: 检查Nested Loops Join，当数据量大时可能不高效
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if nested_loops:
        print("警告: 查询中存在Nested Loops操作，可能需要进一步优化。")

    # Rule 204: 检查大量的Sort操作，可能影响性能
    sort_operations = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_operations:
        print("警告: 查询中存在多个Sort操作，可能影响性能。")

    # Rule 205: 检查Parallelism操作，可能意味着查询可以进一步优化
    parallelism_operations = root.findall(".//RelOp[@PhysicalOp='Parallelism']")
    if parallelism_operations:
        print("警告: 查询中存在Parallelism操作，考虑进一步优化查询。")

    # Rule 206: 检查Filter操作，可能意味着查询条件不高效
    filter_operations = root.findall(".//RelOp[@PhysicalOp='Filter']")
    if filter_operations:
        print("警告: 查询中存在Filter操作，可能需要调整查询条件。")

    # Rule 207: 检查Compute Scalar操作，可能影响性能
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalars:
        print("警告: 查询中存在Compute Scalar操作，可能影响性能。")

    # Rule 208: 检查非优化的Bitmap操作
    non_optimized_bitmaps = root.findall(".//RelOp[@PhysicalOp='Bitmap']")
    if non_optimized_bitmaps:
        print("警告: 查询中存在非优化的Bitmap操作，考虑进一步优化查询。")

    # Rule 209: 检查Sequence Project操作，可能意味着查询需要优化
    sequence_projects = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_projects:
        print("警告: 查询中存在Sequence Project操作，可能需要进一步优化。")

    # Rule 210: 检查流水线操作，可能意味着查询中的某些部分不高效
    stream_aggregate = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_aggregate:
        print("警告: 查询中存在Stream Aggregate操作，可能需要进一步优化。")

    # ... [继续上述的代码]

    # Rule 211: 检查存在的递归操作，可能影响性能
    recursive_operations = root.findall(".//RelOp[@PhysicalOp='Recursive Union']")
    if recursive_operations:
        print("警告: 查询中存在递归操作，可能影响性能。")

    # Rule 212: 检查Hash Team操作，可能意味着需要更大的内存
    hash_teams = root.findall(".//RelOp[@PhysicalOp='Hash Team']")
    if hash_teams:
        print("警告: 查询中存在Hash Team操作，考虑增加可用内存或优化查询。")

    # Rule 213: 检查存在的动态索引操作，可能影响性能
    dynamic_indexes = root.findall(".//RelOp[@PhysicalOp='Dynamic Index']")
    if dynamic_indexes:
        print("警告: 查询中存在动态索引操作，可能影响性能。")

    # Rule 214: 检查存在的动态排序操作，可能影响性能
    dynamic_sorts = root.findall(".//RelOp[@PhysicalOp='Dynamic Sort']")
    if dynamic_sorts:
        print("警告: 查询中存在动态排序操作，可能影响性能。")

    # Rule 215: 检查存在的Bitmap Heap操作，可能意味着查询需要优化
    bitmap_heaps = root.findall(".//RelOp[@PhysicalOp='Bitmap Heap']")
    if bitmap_heaps:
        print("警告: 查询中存在Bitmap Heap操作，考虑进一步优化查询。")

    # Rule 216: 检查存在的远程查询操作，可能意味着跨服务器查询不高效
    remote_queries = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，考虑优化跨服务器查询。")

    # Rule 217: 检查存在的流水线排序操作，可能影响性能
    stream_sorts = root.findall(".//RelOp[@PhysicalOp='Stream Sort']")
    if stream_sorts:
        print("警告: 查询中存在Stream Sort操作，可能影响性能。")

    # Rule 218: 检查存在的窗口聚合操作，可能意味着查询需要优化
    window_aggregates = root.findall(".//RelOp[@PhysicalOp='Window Aggregate']")
    if window_aggregates:
        print("警告: 查询中存在窗口聚合操作，考虑进一步优化查询。")

    # Rule 219: 检查存在的列存储索引扫描，可能意味着列存储索引需要优化
    columnstore_index_scans = root.findall(".//RelOp[@PhysicalOp='Columnstore Index Scan']")
    if columnstore_index_scans:
        print("警告: 查询中存在列存储索引扫描操作，考虑优化列存储索引。")

    # Rule 220: 检查存在的分区操作，可能影响性能
    partition_operations = root.findall(".//RelOp[@PhysicalOp='Partition']")
    if partition_operations:
        print("警告: 查询中存在分区操作，可能影响性能。")

    # Rule 221: 检查存在的Hash匹配操作，这可能意味着连接不够高效
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash匹配操作，考虑使用其他连接策略如Merge或Loop。")

    # Rule 222: 检查并行操作，可能意味着查询可以进一步优化以避免并行处理
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能意味着查询需要进一步优化。")

    # Rule 223: 检查存在的Compute Scalar操作，这可能意味着有计算操作可以在查询中优化
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if compute_scalars:
        print("警告: 查询中存在Compute Scalar操作，考虑是否有计算可以优化。")

    # Rule 224: 检查存在的顺序扫描，可能意味着缺少索引
    sequence_scans = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_scans:
        print("警告: 查询中存在顺序扫描，考虑添加适当的索引。")

    # Rule 225: 检查存在的Table Spool操作，可能影响性能
    table_spools = root.findall(".//RelOp[@PhysicalOp='Table Spool']")
    if table_spools:
        print("警告: 查询中存在Table Spool操作，可能影响性能。")

    # Rule 226: 检查存在的RID Lookup操作，可能意味着需要一个聚集索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，考虑添加一个聚集索引。")

    # Rule 227: 检查存在的Top操作，可能意味着查询返回大量数据
    top_ops = root.findall(".//RelOp[@PhysicalOp='Top']")
    if top_ops:
        print("警告: 查询中存在Top操作，考虑是否真的需要返回那么多数据。")

    # Rule 228: 检查存在的Key Lookup操作，可能意味着非聚集索引缺失某些列
    key_lookups = root.findall(".//RelOp[@PhysicalOp='Key Lookup']")
    if key_lookups:
        print("警告: 查询中存在Key Lookup操作，考虑将查找的列包括在非聚集索引中。")

    # Rule 229: 检查存在的Nested Loops操作，可能意味着连接不够高效
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if nested_loops:
        print("警告: 查询中存在Nested Loops操作，考虑优化查询或使用其他连接策略。")

    # Rule 230: 检查存在的Bitmap操作，这可能影响性能
    bitmaps = root.findall(".//RelOp[@PhysicalOp='Bitmap Create']")
    if bitmaps:
        print("警告: 查询中存在Bitmap操作，可能影响性能。")

    # Rule 231: 检查存在的流操作，它可能表示数据排序并可能影响性能
    stream_ops = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_ops:
        print("警告: 查询中存在Stream Aggregate操作，可能表示数据排序并影响性能。")

    # Rule 232: 检查Sort操作，它可能导致性能下降
    sort_ops = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_ops:
        print("警告: 查询中存在Sort操作，考虑优化查询以减少或消除排序。")

    # Rule 233: 检查存在的Remote Query操作，可能意味着跨服务器查询，这可能影响性能
    remote_queries = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if remote_queries:
        print("警告: 查询中存在Remote Query操作，跨服务器查询可能影响性能。")

    # Rule 234: 检查Filter操作，尤其是高成本的Filter，这可能影响性能
    high_cost_filters = [op for op in root.findall(".//RelOp[@PhysicalOp='Filter']") if
                         float(op.attrib['EstimatedTotalSubtreeCost']) > 1.0]
    if high_cost_filters:
        print("警告: 查询中存在高成本的Filter操作，考虑优化查询条件。")

    # Rule 235: 检查存在的Constant Scan操作，这可能意味着查询中有不必要的常数扫描
    constant_scans = root.findall(".//RelOp[@PhysicalOp='Constant Scan']")
    if constant_scans:
        print("警告: 查询中存在Constant Scan操作，考虑优化查询以避免不必要的常数扫描。")

    # Rule 236: 检查存在的Dynamic Index Seek操作，这可能意味着索引未被完全利用
    dynamic_seeks = root.findall(".//RelOp[@PhysicalOp='Dynamic Index Seek']")
    if dynamic_seeks:
        print("警告: 查询中存在Dynamic Index Seek操作，考虑优化索引以提高其效率。")

    # Rule 237: 检查存在的Bitmap Heap Scan，这可能意味着需要一个索引来改善性能
    bitmap_heap_scans = root.findall(".//RelOp[@PhysicalOp='Bitmap Heap Scan']")
    if bitmap_heap_scans:
        print("警告: 查询中存在Bitmap Heap Scan操作，考虑添加索引以改善性能。")

    # Rule 238: 检查存在的动态序列扫描，可能表示查询中有动态生成的序列
    dynamic_sequence_scans = root.findall(".//RelOp[@PhysicalOp='Dynamic Sequence Project']")
    if dynamic_sequence_scans:
        print("警告: 查询中存在Dynamic Sequence Project操作，可能影响性能。")

    # Rule 239: 检查存在的列存储索引扫描，这可能意味着列存储索引未被完全利用
    columnstore_scans = root.findall(".//RelOp[@PhysicalOp='Columnstore Index Scan']")
    if columnstore_scans:
        print("警告: 查询中存在Columnstore Index Scan操作，考虑优化查询以更好地利用列存储索引。")

    # Rule 240: 检查存在的外部表扫描，这可能意味着查询正在从外部数据源检索数据
    external_table_scans = root.findall(".//RelOp[@PhysicalOp='External Table Scan']")
    if external_table_scans:
        print("警告: 查询中存在External Table Scan操作，访问外部数据源可能影响性能。")

    # Rule 241: 检查大量的嵌套循环连接操作
    nested_loops = root.findall(".//RelOp[@PhysicalOp='Nested Loops']")
    if len(nested_loops) > 5:
        print("警告: 查询中存在大量的Nested Loops操作，可能影响性能。考虑优化查询或索引。")

    # Rule 242: 检查Hash Match操作，它可能导致内存中的数据溢出到磁盘
    hash_matches = root.findall(".//RelOp[@PhysicalOp='Hash Match']")
    if hash_matches:
        print("警告: 查询中存在Hash Match操作。这可能导致内存中的数据溢出到磁盘，影响性能。")

    # Rule 243: 检查递归CTE，它可能导致性能问题
    recursive_ctes = root.findall(".//RelOp[@LogicalOp='Recursive Union']")
    if recursive_ctes:
        print("警告: 查询中使用了递归CTE，可能导致性能问题。")

    # Rule 244: 检查RID Lookup操作，它可能表示需要聚集索引
    rid_lookups = root.findall(".//RelOp[@PhysicalOp='RID Lookup']")
    if rid_lookups:
        print("警告: 查询中存在RID Lookup操作，可能需要聚集索引来改善性能。")

    # Rule 245: 检查Adaptive Join操作，可能影响性能
    adaptive_joins = root.findall(".//RelOp[@PhysicalOp='Adaptive Join']")
    if adaptive_joins:
        print("警告: 查询中存在Adaptive Join操作，可能影响性能。")

    # Rule 246: 检查并行操作，它们可能导致线程竞争和性能下降
    parallel_ops = root.findall(".//RelOp[@Parallel='1']")
    if parallel_ops:
        print("警告: 查询中存在并行操作，可能导致线程竞争和性能下降。")

    # Rule 247: 检查表变量操作，它们可能没有统计信息并导致性能问题
    table_vars = root.findall(".//RelOp[@PhysicalOp='Table-valued function']")
    if table_vars:
        print("警告: 查询中使用了表变量，它们可能没有统计信息并导致性能问题。")

    # Rule 248: 检查Compute Scalar操作，大量的Compute Scalar可能影响性能
    compute_scalars = root.findall(".//RelOp[@PhysicalOp='Compute Scalar']")
    if len(compute_scalars) > 5:
        print("警告: 查询中存在大量的Compute Scalar操作，可能影响性能。")

    # Rule 249: 检查非SARGable操作，如函数在WHERE子句中的列上
    non_sargable = root.findall(".//ScalarOperator[Function]")
    if non_sargable:
        print("警告: 查询中存在非SARGable操作，可能影响性能。")

    # Rule 250: 检查大量的Spool操作，可能影响性能
    spool_ops = root.findall(".//RelOp[@PhysicalOp='Spool']")
    if len(spool_ops) > 3:
        print("警告: 查询中存在大量的Spool操作，可能影响性能。")

    # Rule 251: 检查Sort操作，因为它们可能导致内存中的数据溢出到磁盘
    sort_ops = root.findall(".//RelOp[@PhysicalOp='Sort']")
    if sort_ops:
        print("警告: 查询中存在Sort操作，这可能导致内存中的数据溢出到磁盘，影响性能。")

    # Rule 252: 检查存在的外部表操作，这可能表示跨数据库或远程查询
    external_tables = root.findall(".//RelOp[@PhysicalOp='Remote Query']")
    if external_tables:
        print("警告: 查询中存在远程查询操作，可能影响性能。考虑将数据本地化。")

    # Rule 253: 检查Bitmap操作，因为它们可能导致CPU使用率增加
    bitmap_ops = root.findall(".//RelOp[@PhysicalOp='Bitmap']")
    if bitmap_ops:
        print("警告: 查询中存在Bitmap操作，这可能导致CPU使用率增加。")

    # Rule 254: 检查存在的流聚合，因为在大数据集上可能不高效
    stream_aggregates = root.findall(".//RelOp[@PhysicalOp='Stream Aggregate']")
    if stream_aggregates:
        print("警告: 查询中存在流聚合操作，这在大数据集上可能不高效。")

    # Rule 255: 检查存在的窗口聚合，因为它们可能影响性能
    window_aggs = root.findall(".//RelOp[@PhysicalOp='Window Aggregate']")
    if window_aggs:
        print("警告: 查询中存在窗口聚合操作，这可能影响性能。")

    # Rule 256: 检查存在的序列投影，它们可能导致内存压力
    sequence_projections = root.findall(".//RelOp[@PhysicalOp='Sequence Project']")
    if sequence_projections:
        print("警告: 查询中存在序列投影操作，这可能导致内存压力。")

    # Rule 257: 检查高成本的操作，因为它们可能是性能瓶颈
    high_cost_ops = [op for op in root.findall(".//RelOp") if float(op.get('EstimatedTotalSubtreeCost', '0')) > 50]
    if high_cost_ops:
        print("警告: 查询中存在高成本的操作，可能是性能瓶颈。")

    # Rule 258: 检查存在的哈希匹配部分连接，因为它们可能导致内存中的数据溢出到磁盘
    hash_partial_joins = root.findall(".//RelOp[@PhysicalOp='Partial Hash Match']")
    if hash_partial_joins:
        print("警告: 查询中存在哈希匹配部分连接，这可能导致内存中的数据溢出到磁盘。")

    # Rule 259: 检查存在的懒惰溢出，因为这可能表示内存压力
    lazy_spools = root.findall(".//RelOp[@PhysicalOp='Lazy Spool']")
    if lazy_spools:
        print("警告: 查询中存在懒惰溢出操作，这可能表示内存压力。")

    # Rule 260: 检查存在的非优化的嵌套循环，因为它们可能是性能瓶颈
    non_opt_loops = root.findall(".//RelOp[@PhysicalOp='Non-Optimized Nested Loops']")
    if non_opt_loops:
        print("警告: 查询中存在非优化的嵌套循环操作，这可能是性能瓶颈。")

    # Rule 261: 检查是否存在过多的计算列
    computed_columns = root.findall(".//ComputeScalar")
    if len(computed_columns) > 5:
        print("警告: 查询中存在过多的计算列，可能导致CPU负担增加。")

    # Rule 262: 检查是否使用了全文搜索
    full_text_search = root.findall(".//Contains")
    if full_text_search:
        print("警告: 查询使用了全文搜索，可能影响性能。确保全文搜索已正确配置并优化。")

    # Rule 263: 检查是否存在并行操作，但并行度过低
    parallel_ops = [op for op in root.findall(".//RelOp") if "Parallel" in op.get('PhysicalOp', '')]
    if parallel_ops and int(root.get('DegreeOfParallelism', 1)) < 2:
        print("警告: 查询中存在并行操作，但并行度过低。考虑提高并行度。")

    # Rule 264: 检查是否存在非平衡的并行操作
    non_balanced_parallel_ops = [op for op in parallel_ops if op.get('NonParallelPlanReason') == 'NonParallelizable']
    if non_balanced_parallel_ops:
        print("警告: 查询中存在非平衡的并行操作，可能导致资源未被充分利用。")

    # Rule 265: 检查是否存在过多的UDF调用
    udf_calls = root.findall(".//UDF")
    if len(udf_calls) > 3:
        print("警告: 查询中存在过多的UDF调用，可能导致性能下降。")

    # Rule 266: 检查是否存在非SARGable操作，导致索引未能有效使用
    non_sargable_ops = root.findall(".//Filter[@NonSargable]")
    if non_sargable_ops:
        print("警告: 查询中存在非SARGable操作，可能导致索引未能有效使用。")

    # Rule 267: 检查是否有因为数据类型不匹配导致的隐式转换
    implicit_conversions = root.findall(".//Convert[@Implicit]")
    if implicit_conversions:
        print("警告: 查询中存在隐式数据类型转换，可能导致性能下降。")

    # Rule 268: 检查是否存在过大的数据移动操作
    large_data_movement_ops = [op for op in root.findall(".//RelOp") if
                               float(op.get('EstimatedDataSize', '0')) > 1000000]
    if large_data_movement_ops:
        print("警告: 查询中存在大量数据移动操作，可能导致性能瓶颈。")

    # Rule 269: 检查是否有太多的内存授予操作，可能导致内存压力
    high_memory_grants = [op for op in root.findall(".//MemoryGrant") if float(op.get('RequestedMemory', '0')) > 10000]
    if high_memory_grants:
        print("警告: 查询请求了大量的内存，可能导致内存压力。")

    # Rule 270: 检查是否存在多次读取同一表的操作
    multiple_table_reads = {}
    for op in root.findall(".//RelOp"):
        table_name = op.find(".//Object[@Table]")
        if table_name is not None:
            table_name = table_name.get('Table', '')
            multiple_table_reads[table_name] = multiple_table_reads.get(table_name, 0) + 1

    tables_read_multiple_times = [table for table, count in multiple_table_reads.items() if count > 1]
    if tables_read_multiple_times:
        print(f"警告: 表 {', '.join(tables_read_multiple_times)} 在查询中被多次读取，可能导致性能下降。")

    # Rule 271: 检查是否存在过大的哈希匹配
    large_hash_matches = [op for op in root.findall(".//HashMatch") if
                          float(op.get('EstimatedDataSize', '0')) > 1000000]
    if large_hash_matches:
        print("警告: 查询中存在大型的哈希匹配操作，可能导致性能下降。")

    # Rule 272: 检查是否存在高开销的嵌套循环
    expensive_loops = [op for op in root.findall(".//NestedLoops") if
                       float(op.get('EstimatedTotalSubtreeCost', '0')) > 5.0]
    if expensive_loops:
        print("警告: 查询中存在高开销的嵌套循环操作，考虑优化相关逻辑。")

    # Rule 273: 检查是否使用了远程查询
    remote_queries = root.findall(".//RemoteQuery")
    if remote_queries:
        print("警告: 查询中存在远程查询操作，可能导致性能延迟。")

    # Rule 274: 检查是否存在不必要的物理操作，如排序
    redundant_sorts = root.findall(".//Sort[@IsExplicitlyForced]")
    if redundant_sorts:
        print("警告: 查询中存在不必要的排序操作，考虑移除冗余的ORDER BY子句。")

    # Rule 275: 检查是否有大量数据的插入操作
    large_inserts = [op for op in root.findall(".//Insert") if float(op.get('EstimatedDataSize', '0')) > 500000]
    if large_inserts:
        print("警告: 查询中存在大量数据的插入操作，可能导致性能瓶颈。")

    # Rule 276: 检查是否有大量数据的更新操作
    large_updates = [op for op in root.findall(".//Update") if float(op.get('EstimatedDataSize', '0')) > 500000]
    if large_updates:
        print("警告: 查询中存在大量数据的更新操作，可能导致性能瓶颈。")

    # Rule 277: 检查查询是否有回退操作
    rollbacks = root.findall(".//Rollback")
    if rollbacks:
        print("警告: 查询中存在回退操作，可能导致性能下降和数据不一致。")

    # Rule 278: 检查是否存在大量的动态SQL执行
    dynamic_sql_ops = [op for op in root.findall(".//DynamicSQL") if float(op.get('EstimatedDataSize', '0')) > 100000]
    if dynamic_sql_ops:
        print("警告: 查询中存在大量的动态SQL执行，可能导致安全风险和性能问题。")

    # Rule 279: 检查查询是否过于复杂，包含过多的连接和子查询
    highly_complex_queries = [op for op in root.findall(".//Query") if
                              len(op.findall(".//Join")) > 10 or len(op.findall(".//Subquery")) > 5]
    if highly_complex_queries:
        print("警告: 查询可能过于复杂，考虑分解或重新设计查询逻辑。")

    # Rule 280: 检查是否有过多的列被选择，可能导致不必要的数据传输
    over_selected_columns = [op for op in root.findall(".//OutputList") if len(op.findall(".//ColumnReference")) > 20]
    if over_selected_columns:
        print("警告: 查询中选择了过多的列，考虑只选择必要的列以提高性能。")

    # Rule 281: 检查表的广播操作
    broadcast_ops = root.findall(".//Broadcast")
    if broadcast_ops:
        print("警告: 查询中存在表的广播操作，这可能会导致性能问题，特别是在大数据集上。")

    # Rule 282: 检查是否有在内存中的大型表扫描操作
    in_memory_table_scans = [op for op in root.findall(".//TableScan") if op.get('Storage') == 'MemoryOptimized']
    if in_memory_table_scans:
        print("警告: 查询中存在内存中的大型表扫描，考虑优化索引或查询以避免全表扫描。")

    # Rule 283: 检查是否有过多的自连接
    self_joins = [op for op in root.findall(".//RelOp[.//Join]") if
                  op.find(".//Object").get('Database') == op.find(".//Object[2]").get('Database') and op.find(
                      ".//Object").get('Schema') == op.find(".//Object[2]").get('Schema') and op.find(".//Object").get(
                      'Table') == op.find(".//Object[2]").get('Table')]
    if self_joins:
        print("警告: 查询中存在过多的自连接，可能导致性能下降。")

    # Rule 284: 检查是否有使用悲观锁定
    pessimistic_locks = root.findall(".//PessimisticLock")
    if pessimistic_locks:
        print("警告: 查询中使用了悲观锁定，可能导致其他查询被阻塞。")

    # Rule 285: 检查是否存在高代价的远程查询
    expensive_remote_queries = [op for op in root.findall(".//RemoteQuery") if
                                float(op.get('EstimatedTotalSubtreeCost', '0')) > 5.0]
    if expensive_remote_queries:
        print("警告: 查询中存在高代价的远程查询，考虑优化远程查询或将数据本地化。")

    # Rule 286: 检查是否有大量的数据压缩和解压缩操作
    data_compression_ops = root.findall(".//DataCompression")
    if data_compression_ops:
        print("警告: 查询中存在大量的数据压缩和解压缩操作，这可能会导致性能下降。")

    # Rule 287: 检查是否使用了过时的或不建议使用的操作符
    deprecated_ops = root.findall(".//DeprecatedOperator")
    if deprecated_ops:
        print("警告: 查询中使用了过时的或不建议使用的操作符，考虑更新查询。")

    # Rule 288: 检查是否有大量的数据转换操作
    data_conversion_ops = root.findall(".//Convert")
    if data_conversion_ops:
        print("警告: 查询中存在大量的数据转换操作，这可能会导致性能下降。")

    # Rule 289: 检查是否存在大量的行到列的转换
    pivot_ops = root.findall(".//Pivot")
    if pivot_ops:
        print("警告: 查询中存在大量的行到列的转换，可能导致性能问题。")

    # Rule 290: 检查是否有不必要的数据复制操作
    copy_ops = root.findall(".//Copy")
    if copy_ops:
        print("警告: 查询中存在不必要的数据复制操作，考虑优化查询逻辑。")

    # Rule 291: 检查是否存在对大表的 Nested Loops
    large_nested_loops = [op for op in root.findall(".//NestedLoops") if
                          float(op.get('EstimatedRowCount', '0')) > 100000]
    if large_nested_loops:
        print("警告: 查询中存在对大表的 Nested Loops 操作，这可能会导致性能问题。考虑优化查询或使用其他的连接策略。")

    # Rule 292: 检查是否有不必要的 ORDER BY 操作
    unnecessary_order_by = [op for op in root.findall(".//Sort") if op.find(".//TopSort") is not None]
    if unnecessary_order_by:
        print("警告: 查询中存在不必要的 ORDER BY 操作，这可能会导致性能下降。考虑移除或优化排序操作。")

    # Rule 293: 检查是否存在对外部资源的访问，如链接的服务器
    external_access = root.findall(".//RemoteQuery")
    if external_access:
        print("警告: 查询中存在对外部资源的访问，这可能会导致性能问题。考虑将外部数据本地化或优化远程查询。")

    # Rule 294: 检查是否有过度的并行操作，可能导致资源争用
    excessive_parallelism = [op for op in root.findall(".//Parallelism") if
                             float(op.get('EstimatedTotalSubtreeCost', '0')) < 1.0]
    if excessive_parallelism:
        print("警告: 查询中存在过度的并行操作，这可能会导致资源争用和性能问题。考虑减少并行度或优化查询逻辑。")

    # Rule 295: 检查是否存在大量的空值检查操作
    null_checks = root.findall(".//IsNull")
    if null_checks:
        print("警告: 查询中存在大量的空值检查操作，这可能会导致性能下降。考虑优化查询逻辑或使用其他方法处理空值。")

    # Rule 296: 检查是否有大量的数据分区操作，可能导致数据碎片和性能问题
    partition_ops = root.findall(".//PartitionRange")
    if partition_ops:
        print("警告: 查询中存在大量的数据分区操作，这可能会导致数据碎片和性能问题。考虑优化数据分区策略或查询逻辑。")

    # Rule 297: 检查是否存在不必要的数据聚合操作
    unnecessary_aggregation = [op for op in root.findall(".//Aggregate") if op.get('GroupBy') is None]
    if unnecessary_aggregation:
        print("警告: 查询中存在不必要的数据聚合操作，这可能会导致性能下降。考虑移除或优化聚合操作。")

    # Rule 298: 检查是否有大量的数据合并操作，可能导致性能问题
    merge_ops = root.findall(".//Merge")
    if merge_ops:
        print("警告: 查询中存在大量的数据合并操作，这可能会导致性能问题。考虑优化数据合并策略或查询逻辑。")

    # Rule 299: 检查是否存在大量的动态数据操作
    dynamic_ops = root.findall(".//Dynamic")
    if dynamic_ops:
        print("警告: 查询中存在大量的动态数据操作，这可能会导致性能问题。考虑优化动态数据策略或查询逻辑。")

    # Rule 300: 检查是否有对非优化视图的访问
    non_optimized_views = [op for op in root.findall(".//View") if op.get('Optimized') == 'false']
    if non_optimized_views:
        print("警告: 查询中存在对非优化视图的访问，这可能会导致性能问题。考虑优化视图或查询逻辑。")

    # Rule 301: 检查Top操作，可能意味着查询不高效
    top_operations = root.findall(".//RelOp[@PhysicalOp='Top']")
    if top_operations:
        print("警告: 查询中存在Top操作，考虑进一步优化查询。")

