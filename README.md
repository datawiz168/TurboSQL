# sql-server-sql-rule
基于规则的sql server sql 审计调优工具

    将主要的四个审核函数（SQL语句审核、执行计划审核、索引审核、表结构审核）分别放入四个不同的 .py 文件中。
    在主函数中导入并调用这四个 .py 文件。
    
   1.sql_query_audit.py 包含 audit_query 函数和相关的辅助函数。
   2.execution_plan_audit.py 包含 get_execution_plan 和 audit_execution_plan 函数。
   3.indexes_audit.py 包含 audit_indexes 函数。
   4.table_structure_audit.py 包含 audit_table_structure 函数。
