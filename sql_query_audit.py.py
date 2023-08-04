import pyodbc

# 审核SQL查询的函数
def audit_query(query):
    issues = []  # 初始化issues为一个空列表
    #  规则1：检查是否使用了NOLOCK
    if "NOLOCK" in query:
        print("警告: 使用NOLOCK可能会读取到未提交的数据。确保你知道你在做什么。")

    # 规则2：检查是否使用了新的字符串连接方法
    if "+" in query:
        print("提示: 考虑使用CONCAT或STRING_AGG来连接字符串，它们提供了更好的性能和功能。")

    # 规则3：检查是否使用了旧的JOIN语法
    if "," in query and "WHERE" in query:
        print("警告: 避免使用旧的JOIN语法，使用明确的JOIN语句，如INNER JOIN、LEFT JOIN等。")

    # 规则4：检查是否使用了SET ROWCOUNT
    if "SET ROWCOUNT" in query:
        print("警告: SET ROWCOUNT已被弃用，考虑使用TOP子句。")

    # 规则5：检查是否使用了星号(*)来选择所有列
    if "SELECT *" in query:
        print("警告: 使用具体的列名而不是星号(*)来选择列，以提高性能和可读性。")

    # 规则6：检查是否使用了非SARGable查询
    if "LIKE '%value%'" in query or "DATEPART" in query:
        print("警告: 避免使用非SARGable查询，它们可能会导致性能问题。")

    # 规则7：检查是否使用了表变量而不是临时表
    if "DECLARE @" in query:
        print("提示: 对于大量数据，考虑使用临时表而不是表变量，因为临时表可以有索引。")

    # 规则8：检查是否使用了CURSOR
    if "CURSOR" in query:
        print("警告: 尽量避免使用CURSOR，因为它们通常比集合操作慢。")

    # 规则9：检查是否使用了动态SQL
    if "sp_executesql" in query or "EXEC(" in query:
        print("警告: 使用动态SQL时要小心，确保正确地参数化查询以避免SQL注入。")

    # 规则10：检查是否使用了非持久化的计算列
    if "AS" in query and "PERSISTED" not in query:
        print("提示: 考虑使用PERSISTED关键字使计算列持久化，以提高查询性能。")

    # 规则11：检查是否使用了索引提示
    if "WITH (INDEX(" in query:
        print("警告: 使用索引提示可能会影响查询优化器的决策。确保你知道你在做什么。")

    # 规则12：检查是否使用了表扫描
    if "TABLE SCAN" in query:
        print("警告: 表扫描可能会影响性能，考虑优化查询或添加适当的索引。")

    # 规则13：检查LIKE操作符的使用，特别是以%开始的模式
    if "LIKE '%_" in query or "LIKE '%" in query:
       print("警告: 使用LIKE操作符并且模式以%开始可能会导致性能问题，考虑优化查询。")

    # 规则14：检查子查询的使用
    if "(" in query and "SELECT" in query:
        print("警告: 子查询可能导致性能问题，考虑是否可以使用JOIN重写。")

    # 规则15：检查WHERE子句中的函数使用
    if "WHERE UPPER(" in query or "WHERE LOWER(" in query:
        print("警告: 在WHERE子句中使用函数可能影响索引的使用。")

    # 规则16：检查HAVING子句的使用
    if "HAVING" in query:
        print("警告: 过度使用HAVING子句可能导致性能问题。")

    # 规则17：检查ORDER BY使用非索引列
    if "ORDER BY" in query and not "WITH INDEX(" in query:
        print("警告: 使用非索引列进行排序可能增加排序的开销。")

    # 规则18：检查DISTINCT的过度使用
    if "SELECT DISTINCT" in query:
        print("警告: 过度使用DISTINCT可能导致额外的去重开销。")
    
    # 规则19：检查使用前导通配符
    if "LIKE '%value'" in query:
        print("警告: 使用前导通配符可能导致全表扫描。")

    # 规则20：检查FLOAT和REAL数据类型的使用
    if "FLOAT" in query or "REAL" in query:
        print("警告: FLOAT和REAL数据类型可能导致不精确的比较。")

    # 规则21：检查WHERE子句中的非确定性函数使用
    if "WHERE GETDATE()" in query:
        print("警告: 在WHERE子句中使用非确定性函数可能阻止使用索引。")

    # 规则22：检查@@IDENTITY的使用
    if "@@IDENTITY" in query:
        print("警告: 使用@@IDENTITY可能返回错误的值。考虑使用SCOPE_IDENTITY()或IDENT_CURRENT('table_name')。")

    # 规则23：检查RECOMPILE查询提示的使用
    if "OPTION (RECOMPILE)" in query:
        print("警告: 不要使用RECOMPILE查询提示，除非有充分的理由。")

    # 规则13：检查是否使用了视图
    if "VIEW" in query:
        print("提示: 使用视图可以提高查询的可读性，但可能会影响性能。")

    # 规则14：检查是否使用了大量的自连接
    if query.count("JOIN") > 3:
        print("警告: 使用大量的自连接可能会导致性能问题。")

    # 规则15：检查是否使用了FOR XML或FOR JSON
    if "FOR XML" in query or "FOR JSON" in query:
        print("提示: 使用FOR XML或FOR JSON可以将结果转换为XML或JSON格式。")

    # 规则16：检查是否使用了大量的CASE语句
    if query.count("CASE") > 3:
        print("警告: 使用大量的CASE语句可能会影响性能。")

    # 规则17：检查是否使用了递归公共表表达式
    if "WITH" in query and "UNION ALL" in query:
        print("提示: 使用递归公共表表达式可以处理层次结构数据，但可能会影响性能。")

    # 规则18：检查是否使用了非持久化的计算列
    if "COMPUTE" in query:
        print("警告: 使用COMPUTE生成报告格式的输出，但它已被弃用。")

    # 规则19：检查是否使用了WAITFOR DELAY或WAITFOR TIME
    if "WAITFOR DELAY" in query or "WAITFOR TIME" in query:
        print("警告: 使用WAITFOR可以引入延迟，确保你知道你在做什么。")

    # 规则20：检查是否使用了OPENQUERY或OPENROWSET
    if "OPENQUERY" in query or "OPENROWSET" in query:
        print("警告: 使用OPENQUERY或OPENROWSET可以查询其他服务器，但可能会引入安全风险。")

    # 规则21：检查是否使用了xp_cmdshell存储过程
    if "xp_cmdshell" in query:
        print("警告: xp_cmdshell可以执行操作系统命令，但可能会引入安全风险。")

    # 规则22：检查是否使用了大量的嵌套子查询
    if query.count("SELECT") > 5:
        print("警告: 使用大量的嵌套子查询可能会导致性能问题。")

    # 规则23：检查是否使用了不推荐的旧式连接语法
    if "=" in query and "JOIN" not in query:
        print("警告: 使用旧式连接语法可能会导致不可预测的结果。")

    # 规则24：检查是否使用了分区函数或方案
    if "PARTITION BY" in query:
        print("提示: 使用分区可以提高查询和管理性能。")

    # 规则25：检查是否使用了大量的变量或参数
    if query.count("@") > 10:
        print("警告: 使用大量的变量或参数可能会使查询变得复杂。")

    # 规则26：检查是否使用了全文搜索
    if "CONTAINS" in query or "FREETEXT" in query:
        print("提示: 使用全文搜索可以提高文本查询的性能。")

    # 规则27：检查是否使用了大型对象数据类型
    if "TEXT" in query or "NTEXT" in query or "IMAGE" in query:
        print("警告: TEXT, NTEXT, 和 IMAGE 数据类型已被弃用，考虑使用VARCHAR(MAX), NVARCHAR(MAX) 和 VARBINARY(MAX)。")

    # 规则28：检查是否使用了非确定性函数
    if "NEWID()" in query or "RAND()" in query:
        print("警告: 使用非确定性函数可能会导致不可预测的结果。")

    # 规则29：检查是否使用了CLR集成
    if "ASSEMBLY" in query:
        print("提示: 使用CLR集成可以执行.NET代码，但需要确保安全性。")

    # 规则30：检查是否使用了表值参数
    if "READONLY" in query:
        print("提示: 使用表值参数可以传递多行数据给存储过程或函数。")

    # 规则31：检查是否使用了不推荐的系统存储过程
    if "sp_OA" in query:
        print("警告: sp_OA* 系列的系统存储过程已被弃用，考虑使用其他方法。")

    # 规则32：检查是否使用了不推荐的SET选项
    if "SET ANSI_NULLS OFF" in query or "SET QUOTED_IDENTIFIER OFF" in query:
        print("警告: 使用不推荐的SET选项可能会导致不可预测的结果。")

    # 规则33：检查是否使用了不安全的配置选项
    if "xp_cmdshell" in query or "sp_configure 'show advanced options', 1" in query:
        print("警告: 修改配置选项可能会引入安全风险。")

    # 规则34：检查是否使用了不推荐的旧式TOP语法
    if "TOP 100 PERCENT" in query:
        print("警告: 使用旧式的TOP 100 PERCENT语法可能不会按预期工作。")

    # 规则35：检查是否使用了不必要的CAST或CONVERT
    if "CAST(0 AS" in query or "CONVERT(INT, 0)" in query:
        print("警告: 使用不必要的CAST或CONVERT可能会影响性能。")

    # 规则36：检查是否使用了不推荐的ISNULL函数
    if "ISNULL" in query:
        print("提示: 考虑使用COALESCE函数代替ISNULL，因为它更加灵活。")

    # 规则37：检查是否使用了不推荐的星号(*)来计数
    if "COUNT(*)" in query:
        print("提示: 考虑使用COUNT(1)代替COUNT(*)，因为它可能更加高效。")

    # 规则38：检查是否使用了不推荐的FLOAT或REAL数据类型
    if "FLOAT" in query or "REAL" in query:
        print("警告: FLOAT和REAL数据类型可能不精确，考虑使用DECIMAL或NUMERIC。")

    # 规则39：检查是否使用了不推荐的@@IDENTITY
    if "@@IDENTITY" in query:
        print("警告: 使用@@IDENTITY可能会返回不正确的值，考虑使用SCOPE_IDENTITY()。")

    # 规则40：检查是否使用了不推荐的LEN函数来计算数据长度
    if "LEN(" in query:
        print("提示: 使用DATALENGTH函数代替LEN，因为它可以返回更准确的长度。")

    # 规则41：检查是否使用了不推荐的远程过程调用(RPC)
    if "sp_serveroption" in query:
        print("警告: 使用远程过程调用(RPC)可能会引入安全风险和性能问题。")

    # 规则42：检查是否使用了不推荐的IDENTITY_INSERT设置
    if "SET IDENTITY_INSERT" in query:
        print("警告: 使用IDENTITY_INSERT可能会导致数据完整性问题。")

    # 规则43：检查是否使用了不推荐的SQL_VARIANT数据类型
    if "SQL_VARIANT" in query:
        print("警告: SQL_VARIANT数据类型可能会导致性能问题和数据不一致。")

    # 规则44：检查是否使用了不推荐的表提示
    if "WITH (TABLOCKX)" in query or "WITH (PAGLOCK)" in query:
        print("警告: 使用某些表提示可能会导致锁定问题。")

    # 规则45：检查是否使用了不推荐的SET选项
    if "SET ARITHABORT" in query or "SET ARITHIGNORE" in query:
        print("警告: 使用某些SET选项可能会导致不可预测的结果。")

    # 规则46：检查是否使用了不推荐的统计函数
    if "FN_STATS" in query:
        print("警告: 使用不推荐的统计函数可能会导致性能问题。")

    # 规则47：检查是否使用了不推荐的系统视图
    if "sysobjects" in query or "syscolumns" in query:
        print("警告: 使用旧的系统视图可能会导致不可预测的结果。考虑使用新的系统视图。")

    # 规则48：检查是否使用了不推荐的全局变量
    if "@@PROCID" in query:
        print("警告: 使用某些全局变量可能会导致不可预测的结果。")

    # 规则49：检查是否使用了不推荐的WAITFOR语句
    if "WAITFOR DELAY '00:00:10'" in query:
        print("警告: 使用WAITFOR DELAY可能会导致不必要的延迟。")

    # 规则50：检查是否使用了不推荐的DEADLOCK_PRIORITY设置
    if "SET DEADLOCK_PRIORITY" in query:
        print("警告: 修改DEADLOCK_PRIORITY可能会导致死锁问题。")

    # 规则51：检查是否使用了不推荐的KILL语句
    if "KILL" in query:
        print("警告: 使用KILL语句可能会导致会话中断。")

    # 规则52：检查是否使用了不推荐的BREAK语句
    if "BREAK" in query:
        print("警告: 使用BREAK语句可能会导致循环或分支提前结束。")

    # 规则53：检查是否使用了不推荐的GOTO语句
    if "GOTO" in query:
        print("警告: 使用GOTO语句可能会导致代码难以理解和维护。")

    # 规则54：检查是否使用了不推荐的PRINT语句
    if "PRINT" in query:
        print("提示: 使用PRINT语句可以输出消息，但可能会影响性能。")

    # 规则55：检查是否使用了不推荐的RAISERROR语句
    if "RAISERROR" in query:
        print("警告: 使用RAISERROR语句可能会导致事务中断。")

    # 规则56：检查是否使用了不推荐的RECONFIGURE语句
    if "RECONFIGURE" in query:
        print("警告: 使用RECONFIGURE可能会导致服务器配置更改。")

    # 规则57：检查是否使用了不推荐的DBCC命令
    if "DBCC" in query:
        print("警告: 使用DBCC命令可能会导致数据库行为更改或性能问题。")

    # 规则58：检查是否使用了不推荐的TRUNCATE TABLE语句
    if "TRUNCATE TABLE" in query:
        print("警告: 使用TRUNCATE TABLE会删除所有行，且不可恢复。")

    # 规则59：检查是否使用了不推荐的DROP语句
    if "DROP" in query:
        print("警告: 使用DROP语句会永久删除对象。")

    # 规则60：检查是否使用了不推荐的SHUTDOWN语句
    if "SHUTDOWN" in query:
        print("警告: 使用SHUTDOWN会关闭SQL Server实例。")

    # 规则61：检查是否使用了不推荐的DENY语句
    if "DENY" in query:
        print("警告: 使用DENY会限制用户访问。确保你知道你在做什么。")

    # 规则62：检查是否使用了不推荐的xp_fixeddrives存储过程
    if "xp_fixeddrives" in query:
        print("提示: 使用xp_fixeddrives可以查看硬盘空间，但可能存在安全风险。")

    # 规则63：检查是否使用了不推荐的xp_dirtree存储过程
    if "xp_dirtree" in query:
        print("警告: 使用xp_dirtree可能会导致安全风险。")

    # 规则64：检查是否使用了不推荐的xp_regread存储过程
    if "xp_regread" in query:
        print("警告: 使用xp_regread可能会导致安全风险。")

    # 规则65：检查是否使用了不推荐的xp_cmdshell存储过程
    if "xp_cmdshell" in query:
        print("警告: 使用xp_cmdshell可能会导致安全风险。")

    # 规则66：检查是否使用了不推荐的xp_loginconfig存储过程
    if "xp_loginconfig" in query:
        print("警告: 使用xp_loginconfig可能会导致安全风险。")

    # 规则67：检查是否使用了不推荐的xp_enumerrorlogs存储过程
    if "xp_enumerrorlogs" in query:
        print("警告: 使用xp_enumerrorlogs可能会导致安全风险。")

    # 规则68：检查是否使用了不推荐的xp_enumgroups存储过程
    if "xp_enumgroups" in query:
        print("警告: 使用xp_enumgroups可能会导致安全风险。")

    # 规则69：检查是否使用了不推荐的xp_logevent存储过程
    if "xp_logevent" in query:
        print("警告: 使用xp_logevent可能会导致安全风险。")

    # 规则70：检查是否使用了不推荐的xp_msver存储过程
    if "xp_msver" in query:
        print("警告: 使用xp_msver可能会导致安全风险。")

    # 规则71：检查是否使用了不推荐的xp_getnetname存储过程
    if "xp_getnetname" in query:
        print("警告: 使用xp_getnetname可能会导致安全风险。")

    # 规则72：检查是否使用了不推荐的xp_availablemedia存储过程
    if "xp_availablemedia" in query:
        print("警告: 使用xp_availablemedia可能会导致安全风险。")

    # 规则73：检查是否使用了不推荐的xp_delete_file存储过程
    if "xp_delete_file" in query:
        print("警告: 使用xp_delete_file可能会导致安全风险。")

    # 规则74：检查是否使用了不推荐的xp_fileexist存储过程
    if "xp_fileexist" in query:
        print("警告: 使用xp_fileexist可能会导致安全风险。")

    # 规则75：检查是否使用了不推荐的xp_servicecontrol存储过程
    if "xp_servicecontrol" in query:
        print("警告: 使用xp_servicecontrol可能会导致安全风险。")

    # 规则76：检查是否使用了不推荐的xp_sscanf存储过程
    if "xp_sscanf" in query:
        print("警告: 使用xp_sscanf可能会导致安全风险。")

    # 规则77：检查是否使用了不推荐的xp_terminate_process存储过程
    if "xp_terminate_process" in query:
        print("警告: 使用xp_terminate_process可能会导致安全风险。")

    # 规则78：检查是否使用了不推荐的xp_grantlogin存储过程
    if "xp_grantlogin" in query:
        print("警告: 使用xp_grantlogin可能会导致安全风险。")

    # 规则79：检查是否使用了不推荐的xp_revokelogin存储过程
    if "xp_revokelogin" in query:
        print("警告: 使用xp_revokelogin可能会导致安全风险。")

    # 规则80：检查是否使用了不推荐的xp_logininfo存储过程
    if "xp_logininfo" in query:
        print("警告: 使用xp_logininfo可能会导致安全风险。")

    # 规则81：检查是否使用了不推荐的xp_instance_regread存储过程
    if "xp_instance_regread" in query:
        print("警告: 使用xp_instance_regread可能会导致安全风险。")

    # 规则82：检查是否使用了不推荐的xp_regwrite存储过程
    if "xp_regwrite" in query:
        print("警告: 使用xp_regwrite可能会导致安全风险。")

    # 规则83：检查是否使用了不推荐的xp_regdeletevalue存储过程
    if "xp_regdeletevalue" in query:
        print("警告: 使用xp_regdeletevalue可能会导致安全风险。")

    # 规则84：检查是否使用了不推荐的xp_regdeletekey存储过程
    if "xp_regdeletekey" in query:
        print("警告: 使用xp_regdeletekey可能会导致安全风险。")

    # 规则85：检查是否使用了不推荐的xp_regremovemultistring存储过程
    if "xp_regremovemultistring" in query:
        print("警告: 使用xp_regremovemultistring可能会导致安全风险。")

    # 规则86：检查是否使用了不推荐的xp_makecab存储过程
    if "xp_makecab" in query:
        print("警告: 使用xp_makecab可能会导致安全风险。")

    # 规则87：检查是否使用了不推荐的xp_ntsec存储过程
    if "xp_ntsec" in query:
        print("警告: 使用xp_ntsec可能会导致安全风险。")

    # 规则88：检查是否使用了不推荐的xp_getfiledetails存储过程
    if "xp_getfiledetails" in query:
        print("警告: 使用xp_getfiledetails可能会导致安全风险。")

    # 规则89：检查是否使用了不推荐的xp_dirtree存储过程
    if "xp_dirtree" in query:
        print("警告: 使用xp_dirtree可能会导致安全风险。")

    # 规则90：检查是否使用了不推荐的xp_fileexist存储过程
    if "xp_fileexist" in query:
        print("警告: 使用xp_fileexist可能会导致安全风险。")

    # 规则91：检查是否使用了不推荐的xp_getnetname存储过程
    if "xp_getnetname" in query:
        print("警告: 使用xp_getnetname可能会导致安全风险。")

    # 规则92：检查是否使用了不推荐的xp_loginconfig存储过程
    if "xp_loginconfig" in query:
        print("警告: 使用xp_loginconfig可能会导致安全风险。")

    # 规则93：检查是否使用了不推荐的xp_msver存储过程
    if "xp_msver" in query:
        print("警告: 使用xp_msver可能会导致安全风险。")

    # 规则94：检查是否使用了不推荐的xp_enumerrorlogs存储过程
    if "xp_enumerrorlogs" in query:
        print("警告: 使用xp_enumerrorlogs可能会导致安全风险。")

    # 规则95：检查是否使用了不推荐的xp_enumgroups存储过程
    if "xp_enumgroups" in query:
        print("警告: 使用xp_enumgroups可能会导致安全风险。")

    # 规则96：检查是否使用了不推荐的xp_logevent存储过程
    if "xp_logevent" in query:
        print("警告: 使用xp_logevent可能会导致安全风险。")

    # 规则97：检查是否使用了不推荐的xp_terminate_process存储过程
    if "xp_terminate_process" in query:
        print("警告: 使用xp_terminate_process可能会导致安全风险。")

    # 规则98：检查是否使用了不推荐的xp_grantlogin存储过程
    if "xp_grantlogin" in query:
        print("警告: 使用xp_grantlogin可能会导致安全风险。")

    # 规则99：检查是否使用了不推荐的xp_revokelogin存储过程
    if "xp_revokelogin" in query:
        print("警告: 使用xp_revokelogin可能会导致安全风险。")

    # 规则100：检查是否使用了不推荐的xp_instance_regread存储过程
    if "xp_instance_regread" in query:
        print("警告: 使用xp_instance_regread可能会导致安全风险。")

    # 规则101：检查是否使用了LEFT JOIN而不是INNER JOIN
    if "LEFT JOIN" in query:
        print("提示: 使用LEFT JOIN可能会返回NULL值，确保这是预期的结果。")

    # 规则102：检查是否使用了CROSS JOIN
    if "CROSS JOIN" in query:
        print("警告: 使用CROSS JOIN可能会导致大量的结果，确保这是预期的行为。")

    # 规则103：检查是否使用了非SARGable查询
    if "LIKE '%value%'" in query:
        print("警告: 使用非SARGable查询可能会导致性能下降。")

    # 规则104：检查是否使用了ORDER BY RAND()
    if "ORDER BY RAND()" in query:
        print("警告: 使用ORDER BY RAND()可能会导致性能问题。")

    # 规则105：检查是否使用了子查询而不是JOIN
    if "WHERE column IN (SELECT column FROM table)" in query:
        print("提示: 考虑使用JOIN代替子查询，可能会提高性能。")

    # 规则106：检查是否使用了非参数化的查询
    if "'" in query and "WHERE" in query and "=" in query:
        print("警告: 使用非参数化的查询可能会导致SQL注入风险。")

    # 规则107：检查是否使用了DISTINCT
    if "DISTINCT" in query:
        print("提示: 使用DISTINCT可能会影响性能，确保其是必要的。")

    # 规则108：检查是否使用了COUNT(DISTINCT column)
    if "COUNT(DISTINCT" in query:
        print("提示: 使用COUNT(DISTINCT column)可能会影响性能。")

    # 规则109：检查是否使用了UNION而不是UNION ALL
    if "UNION" in query and "UNION ALL" not in query:
        print("提示: 使用UNION可能会影响性能，如果不需要去重，考虑使用UNION ALL。")

    # 规则110：检查是否使用了多个OR条件
    if query.count(" OR ") > 2:
        print("警告: 使用多个OR条件可能会导致性能问题。")

    # 规则111：检查是否使用了CASE语句在WHERE子句中
    if "WHERE CASE" in query:
        print("警告: 在WHERE子句中使用CASE语句可能会导致性能问题。")

    # 规则112：检查是否使用了HAVING子句而不是WHERE子句
    if "HAVING" in query and "WHERE" not in query:
        print("警告: 使用HAVING子句而不是WHERE子句可能会导致性能问题。")

    # 规则113：检查是否使用了表变量而不是临时表
    if "DECLARE @TableName TABLE" in query:
        print("提示: 考虑使用临时表代替表变量，可能会提高性能。")

    # 规则114：检查是否使用了CURSOR
    if "DECLARE CURSOR" in query:
        print("警告: 使用CURSOR可能会导致性能问题，考虑使用集合操作代替。")

    # 规则115：检查是否使用了大量的嵌套子查询
    if query.count("(SELECT") > 3:
        print("警告: 使用大量的嵌套子查询可能会导致性能问题。")

   # 规则116：检查是否使用了不推荐的*选择
    if "SELECT *" in query:
        print("警告: 使用SELECT *可能会导致性能问题和未预期的结果。建议明确列出所需的列。")

    # 规则117：检查是否使用了不推荐的FLOAT数据类型
    if " FLOAT" in query:
        print("提示: 使用FLOAT数据类型可能会导致精度问题。考虑使用DECIMAL或NUMERIC。")

    # 规则118：检查是否使用了不推荐的IMAGE数据类型
    if " IMAGE" in query:
        print("警告: IMAGE数据类型已被弃用。考虑使用VARBINARY(MAX)。")

    # 规则119：检查是否使用了不推荐的TEXT数据类型
    if " TEXT" in query:
        print("警告: TEXT数据类型已被弃用。考虑使用VARCHAR(MAX)。")

    # 规则120：检查是否使用了不推荐的NTEXT数据类型
    if " NTEXT" in query:
        print("警告: NTEXT数据类型已被弃用。考虑使用NVARCHAR(MAX)。")

    # 规则121：检查是否使用了不推荐的非ANSI JOIN语法
    if "," in query and "WHERE" in query and "=" in query and "JOIN" not in query:
        print("警告: 使用非ANSI JOIN语法可能会导致不可预测的结果。考虑使用ANSI JOIN语法。")

    # 规则122：检查是否使用了不推荐的非ANSI NOT NULL语法
    if "NOT NULL" not in query and "ISNULL" in query:
        print("警告: 使用非ANSI NOT NULL语法可能会导致不可预测的结果。考虑使用ANSI NOT NULL语法。")

    # 规则123：检查是否使用了不推荐的SET ROWCOUNT
    if "SET ROWCOUNT" in query:
        print("警告: 使用SET ROWCOUNT可能会导致不可预测的结果。考虑使用TOP子句。")

    # 规则124：检查是否使用了不推荐的自动更新统计信息
    if "AUTO_UPDATE_STATISTICS" in query:
        print("提示: 确保你了解自动更新统计信息的影响，它可能会影响查询性能。")

    # 规则125：检查是否使用了不推荐的自动创建统计信息
    if "AUTO_CREATE_STATISTICS" in query:
        print("提示: 确保你了解自动创建统计信息的影响，它可能会影响查询性能。")

    # 规则126：检查是否使用了不推荐的表变量
    if "DECLARE @" in query and " TABLE" in query:
        print("提示: 表变量可能不会优化得很好。在需要性能的场景中，考虑使用临时表。")

    # 规则127：检查是否使用了不推荐的隐式转换
    if "=" in query and "CONVERT(" not in query and "CAST(" not in query:
        print("提示: 确保你的查询中没有隐式转换，它可能会影响性能。")

    # 规则128：检查是否使用了不推荐的非SARGable查询
    if "DATEPART(" in query or "YEAR(" in query or "MONTH(" in query or "DAY(" in query:
        print("警告: 使用这些日期函数可能会导致非SARGable查询，影响性能。")

    # 规则129：检查是否使用了不推荐的视图嵌套
    if "SELECT" in query and "FROM (SELECT" in query:
        print("警告: 视图嵌套可能会导致性能问题。")

    # 规则130：检查是否使用了不推荐的多表UPDATE
    if "UPDATE" in query and "JOIN" in query:
        print("警告: 使用多表UPDATE可能会导致复杂性和性能问题。")

    # 规则131：检查是否使用了不推荐的全局临时表
    if "##" in query:
        print("警告: 使用全局临时表可能会导致数据隔离问题。")

    # 规则132：检查是否使用了不推荐的sp_executesql
    if "sp_executesql" in query:
        print("警告: 使用sp_executesql可能会导致SQL注入风险。")

    # 规则133：检查是否使用了不推荐的WAITFOR DELAY
    if "WAITFOR DELAY" in query:
        print("警告: 使用WAITFOR DELAY可能会导致性能问题。")

    # 规则134：检查是否使用了不推荐的DBCC语句
    if "DBCC" in query:
        print("警告: 使用DBCC语句可能会导致性能和数据完整性问题。")

    # 规则135：检查是否使用了不推荐的OPENQUERY
    if "OPENQUERY" in query:
        print("警告: 使用OPENQUERY可能会导致性能问题和SQL注入风险。")

    # 规则136：检查是否使用了不推荐的OPENROWSET
    if "OPENROWSET" in query:
        print("警告: 使用OPENROWSET可能会导致性能问题和SQL注入风险。")

    # 规则137：检查是否使用了不推荐的OPENDATASOURCE
    if "OPENDATASOURCE" in query:
        print("警告: 使用OPENDATASOURCE可能会导致性能问题和SQL注入风险。")

    # 规则138：检查是否使用了不推荐的动态SQL
    if "EXEC(" in query or "EXECUTE(" in query:
        print("警告: 使用动态SQL可能会导致SQL注入风险。")

    # 规则139：检查是否使用了不推荐的表值函数
    if "CROSS APPLY" in query or "OUTER APPLY" in query:
        print("提示: 使用表值函数可能会导致性能问题。")

    # 规则140：检查是否使用了不推荐的非确定性函数
    if "NEWID()" in query or "RAND()" in query:
        print("警告: 使用非确定性函数可能会导致性能问题和不可预测的结果。")

    # 规则141：检查是否使用了不推荐的SET语句
    if "SET " in query and "=" in query:
        print("提示: 使用SET语句可能会导致不可预测的会话设置。")

    # 规则142：检查是否使用了不推荐的RECOMPILE查询提示
    if "OPTION (RECOMPILE)" in query:
        print("提示: 使用RECOMPILE查询提示可能会导致性能问题。")

    # 规则143：检查是否使用了不推荐的FOR XML语法
    if "FOR XML" in query:
        print("提示: 使用FOR XML语法可能会导致性能问题。")

    # 规则144：检查是否使用了不推荐的FOR JSON语法
    if "FOR JSON" in query:
        print("提示: 使用FOR JSON语法可能会导致性能问题。")

    # 规则145：检查是否使用了不推荐的递归查询
    if "WITH RECURSIVE" in query or "CTE" in query and "JOIN" in query and "CTE" in query:
        print("警告: 使用递归查询可能会导致性能问题。")

    # 规则146：检查是否使用了不推荐的非聚集索引
    if "NONCLUSTERED" in query and "INDEX" in query:
        print("提示: 使用非聚集索引可能会导致性能问题。")

    # 规则147：检查是否使用了不推荐的表扫描
    if "TABLE SCAN" in query:
        print("警告: 使用表扫描可能会导致性能问题。")

    # 规则148：检查是否使用了不推荐的索引扫描
    if "INDEX SCAN" in query:
        print("警告: 使用索引扫描可能会导致性能问题。")

    # 规则149：检查是否使用了不推荐的索引查找
    if "INDEX SEEK" in query and "LOOKUP" in query:
        print("警告: 使用索引查找可能会导致性能问题。")

    # 规则150：检查是否使用了不推荐的UPDATE锁
    if "WITH (UPDLOCK)" in query:
        print("警告: 使用UPDATE锁可能会导致死锁。")

     # 规则151：检查是否使用了不推荐的NOLOCK查询提示
    if "WITH (NOLOCK)" in query:
        print("警告: 使用NOLOCK查询提示可能会导致脏读。")

    # 规则152：检查是否使用了不推荐的FORCESEEK查询提示
    if "WITH (FORCESEEK)" in query:
        print("提示: 使用FORCESEEK查询提示可能会导致性能问题。")

    # 规则153：检查是否使用了不推荐的FORCESCAN查询提示
    if "WITH (FORCESCAN)" in query:
        print("提示: 使用FORCESCAN查询提示可能会导致性能问题。")

    # 规则154：检查是否使用了不推荐的PAGELOCK查询提示
    if "WITH (PAGELOCK)" in query:
        print("警告: 使用PAGELOCK查询提示可能会导致锁争用。")

    # 规则155：检查是否使用了不推荐的ROWLOCK查询提示
    if "WITH (ROWLOCK)" in query:
        print("警告: 使用ROWLOCK查询提示可能会导致锁争用。")

    # 规则156：检查是否使用了不推荐的READPAST查询提示
    if "WITH (READPAST)" in query:
        print("提示: 使用READPAST查询提示可能会导致跳过锁定的行。")

    # 规则157：检查是否使用了不推荐的READCOMMITTED查询提示
    if "WITH (READCOMMITTED)" in query:
        print("提示: 使用READCOMMITTED查询提示可能会导致读已提交的数据。")

    # 规则158：检查是否使用了不推荐的READCOMMITTEDLOCK查询提示
    if "WITH (READCOMMITTEDLOCK)" in query:
        print("提示: 使用READCOMMITTEDLOCK查询提示可能会导致读已提交的数据并使用锁。")

    # 规则159：检查是否使用了不推荐的READUNCOMMITTED查询提示
    if "WITH (READUNCOMMITTED)" in query:
        print("警告: 使用READUNCOMMITTED查询提示可能会导致脏读。")

    # 规则160：检查是否使用了不推荐的REPEATABLEREAD查询提示
    if "WITH (REPEATABLEREAD)" in query:
        print("提示: 使用REPEATABLEREAD查询提示可能会导致重复读取。")

    # 规则161：检查是否使用了不推荐的SERIALIZABLE查询提示
    if "WITH (SERIALIZABLE)" in query:
        print("警告: 使用SERIALIZABLE查询提示可能会导致序列化隔离级别。")

    # 规则162：检查是否使用了不推荐的TABLOCK查询提示
    if "WITH (TABLOCK)" in query:
        print("警告: 使用TABLOCK查询提示可能会导致表级锁。")

    # 规则163：检查是否使用了不推荐的TABLOCKX查询提示
    if "WITH (TABLOCKX)" in query:
        print("警告: 使用TABLOCKX查询提示可能会导致表级排他锁。")

    # 规则164：检查是否使用了不推荐的UPDLOCK查询提示
    if "WITH (UPDLOCK)" in query:
        print("警告: 使用UPDLOCK查询提示可能会导致更新锁。")

    # 规则165：检查是否使用了不推荐的XLOCK查询提示
    if "WITH (XLOCK)" in query:
        print("警告: 使用XLOCK查询提示可能会导致排他锁。")

    # 规则166：检查是否使用了不推荐的PAGLOCK查询提示
    if "WITH (PAGLOCK)" in query:
        print("警告: 使用PAGLOCK查询提示可能会导致页面级锁。")

    # 规则167：检查是否使用了不推荐的ROWLOCK查询提示
    if "WITH (ROWLOCK)" in query:
        print("警告: 使用ROWLOCK查询提示可能会导致行级锁。")

    # 规则168：检查是否使用了不推荐的NOEXPAND查询提示
    if "WITH (NOEXPAND)" in query:
        print("提示: 使用NOEXPAND查询提示可能会导致不展开视图。")

    # 规则169：检查是否使用了不推荐的KEEPFIXED PLAN查询提示
    if "OPTION (KEEPFIXED PLAN)" in query:
        print("提示: 使用KEEPFIXED PLAN查询提示可能会导致保持固定的查询计划。")

    # 规则170：检查是否使用了不推荐的KEEP PLAN查询提示
    if "OPTION (KEEP PLAN)" in query:
        print("提示: 使用KEEP PLAN查询提示可能会导致保持查询计划。")

    # 规则171：检查是否使用了不推荐的LOOP JOIN查询提示
    if "OPTION (LOOP JOIN)" in query:
        print("提示: 使用LOOP JOIN查询提示可能会导致循环连接。")

    # 规则172：检查是否使用了不推荐的MERGE JOIN查询提示
    if "OPTION (MERGE JOIN)" in query:
        print("提示: 使用MERGE JOIN查询提示可能会导致合并连接。")

    # 规则173：检查是否使用了不推荐的HASH JOIN查询提示
    if "OPTION (HASH JOIN)" in query:
        print("提示: 使用HASH JOIN查询提示可能会导致哈希连接。")

    # 规则174：检查是否使用了不推荐的FAST查询提示
    if "OPTION (FAST" in query:
        print("提示: 使用FAST查询提示可能会导致优化为快速响应。")

    # 规则175：检查是否使用了不推荐的FORCE ORDER查询提示
    if "OPTION (FORCE ORDER)" in query:
        print("警告: 使用FORCE ORDER查询提示可能会导致强制执行连接顺序。")

    # 规则176：检查是否使用了不推荐的OPTIMIZE FOR查询提示
    if "OPTION (OPTIMIZE FOR" in query:
        print("提示: 使用OPTIMIZE FOR查询提示可能会导致为特定值优化。")

    # 规则177：检查是否使用了不推荐的ROBUST PLAN查询提示
    if "OPTION (ROBUST PLAN)" in query:
        print("提示: 使用ROBUST PLAN查询提示可能会导致生成健壮的查询计划。")

    # 规则178：检查是否使用了不推荐的USE PLAN查询提示
    if "OPTION (USE PLAN)" in query:
        print("警告: 使用USE PLAN查询提示可能会导致使用特定的查询计划。")

    # 规则179：检查是否使用了不推荐的IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX查询提示
    if "OPTION (IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX)" in query:
        print("警告: 使用IGNORE_NONCLUSTERED_COLUMNSTORE_INDEX查询提示可能会导致忽略非聚集列存储索引。")

    # 规则180：检查是否使用了不推荐的MAXDOP查询提示
    if "OPTION (MAXDOP" in query:
        print("提示: 使用MAXDOP查询提示可能会导致限制并行度。")

    # 规则181：检查是否使用了不推荐的MAXRECURSION查询提示
    if "OPTION (MAXRECURSION" in query:
        print("提示: 使用MAXRECURSION查询提示可能会限制递归的深度。")

    # 规则182：检查是否使用了不推荐的MIN_GRANT_PERCENT查询提示
    if "OPTION (MIN_GRANT_PERCENT" in query:
        print("提示: 使用MIN_GRANT_PERCENT查询提示可能会设置最小的内存授权百分比。")

    # 规则183：检查是否使用了不推荐的MAX_GRANT_PERCENT查询提示
    if "OPTION (MAX_GRANT_PERCENT" in query:
        print("提示: 使用MAX_GRANT_PERCENT查询提示可能会设置最大的内存授权百分比。")

    # 规则184：检查是否使用了不推荐的NO_PERFORMANCE_SPOOL查询提示
    if "OPTION (NO_PERFORMANCE_SPOOL)" in query:
        print("提示: 使用NO_PERFORMANCE_SPOOL查询提示可能会禁用性能池。")

    # 规则185：检查是否使用了不推荐的RECOMPILE查询提示
    if "OPTION (RECOMPILE)" in query:
        print("警告: 使用RECOMPILE查询提示可能会导致每次执行都重新编译。")

    # 规则186：检查是否使用了不推荐的USE HINT查询提示
    if "OPTION (USE HINT" in query:
        print("提示: 使用USE HINT查询提示可能会提供特定的查询优化器行为。")

    # 规则187：检查是否使用了不推荐的WAIT_AT_LOW_PRIORITY查询提示
    if "OPTION (WAIT_AT_LOW_PRIORITY)" in query:
        print("提示: 使用WAIT_AT_LOW_PRIORITY查询提示可能会在低优先级下等待。")

    # 规则188：检查是否使用了不推荐的NO_WAIT查询提示
    if "OPTION (NO_WAIT)" in query:
        print("警告: 使用NO_WAIT查询提示可能会导致不等待并立即返回。")

    # 规则189：检查是否使用了不推荐的ROWLOCK查询提示
    if "OPTION (ROWLOCK)" in query:
        print("警告: 使用ROWLOCK查询提示可能会导致行级锁定。")

    # 规则190：检查是否使用了不推荐的PAGLOCK查询提示
    if "OPTION (PAGLOCK)" in query:
        print("警告: 使用PAGLOCK查询提示可能会导致页面级锁定。")

    # 规则191：检查是否使用了不推荐的TABLOCK查询提示
    if "OPTION (TABLOCK)" in query:
        print("警告: 使用TABLOCK查询提示可能会导致表级锁定。")

    # 规则192：检查是否使用了不推荐的TABLOCKX查询提示
    if "OPTION (TABLOCKX)" in query:
        print("警告: 使用TABLOCKX查询提示可能会导致表级排他锁。")

    # 规则193：检查是否使用了不推荐的READPAST查询提示
    if "OPTION (READPAST)" in query:
        print("提示: 使用READPAST查询提示可能会跳过锁定的行。")

    # 规则194：检查是否使用了不推荐的READCOMMITTED查询提示
    if "OPTION (READCOMMITTED)" in query:
        print("提示: 使用READCOMMITTED查询提示可能会读取已提交的数据。")

    # 规则195：检查是否使用了不推荐的READUNCOMMITTED查询提示
    if "OPTION (READUNCOMMITTED)" in query:
        print("警告: 使用READUNCOMMITTED查询提示可能会导致脏读。")

    # 规则196：检查是否使用了不推荐的REPEATABLEREAD查询提示
    if "OPTION (REPEATABLEREAD)" in query:
        print("提示: 使用REPEATABLEREAD查询提示可能会导致重复读取。")

    # 规则197：检查是否使用了不推荐的SERIALIZABLE查询提示
    if "OPTION (SERIALIZABLE)" in query:
        print("警告: 使用SERIALIZABLE查询提示可能会导致序列化隔离级别。")

    # 规则198：检查是否使用了不推荐的XLOCK查询提示
    if "OPTION (XLOCK)" in query:
        print("警告: 使用XLOCK查询提示可能会导致排他锁。")

    # 规则199：检查是否使用了不推荐的NOEXPAND查询提示
    if "OPTION (NOEXPAND)" in query:
        print("提示: 使用NOEXPAND查询提示可能会导致不展开索引视图。")

    # 规则200：检查是否使用了不推荐的FORCE ORDER查询提示
    if "OPTION (FORCE ORDER)" in query:
        print("警告: 使用FORCE ORDER查询提示可能会导致强制执行连接顺序。")
    
   # 规则201：检查是否使用了不推荐的REMOTE查询提示
    if "OPTION (REMOTE)" in query:
        print("警告: 使用REMOTE查询提示可能会导致远程查询。")

    # 规则202：检查是否使用了不推荐的KEEPIDENTITY查询提示
    if "OPTION (KEEPIDENTITY)" in query:
        print("提示: 使用KEEPIDENTITY查询提示可能会保持源数据的标识值。")

    # 规则203：检查是否使用了不推荐的KEEPDEFAULTS查询提示
    if "OPTION (KEEPDEFAULTS)" in query:
        print("提示: 使用KEEPDEFAULTS查询提示可能会保持目标表的默认值。")

    # 规则204：检查是否使用了不推荐的IGNORE_CONSTRAINTS查询提示
    if "OPTION (IGNORE_CONSTRAINTS)" in query:
        print("警告: 使用IGNORE_CONSTRAINTS查询提示可能会忽略约束。")

    # 规则205：检查是否使用了不推荐的IGNORE_TRIGGERS查询提示
    if "OPTION (IGNORE_TRIGGERS)" in query:
        print("警告: 使用IGNORE_TRIGGERS查询提示可能会忽略触发器。")

    # 规则206：检查是否使用了不推荐的ALLOW_PAGE_LOCKS查询提示
    if "OPTION (ALLOW_PAGE_LOCKS)" in query:
        print("提示: 使用ALLOW_PAGE_LOCKS查询提示可能会允许页面锁。")

    # 规则207：检查是否使用了不推荐的ALLOW_ROW_LOCKS查询提示
    if "OPTION (ALLOW_ROW_LOCKS)" in query:
        print("提示: 使用ALLOW_ROW_LOCKS查询提示可能会允许行锁。")

    # 规则208：检查是否使用了不推荐的OPTIMIZE FOR UNKNOWN查询提示
    if "OPTION (OPTIMIZE FOR UNKNOWN)" in query:
        print("提示: 使用OPTIMIZE FOR UNKNOWN查询提示可能会为未知的参数值优化。")

    # 规则209：检查是否使用了不推荐的NOLOCK查询提示
    if "WITH (NOLOCK)" in query:
        print("警告: 使用NOLOCK查询提示可能会导致脏读。")

    # 规则210：检查是否使用了不推荐的INDEX查询提示
    if "WITH (INDEX" in query:
        print("提示: 使用INDEX查询提示可能会强制使用特定的索引。")

    # 规则211：检查是否使用了不推荐的FORCESEEK查询提示
    if "WITH (FORCESEEK)" in query:
        print("提示: 使用FORCESEEK查询提示可能会强制使用索引查找。")

    # 规则212：检查是否使用了不推荐的FORCESCAN查询提示
    if "WITH (FORCESCAN)" in query:
        print("提示: 使用FORCESCAN查询提示可能会强制使用索引扫描。")

    # 规则213：检查是否使用了不推荐的NOEXPAND查询提示
    if "WITH (NOEXPAND)" in query:
        print("提示: 使用NOEXPAND查询提示可能会导致不展开索引视图。")

    # 规则214：检查是否使用了不推荐的READCOMMITTEDLOCK查询提示
    if "WITH (READCOMMITTEDLOCK)" in query:
        print("提示: 使用READCOMMITTEDLOCK查询提示可能会导致读已提交的隔离级别。")

    # 规则215：检查是否使用了不推荐的PAGLOCK查询提示
    if "WITH (PAGLOCK)" in query:
        print("警告: 使用PAGLOCK查询提示可能会导致页面锁。")

    # 规则216：检查是否使用了不推荐的ROWLOCK查询提示
    if "WITH (ROWLOCK)" in query:
        print("警告: 使用ROWLOCK查询提示可能会导致行锁。")

    # 规则217：检查是否使用了不推荐的UPDLOCK查询提示
    if "WITH (UPDLOCK)" in query:
        print("警告: 使用UPDLOCK查询提示可能会导致更新锁。")

    # 规则218：检查是否使用了不推荐的XLOCK查询提示
    if "WITH (XLOCK)" in query:
        print("警告: 使用XLOCK查询提示可能会导致排他锁。")

    # 规则219：检查是否使用了不推荐的TABLOCK查询提示
    if "WITH (TABLOCK)" in query:
        print("警告: 使用TABLOCK查询提示可能会导致表锁。")

    # 规则220：检查是否使用了不推荐的TABLOCKX查询提示
    if "WITH (TABLOCKX)" in query:
        print("警告: 使用TABLOCKX查询提示可能会导致表排他锁。")

    # 规则221：检查是否使用了不推荐的HOLDLOCK查询提示
    if "WITH (HOLDLOCK)" in query:
        print("警告: 使用HOLDLOCK查询提示可能会导致保持锁。")

    return issues
