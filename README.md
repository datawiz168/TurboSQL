## sql-server-sql-rule

### 基于规则的 SQL Server SQL 审计调优工具

#### 主要功能

此工具提供了四个核心的审计功能，每个功能都被分离到单独的 .py 文件中，主函数则导入并调用这四个文件：

1. **SQL语句审核** (`sql_query_audit.py`):
   - 主函数: `audit_query`
   - 功能: 审核SQL查询的安全和性能问题。

2. **执行计划审核** (`execution_plan_audit.py`):
   - 函数: `get_execution_plan` 和 `audit_execution_plan`
     - 功能: 提取和审核SQL查询的执行计划。

3. **索引审核** (`indexes_audit.py`):
   - 主函数: `audit_indexes`
   - 功能: 审核数据库索引的使用和配置。

4. **表结构审核** (`table_structure_audit.py`):
   - 主函数: `audit_table_structure`
   - 功能: 审核数据库表的结构。

#### 安装

为了运行此工具，您需要安装以下Python包：

```
pip install pyodbc
pip install sqlparse
pip install sql_metadata
```

#### 使用方法

1. 设置并连接到 SQL Server。
2. 创建新的数据库以及用户。
3. 在Python脚本中使用以下信息来连接到这个数据库：

```python
server = "YOUR_SERVER_NAME"
database = "YOUR_DB_NAME"
user = "YOUR_USER_NAME"
password = "YOUR_PASSWORD"
```

确保替换适当的值。

---
#### 完整借助工具优化sql案例
case1:
case2:
case3:

   更新记录
2023/8/5 ①表结构审核删除重复规则。②修复表结构审核中元素数量和占位符不匹配规则③表结构审核补充部分规则④调通部分表结构审核部分规则。⑥只审核sql中出现的表。⑦sql_metadata库的引入。
2023/8/6 ①索引审计表范围修复。待处理：①索引审计去重，补充部分规则。③表结构表范围修复。③存储过程生成各种表跟数据-各种待优化sql生成-审计调优建议生成-根据建议改写sql-校验（逻辑分析，哈希，随机，）完整流程case整理。
2023/8/7 ③索引审计修复，去重，补充高级规则。
2023/8/8 ①表结构去重，递增，表范围限定。②执行计划部分规则校验。创建表,数据,存储过程，sql语句，测试是否触发规则。下一步：测试一些复杂sql，看看能否触发更多执行计划审计部分规则，修正部分执行计划规则。整体验证工具效果。修改readme。
