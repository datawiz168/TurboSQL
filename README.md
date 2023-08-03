## sql-server-sql-rule

### 基于规则的 SQL Server SQL 审计调优工具

#### 主要功能
此工具提供了四个核心的审计功能，每个功能都被分离到单独的 .py 文件中，主函数则导入并调用这四个文件：

1. **SQL语句审核** (`sql_query_audit.py`):
    - 主函数: `audit_query`
    - 功能: 审核SQL查询的安全和性能问题。

2. **执行计划审核** (`execution_plan_audit.py`):
    - 函数: `get_execution_plan`
      - 功能: 提取SQL查询的执行计划。
    - 函数: `audit_execution_plan`
      - 功能: 审核提取到的执行计划。

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

#### 示例测试样例

为了展示此工具的强大性能调优能力，提供了以下的测试样例：

1. **检查未优化的联接**:
   执行一个查询，其中涉及到大量的表联接但没有使用索引。
   ```sql
   SELECT * FROM users 
   JOIN orders ON users.id = orders.user_id 
   JOIN products ON orders.product_id = products.id 
   WHERE users.age > 25;
   ```

2. **未使用的索引**:
   创建一个索引，但在查询中并没有使用到，工具应该能识别出这个索引没有被使用。
   ```sql
   CREATE INDEX idx_unused_email ON users(email);
   SELECT * FROM users WHERE name LIKE '%zhang%';
   ```

3. **全表扫描**:
   执行一个查询，这个查询会扫描整个表，而不是使用索引。
   ```sql
   SELECT * FROM users WHERE name LIKE '%zhang%';
   ```

4. **复杂的子查询**:
   执行一个包含多个子查询和联接的查询，此查询可能需要优化。
   ```sql
   SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count 
   FROM users u 
   WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.user_id = u.id AND o2.amount > 100);
   ```

这些测试样例可以帮助您理解此工具如何辨识和提出SQL查询中的性能问题。
