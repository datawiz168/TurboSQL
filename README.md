sql-server-sql-rule

基于规则的 SQL Server SQL 审计调优工具。
主要功能

此工具提供了四个核心的审计功能，每个功能都被分离到单独的 .py 文件中。主函数则导入并调用这四个文件：
1. SQL语句审核 (sql_query_audit.py):

    主函数: audit_query
    功能: 审核SQL查询的安全和性能问题。
    相关辅助函数: 用于支持主要审计功能。

2. 执行计划审核 (execution_plan_audit.py):

    函数: get_execution_plan
        功能: 提取SQL查询的执行计划。
    函数: audit_execution_plan
        功能: 审核提取到的执行计划。

3. 索引审核 (indexes_audit.py):

    主函数: audit_indexes
    功能: 审核数据库索引的使用和配置。

4. 表结构审核 (table_structure_audit.py):

    主函数: audit_table_structure
    功能: 审核数据库表的结构。
   
测试数据库和数据
以下是创建测试数据库和插入数据的SQL脚本

1. **创建 `users` 表**:
```sql
CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    email NVARCHAR(100) UNIQUE,
    age INT
);
```

2. **创建 `orders` 表**:
```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT FOREIGN KEY REFERENCES users(id),
    product NVARCHAR(100),
    amount DECIMAL(10, 2)
);
```

3. **插入示例数据到 `users` 表**:
```sql
INSERT INTO users (name, email, age) VALUES 
('张三', 'zhangsan@example.com', 25),
('李四', 'lisi@example.com', 30),
('王五', 'wangwu@example.com', 20);
```

4. **插入示例数据到 `orders` 表**:
```sql
INSERT INTO orders (user_id, product, amount) VALUES 
(1, '苹果', 10.5),
(1, '香蕉', 5.5),
(2, '橙子', 7.0);
```

5. **提供的 SQL 查询**:
    - 全表扫描的查询：
      SELECT * FROM users WHERE age > 18;
    - 使用了 `NOLOCK` 查询提示的查询：
      SELECT * FROM users WITH (NOLOCK) WHERE age > 18;
    - 正常的联接查询：
      SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id;


`indexes_audit.py` 文件中定义了多个规则来审查索引的使用和配置。以下是该文件中提到的一些规则及其对应的测试样例建议：

1. **未使用的索引**: 检查系统中未使用的索引。
   - 测试样例: 创建一个未使用的非聚集索引，并检查它是否被识别为未使用。
   
     ```sql
     CREATE INDEX idx_unused ON users(email);
     ```

2. **表没有主键**: 查找没有主键的表。
   - 测试样例: 创建一个没有主键的新表，并检查它是否被识别为没有主键。
   
     ```sql
     CREATE TABLE test_no_pk (col1 INT, col2 NVARCHAR(100));
     ```

3. **表没有聚集索引**: 查找没有聚集索引的表。
   - 测试样例: 创建一个表并添加一个非聚集索引，然后检查该表是否被识别为没有聚集索引。

     ```sql
     CREATE TABLE test_no_clustindex (col1 INT, col2 NVARCHAR(100));
     CREATE NONCLUSTERED INDEX idx_test_no_clustindex ON test_no_clustindex(col1);
     ```

4. **索引的大小超过5MB**: 查找大小超过5MB的索引。
   - 测试样例: 虽然实际创建一个超过5MB的索引可能比较困难，但您可以将此规则作为理论验证。

5. **索引的填充因子**: 检查索引的填充因子是否在70%到90%之间。
   - 测试样例: 创建一个索引，其填充因子不在此范围内，并检查它是否被标识。

     ```sql
     CREATE INDEX idx_fillfactor ON users(age) WITH (FILLFACTOR = 60);
     ```

6. **索引的行锁定**: 检查禁止行锁定的索引。
   - 测试样例: 创建一个索引，并在创建时指定`ALLOW_ROW_LOCKS = OFF`，然后检查它是否被标识。
   
     ```sql
     CREATE INDEX idx_no_row_locks ON users(name) WITH (ALLOW_ROW_LOCKS = OFF);
     ```

7. **索引的页锁定**: 检查禁止页锁定的索引。
   - 测试样例: 创建一个索引，并在创建时指定`ALLOW_PAGE_LOCKS = OFF`，然后检查它是否被标识。

     ```sql
     CREATE INDEX idx_no_page_locks ON users(email) WITH (ALLOW_PAGE_LOCKS = OFF);
     ```

这些测试样例提供了基于`indexes_audit.py`文件中的规则的不同情况。

`execution_plan_audit.py` 文件中定义了多个规则来审查SQL查询的执行计划。以下是一些提到的规则及其对应的测试样例建议：

9. **检查全表扫描**: 
   - 测试样例: 执行一个没有使用任何索引的查询。
   
     ```sql
     SELECT * FROM users WHERE name LIKE '%zhang%';
     ```

10. **检查缺失的索引**:
   - 测试样例: 执行一个查询，系统可能会建议为其添加索引。
   
     ```sql
     SELECT * FROM users WHERE email = 'missing_index@example.com';
     ```

11. **检查预估的行数与实际行数的偏差**: 
   - 测试样例: 这通常涉及到数据库统计信息的问题。您可以考虑在表有大量数据更改后运行查询，而不更新统计信息。

12. **检查数据锁**: 
   - 测试样例: 执行一个需要长时间运行的事务，同时尝试在另一个会话中修改相同的数据。

     ```sql
     BEGIN TRANSACTION;
     UPDATE users SET age = age + 1 WHERE id = 1;
     -- In another session
     UPDATE users SET age = age - 1 WHERE id = 1;
     ```
13. **使用不推荐的PAGLOCK查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users WITH (PAGLOCK) WHERE name = '李四';
     ```

14. **数据分布不均**: 
   - 测试样例: 这通常涉及到分区表或分布式数据库的查询。您可以考虑在其中一部分数据量远大于其他部分的分区表上运行查询。

15. **数据碎片**: 
   - 测试样例: 在表上频繁地进行插入、删除和更新操作，然后查询该表。

16. **数据冗余**: 
   - 测试样例: 在表中插入重复的数据，然后执行查询。
   
     ```sql
     INSERT INTO users (name, email, age) VALUES ('张三', 'zhangsan@example.com', 25);
     SELECT * FROM users WHERE name = '张三';
     ```

17. **大量的UNION操作**: 
   - 测试样例: 执行一个包含多个UNION的查询。
   
     ```sql
     SELECT name FROM users WHERE id = 1
     UNION 
     SELECT name FROM users WHERE id = 2
     UNION
     SELECT name FROM users WHERE id = 3;
     ```

这些测试样例基于`execution_plan_audit.py`文件中的规则提供了不同的情境。

`sql_query_audit.py` 文件中定义了多个规则来审查SQL查询的各种安全和性能问题。以下是一些提到的规则及其对应的测试样例建议：

18. **使用不推荐的xp_enumerrorlogs存储过程**:
   - 测试样例:
     ```sql
     EXEC xp_enumerrorlogs;
     ```

19. **使用不推荐的xp_logevent存储过程**:
   - 测试样例:
     ```sql
     EXEC xp_logevent 60000, 'Test Message';
     ```
20. **使用不推荐的ROWLOCK查询提示**:
   - 测试样例:
     ```sql
    SELECT * FROM users WITH (ROWLOCK) WHERE email = 'lisi@example.com';
     ```

21. **使用不推荐的NOLOCK查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users WITH (NOLOCK) WHERE id = 1;
     ```

22. **使用不推荐的INDEX查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users WITH (INDEX(idx_name)) WHERE name = '张三';
     ```

23. **使用不推荐的FORCESEEK查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users WITH (FORCESEEK) WHERE id = 2;
     ```

24. **使用不推荐的FORCESCAN查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users WITH (FORCESCAN) WHERE age > 20;
     ```

25. **使用不推荐的NOEXPAND查询提示**:
   - 测试样例:
     ```sql
     SELECT * FROM users_view WITH (NOEXPAND);
     ```





这些测试样例基于`sql_query_audit.py`文件中的规则提供了不同的情境。

这些样例涵盖了各种可能的SQL查询安全和性能问题，用来测试审计调优脚本。
