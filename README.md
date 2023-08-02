# sql-server-sql-rule
基于规则的sql server sql 审计调优工具

    将主要的四个审核函数（SQL语句审核、执行计划审核、索引审核、表结构审核）分别放入四个不同的 .py 文件中。
    在主函数中导入并调用这四个 .py 文件。
    
   1.sql_query_audit.py 包含 audit_query 函数和相关的辅助函数。
   2.execution_plan_audit.py 包含 get_execution_plan 和 audit_execution_plan 函数。
   3.indexes_audit.py 包含 audit_indexes 函数。
   4.table_structure_audit.py 包含 audit_table_structure 函数。



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
    - 全表扫描的查询：`SELECT * FROM users WHERE age > 18`。
    - 使用了 `NOLOCK` 查询提示的查询：`SELECT * FROM users WITH (NOLOCK) WHERE age > 18`。
    - 正常的联接查询：`SELECT users.name, orders.product FROM users JOIN orders ON users.id = orders.user_id`。
