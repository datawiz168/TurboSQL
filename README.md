了解。我将为您整合内容，并确保新的查询可以在已有的数据结构上执行。

---

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
pip install sqlparse
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

#### 建表及数据准备

在开始之前，我们需要确保有适当的数据结构和数据来运行测试样例。以下是建表及数据准备的脚本：

```sql
-- 创建 users 表
CREATE TABLE users (
    id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    email NVARCHAR(100) UNIQUE,
    age INT
);

-- 创建 orders 表
CREATE TABLE orders (
    order_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT FOREIGN KEY REFERENCES users(id),
    product_id INT,
    amount DECIMAL(10, 2)
);

-- 创建 products 表
CREATE TABLE products (
    product_id INT PRIMARY KEY IDENTITY(1,1),
    product_name NVARCHAR(100)
);

-- 插入示例数据到 users 表
INSERT INTO users (name, email, age) VALUES 
('张三', 'zhangsan@example.com', 28),
('李四', 'lisi@example.com', 26),
('王五', 'wangwu@example.com', 24);

-- 插入示例数据到 products 表
INSERT INTO products (product_name) VALUES 
('苹果'),
('香蕉'),
('橙子');

-- 插入示例数据到 orders 表
INSERT INTO orders (user_id, product_id, amount) VALUES 
(1, 1, 10.5),
(1, 2, 5.5),
(2, 3, 7.0);
```

#### 示例测试样例

为了展示此工具的强大性能调优能力，提供了以下的测试样例：

1. **检查未优化的联接**: 执行一个查询，其中涉及到大量的表联接但没有使用索引。
   
   ```sql
   SELECT * FROM users 
   JOIN orders ON users.id = orders.user_id 
   JOIN products ON orders.product_id = products.product_id 
   WHERE users.age > 25;
   ```

2. **全表扫描**: 执行一个查询，这个查询会扫描整个表，而不是使用索引。
   
   ```sql
   SELECT * FROM users WHERE name LIKE '%zhang%';
   ```

3. **复杂的子查询**: 执行一个包含多个子查询和联接的查询，此查询可能需要优化。
   
   ```sql
   SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count 
   FROM users u 
   WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.user_id = u.id AND o2.amount > 100);
   ```

4. **多表自联接查询**: 考虑一个场景，我们想查找过去7天内与某用户有相同购买产品的所有其他用户。

   ```sql
   SELECT DISTINCT u2.name AS SimilarUser, p.product_name AS CommonProduct
   FROM orders o1
   JOIN orders o2 ON o1.product_id = o2.product_id AND o1.user_id != o2.user_id
   JOIN users u1 ON o1.user_id = u1.id
   JOIN users u2 ON o2.user_id = u2.id
   JOIN products p ON o1.product_id = p.product_id
   WHERE u1.name = '张三' AND DATEDIFF(day, o2.order_date, GETDATE()) <= 7;
   ```

5. **多重子查询**: 考虑一个场景，我们想查找购买最多产品的用户和他们的平均购买金额。

   ```sql
   SELECT TOP 5 u.name, AVG(o.amount) as AverageSpend
   FROM users u
   JOIN orders o ON u.id = o.user_id
   WHERE o.amount > (
       SELECT AVG(o2.amount) 
       FROM orders o2 
       WHERE o2.user_id = u.id
   ) AND u.id IN (
       SELECT o3.user_id 
       FROM orders o3 
       GROUP BY o3.user_id 
       HAVING COUNT(DISTINCT o3.product_id) > 5
   )
   GROUP BY u.name
   ORDER BY AverageSpend DESC;
   ```
