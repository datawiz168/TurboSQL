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

场景二: ERP系统的销售、产品和地区分析

-- 1. 删除现有表（如果存在，按照依赖关系的相反顺序删除表）

IF OBJECT_ID('OrderDetails', 'U') IS NOT NULL DROP TABLE OrderDetails;
IF OBJECT_ID('Orders', 'U') IS NOT NULL DROP TABLE Orders;
IF OBJECT_ID('Employees', 'U') IS NOT NULL DROP TABLE Employees;
IF OBJECT_ID('Products', 'U') IS NOT NULL DROP TABLE Products;
IF OBJECT_ID('Suppliers', 'U') IS NOT NULL DROP TABLE Suppliers;
IF OBJECT_ID('Categories', 'U') IS NOT NULL DROP TABLE Categories;
IF OBJECT_ID('Departments', 'U') IS NOT NULL DROP TABLE Departments;
IF OBJECT_ID('Locations', 'U') IS NOT NULL DROP TABLE Locations;
IF OBJECT_ID('Countries', 'U') IS NOT NULL DROP TABLE Countries;
IF OBJECT_ID('Regions', 'U') IS NOT NULL DROP TABLE Regions;

-- 2. 创建表结构

CREATE TABLE Regions (
    RegionID INT PRIMARY KEY,
    RegionName VARCHAR(100)
);

CREATE TABLE Countries (
    CountryID INT PRIMARY KEY,
    CountryName VARCHAR(100),
    RegionID INT FOREIGN KEY REFERENCES Regions(RegionID)
);

CREATE TABLE Locations (
    LocationID INT PRIMARY KEY,
    LocationName VARCHAR(100),
    CountryID INT FOREIGN KEY REFERENCES Countries(CountryID)
);

CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(100)
);

CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(100)
);

CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    SupplierID INT FOREIGN KEY REFERENCES Suppliers(SupplierID),
    CategoryID INT FOREIGN KEY REFERENCES Categories(CategoryID),
    Price DECIMAL(18, 2)
);

CREATE TABLE Departments (
    DepartmentID INT PRIMARY KEY,
    DepartmentName VARCHAR(100),
    LocationID INT FOREIGN KEY REFERENCES Locations(LocationID)
);

CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    JobID INT FOREIGN KEY REFERENCES Departments(DepartmentID)
);

CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    EmployeeID INT FOREIGN KEY REFERENCES Employees(EmployeeID)
);

CREATE TABLE OrderDetails (
    OrderDetailID INT PRIMARY KEY,
    OrderID INT FOREIGN KEY REFERENCES Orders(OrderID),
    ProductID INT FOREIGN KEY REFERENCES Products(ProductID),
    Quantity INT
);

-- 3.删除存储过程

IF OBJECT_ID('InsertRegions', 'P') IS NOT NULL DROP PROCEDURE InsertRegions;
IF OBJECT_ID('InsertCountries', 'P') IS NOT NULL DROP PROCEDURE InsertCountries;
IF OBJECT_ID('InsertLocations', 'P') IS NOT NULL DROP PROCEDURE InsertLocations;
IF OBJECT_ID('InsertSuppliers', 'P') IS NOT NULL DROP PROCEDURE InsertSuppliers;
IF OBJECT_ID('InsertCategories', 'P') IS NOT NULL DROP PROCEDURE InsertCategories;
IF OBJECT_ID('InsertProducts', 'P') IS NOT NULL DROP PROCEDURE InsertProducts;
IF OBJECT_ID('InsertDepartments', 'P') IS NOT NULL DROP PROCEDURE InsertDepartments;
IF OBJECT_ID('InsertEmployees', 'P') IS NOT NULL DROP PROCEDURE InsertEmployees;
IF OBJECT_ID('InsertOrders', 'P') IS NOT NULL DROP PROCEDURE InsertOrders;
IF OBJECT_ID('InsertOrderDetails', 'P') IS NOT NULL DROP PROCEDURE InsertOrderDetails;

-- 4. 创建存储过程
-- 存储过程插入 Regions

CREATE PROCEDURE InsertRegions
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Regions (RegionID, RegionName)
        VALUES (@i, 'Region' + CAST(@i AS VARCHAR));
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Countries
CREATE PROCEDURE InsertCountries
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Countries (CountryID, CountryName, RegionID)
        VALUES (@i, 'Country' + CAST(@i AS VARCHAR), (@i - 1) % 100 + 1);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Locations
CREATE PROCEDURE InsertLocations
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Locations (LocationID, LocationName, CountryID)
        VALUES (@i, 'Location' + CAST(@i AS VARCHAR), (@i - 1) % 100 + 1);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Suppliers
CREATE PROCEDURE InsertSuppliers
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Suppliers (SupplierID, SupplierName)
        VALUES (@i, 'Supplier' + CAST(@i AS VARCHAR));
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Categories
CREATE PROCEDURE InsertCategories
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Categories (CategoryID, CategoryName)
        VALUES (@i, 'Category' + CAST(@i AS VARCHAR));
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Products
CREATE PROCEDURE InsertProducts
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Price)
        VALUES (@i, 'Product' + CAST(@i AS VARCHAR), (@i - 1) % 1000 + 1, (@i - 1) % 1000 + 1, RAND() * 100);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Departments
CREATE PROCEDURE InsertDepartments
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Departments (DepartmentID, DepartmentName, LocationID)
        VALUES (@i, 'Department' + CAST(@i AS VARCHAR), (@i - 1) % 1000 + 1);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Employees
CREATE PROCEDURE InsertEmployees
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Employees (EmployeeID, EmployeeName, JobID)
        VALUES (@i, 'Employee' + CAST(@i AS VARCHAR), (@i - 1) % 1000 + 1);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 Orders
CREATE PROCEDURE InsertOrders
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO Orders (OrderID, OrderDate, EmployeeID)
        VALUES (@i, DATEADD(DAY, RAND() * 365, '2022-01-01'), (@i - 1) % 10000 + 1);
        SET @i = @i + 1;
    END
END;
GO

-- 存储过程插入 OrderDetails
CREATE PROCEDURE InsertOrderDetails
AS
BEGIN
    DECLARE @i INT = 1;
    WHILE @i <= 1000000
    BEGIN
        INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity)
        VALUES (@i, (@i - 1) % 10000 + 1, (@i - 1) % 10000 + 1, RAND() * 100);
        SET @i = @i + 1;
    END
END;
GO


-- 5.执行存储过程

EXEC InsertRegions;
EXEC InsertCountries;
EXEC InsertLocations;
EXEC InsertSuppliers;
EXEC InsertCategories;
EXEC InsertProducts;
EXEC InsertDepartments;
EXEC InsertEmployees;
EXEC InsertOrders;
EXEC InsertOrderDetails;



case3:

#### SQL Server 审计工具评价
功能完整性
    SQL 语句审核: 通过分析 SQL 查询，工具可以识别出潜在的安全和性能问题。这是一个强大的功能，可以在开发阶段捕获潜在的问题。
    执行计划审核: 通过分析 SQL 查询的执行计划，工具能够识别出查询的性能瓶颈和潜在的优化点。这有助于深入了解查询如何在数据库中执行，并提出改进建议。
    索引审核: 通过分析数据库的索引配置，工具可以发现无用或缺失的索引，以及可能的优化空间。这有助于提高查询性能和减少资源消耗。
    表结构审核: 通过分析数据库表的结构，工具可以识别出潜在的设计问题和改进点。这有助于确保数据的一致性和完整性。
    
--易用性
此工具的使用相对简单，通过 Python 脚本提供了一组清晰的函数接口。用户可以通过简单的配置和命令行交互来启动审核。

--扩展性
工具的设计允许用户添加更多的规则和功能，以满足特定需求。通过将各个功能模块化，工具具有良好的扩展性。

--依赖性
工具依赖于一些常用的 Python 库，如 pyodbc, sqlparse, 和 sql_metadata，这些库在大多数环境中都容易安装和使用。

--综合评价
此 SQL Server 审计工具是一个强大而灵活的工具，适用于数据库管理员、开发人员和数据分析师。通过提供对 SQL 查询、执行计划、索引和表结构的深入分析，它能够识别和解决许多常见的数据库问题。工具的模块化设计还允许用户根据需要进行自定义和扩展。

#### 更新记录
2023/8/5 ①表结构审核删除重复规则。②修复表结构审核中元素数量和占位符不匹配规则③表结构审核补充部分规则④调通部分表结构审核部分规则。⑥只审核sql中出现的表。⑦sql_metadata库的引入。
2023/8/6 ①索引审计表范围修复。待处理：①索引审计去重，补充部分规则。③表结构表范围修复。③存储过程生成各种表跟数据-各种待优化sql生成-审计调优建议生成-根据建议改写sql-校验（逻辑分析，哈希，随机，）完整流程case整理。
2023/8/7 ③索引审计修复，去重，补充高级规则。
2023/8/8 ①表结构去重，递增，表范围限定。②执行计划部分规则校验。创建表,数据,存储过程，sql语句，测试是否触发规则。下一步：测试一些复杂sql，看看能否触发更多执行计划审计部分规则，修正部分执行计划规则。整体验证工具效果。修改readme。
