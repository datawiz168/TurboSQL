from sql_query_audit import audit_query
from execution_plan_audit import get_execution_plan, audit_execution_plan
from indexes_audit import audit_indexes
from table_structure_audit import audit_table_structure

# 连接到SQL Server的函数
def connect_to_sql_server(server, database, user, password):
    # 构建连接字符串
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
    # 建立连接并返回
    conn = pyodbc.connect(connection_string)
    return conn

# 执行SQL查询的函数
def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        print(f"执行查询时出错: {e}")
        return None

if __name__ == "__main__":
    # 数据库连接信息
    server = "YOUR_SERVER_NAME"
    database = "YOUR_DATABASE_NAME"
    user = "YOUR_USERNAME"
    password = "YOUR_PASSWORD"
    
    # 连接到数据库
    conn = connect_to_sql_server(server, database, user, password)
    
    # 获取用户输入的SQL查询
    query = input("输入你的SQL查询: ")
    
    # 审核查询
    audit_query(query)
    
    # 获取执行计划
    plan = get_execution_plan(conn, query)
    # 审核执行计划
    audit_execution_plan(plan)
    
    # 审核索引
    audit_indexes(conn) 

    # 审核表结构
    audit_table_structure(conn)
    
    # 执行查询并获取结果
    results = execute_query(conn, query)
    
    # 如果有结果，打印结果
    if results:
        for row in results:
            print(row)
    
    # 关闭数据库连接
    conn.close()
