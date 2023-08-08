from sql_query_audit import audit_query, extract_tables_from_sql
from execution_plan_audit import audit_execution_plan
from indexes_audit import audit_indexes
from table_structure_audit import audit_table_structure
import pyodbc

# 定义连接到SQL Server的函数
def connect_to_sql_server(server, database, user, password):
    # 构建连接字符串
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
    # 使用连接字符串建立连接
    conn = pyodbc.connect(connection_string)
    # 返回连接对象
    return conn

def get_execution_plan_for_query(query, conn):
    with conn.cursor() as cursor:
        # 设置并执行查询以获取执行计划
        cursor.execute('SET SHOWPLAN_XML ON')
        cursor.execute(query)

        # 遍历查询结果
        for row in cursor:
            xml_execution_plan = row[0]
            # 保存执行计划为XML文件
            with open("execution_plan.xml", "w") as f:
                f.write(xml_execution_plan)

        # 关闭SHOWPLAN_XML模式
        cursor.execute('SET SHOWPLAN_XML OFF')

if __name__ == "__main__":
    # 定义数据库连接信息
    server = "localhost"
    database = "AuditDemoDB"
    user = "AuditDemoUser"
    password = "c2xYO16edKwep"

    # 使用连接信息连接到数据库
    conn = connect_to_sql_server(server, database, user, password)

    # 获取用户输入的SQL查询
    user_query = ""
    print("请输入你的SQL查询 (以';'结束):")
    # 循环读取用户输入，直到输入结束
    while True:
        line = input()
        user_query += line + "\n"
        if line.strip().endswith(';'):
            break

    # 删除查询末尾的';'
    user_query = user_query.strip()[:-1]
    print("正在执行的查询：")
    print(user_query)

    # 使用函数提取查询中的表名
    tables_in_query = extract_tables_from_sql(user_query)

    # 审核SQL查询
    audit_query(user_query)

    # 获取并保存查询的执行计划
    get_execution_plan_for_query(user_query, conn)
    print("XML执行计划已保存为 'execution_plan.xml'")

    # 从文件中读取并审核执行计划
    with open("execution_plan.xml", "r") as f:
        plan = f.read()
    audit_execution_plan(plan)

    # 审核数据库索引
    audit_indexes(conn, tables_in_query)

    # 审核数据库表结构
    audit_table_structure(conn, tables_in_query)

    # 执行SQL查询并获取查询结果
    cursor = conn.cursor()
    results = cursor.execute(user_query).fetchall()

    # 如果查询有结果，打印结果
    if results:
        for row in results:
            print(row)

    # 关闭数据库连接
    conn.close()
