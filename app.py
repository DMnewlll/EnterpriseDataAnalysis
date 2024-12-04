import csv
import io
import os
import re
import pandas as pd
import requests
from flask import Flask, request, redirect, url_for, render_template, flash
from flask import Flask, json
from impala.dbapi import connect
from hdfs import InsecureClient
from openpyxl.reader.excel import load_workbook

app = Flask(__name__)

HOST = "121.36.46.125"
HDFS_URL = 'http://121.36.46.125:9870'
HDFS_NAMENODE_HOST = 'http://121.36.46.125:9870/webhdfs/v1'
HDFS_USER = 'root'
HDFS_UPLOAD_DIR = '/user/data/'
HIVE_PORT = '10000'

areas = {'4501': '南宁', '4502': '柳州', '4503': '桂林', '4504': '梧州', '4505': '北海', '4506': '防城港',
         '4507': '钦州', '4508': '贵港', '4509': '玉林', '4510': '百色', '4511': '贺州', '4512': '河池', '4513': '来宾',
         '4514': '崇左'}


@app.route("/", methods=["POST", "GET"])
def index():
    return render_template('upload.html')


# 上传文件到HDFS，从HDFS获取文件进行清洗后导入hive数据仓库
@app.route("/upload", methods=["POST", "GET"])
def upload():
    if request.method == 'POST':
        # 1.处理上传的文件
        # 判断请求中是否包含文件
        if 'file' not in request.files:
            a_json = {"result": "没有文件部分", "status": 400}
            print(a_json)
            return json.dumps(a_json, ensure_ascii=False)
        file = request.files['file']

        # 判断有没有选择文件
        if file.filename == '':
            a_json = {"result": "没有选择文件", "status": 400}
            print(a_json)
            return json.dumps(a_json, ensure_ascii=False)

        # 获取上传的文件的内容
        file_content = file.read()
        workbook = load_workbook(io.BytesIO(file_content), data_only=True)
        # 第一个工作表
        sheet = workbook.active
        # 将文件转化为csv文件
        # 准备CSV文件名
        csv_filename = os.path.join('高企源数据.csv')
        # 写入CSV文件
        with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            header = [cell.value for cell in sheet[1]]  # 第一行作为表头
            writer.writerow(header)
            # 写入数据行
            for row in sheet.iter_rows(min_row=2, values_only=True):
                writer.writerow(row)

        # 3.将源数据上传到HDFS
        hdfs_path = os.path.join(HDFS_UPLOAD_DIR, file.filename)
        url = f"{HDFS_NAMENODE_HOST}{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
        headers = {'Content-Type': 'application/octet-stream'}
        response = requests.put(url, data=file_content, headers=headers)
        if response.status_code == 201:
            print("上传源数据成功")
        else:
            print("上传源数据失败")

        # 4.进行数据清洗
        df = pd.read_csv(csv_filename)
        # 删除列名为“数据状态”且其值为“dis”的行
        df = df[df['数据状态'] != 'dis']
        # 删除 序号 数据年份 数据状态 撤销原因 申请撤销状态 管理员审核状态 错误数量  是否填写国家统计局一套表
        columns_to_drop = ['序号', '帐号', '数据年份', '数据状态', '撤消原因', '申请撤消状态', '管理员审核状态',
                           '错误数量', '警告数量', '数据提交状态', '是否填写国家统计局一套表']
        df = df.drop(columns=columns_to_drop)
        # 填充空值
        df.fillna(-2147483640, inplace=True)
        # 去掉列名后面的冒号
        # 除掉列名中的带圈序号
        # 除掉列名中的逗号
        df.rename(columns=lambda col: re.sub(r'[，：.:,（）()]', '', col), inplace=True)

        # 保存回一个新的CSV文件
        cleanDataTableName = 'clean_data.csv'
        df.to_csv(cleanDataTableName, index=False)

        # 4.将清洗后的数据导入到hive
        # 根据清洗过后的表头在hive建表
        importHive(cleanDataTableName)
    return render_template('upload.html', ok='ok')


# 分析各地市高企的产业分布情况
@app.route("/chanYe")
def chanYe():
    # 1需要的数据有：cleanData表里的行政区化代码、行业代码、code表里的产业名
    cursor, conn = connectHive()
    cursor.execute(
        'SELECT cd.`行政区划代码`,c.`行业名`,COUNT(*) AS `行业数量` FROM cleanData AS cd JOIN code AS c ON cd.`行业代码2017` = c.`行业代码` group by cd.`行政区划代码`, c.`行业名`')
    # cursor.execute('select `行业名` from code')
    results = cursor.fetchall()
    print(results)
    a_json = {"result": results, "status": 200}
    print(a_json['result'])
    closeHive(cursor, conn)
    return json.dumps(a_json, ensure_ascii=False)


# 连接hive的函数
def connectHive():
    conn = connect(host="121.36.46.125", port=10000, user="root", password="Hadoop123456",
                   auth_mechanism="PLAIN")
    print("1")
    # 连接后，获取游标，标志指示数据库的当前记录
    cursor = conn.cursor()
    return cursor, conn


# 关闭hive的函数
def closeHive(cursor, conn):
    cursor.close()
    conn.close()


# 测试可不可根据表头信息自动建表，字段太多了，无法手动建表
def importHive(tableName):
    # 连接hive2的服务器
    # 1.读取属性表
    result = ""
    df = pd.read_csv(tableName)

    # 判断数据类型
    # 初始化一个字典来存储列名和数据类型的映射
    column_types = {}
    # 遍历每列，推断数据类型
    print(df.columns)
    for column in df.columns:
        print(column)
        # 检查空值
        if df[column].isnull().all():
            # 如果整列都是空值，可以指定一个默认类型（如STRING）或跳过该列
            column_types[column] = 'STRING'
        else:
            # 尝试推断数据类型
            unique_values = df[column].unique()
            print(unique_values)
            if all(isinstance(value, int) for value in unique_values if pd.notnull(value)):
                # 如果所有非空值都是整数，则指定为INT类型
                column_types[column] = 'INT'
            elif all(isinstance(value, float) for value in unique_values if pd.notnull(value)):
                # 如果所有非空值都是浮点数，则指定为FLOAT类型
                column_types[column] = 'FLOAT'
            elif all(isinstance(value, str) for value in unique_values if pd.notnull(value)):
                # 如果所有非空值都是字符串，则指定为STRING类型
                # 注意：这里可能需要进一步的检查来确定是否为日期或其他特定类型的字符串
                column_types[column] = 'STRING'
            # 可以添加更多的数据类型推断规
    flat = False
    for (key, value) in column_types.items():
        if flat:
            result += ','
        result += f"`{key}` {value}"
        flat = True

    # 删除0-3行
    df = df.iloc[:0].append(df.iloc[3:]).reset_index(drop=True)
    df.to_csv(tableName, index=False)

    conn = connect(host="121.36.46.125", port=10000, user="root", password="Hadoop123456",
                   auth_mechanism="PLAIN")
    print("1")
    # 连接后，获取游标，标志指示数据库的当前记录
    cursor = conn.cursor()
    table_name = 'cleanData'
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({result}) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE"
    print(create_table_sql)
    cursor.execute(create_table_sql)

    # 3.将文件上传到HDFS
    # 创建HDFS客户端

    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    # 上传文件到HDFS
    hdfs_path = HDFS_UPLOAD_DIR + tableName
    print(hdfs_path)
    client.upload(hdfs_path, tableName)
    cursor = conn.cursor()
    cursor.execute(f"LOAD DATA INPATH '{hdfs_path}' INTO TABLE {table_name} ")
    print("2")


# 测试上传本地文件到hdfs
@app.route("/uploadData", methods=['POST', 'GET'])
def uploadHDFS():
    # 检查请求中是否包含文件
    if 'file' not in request.files:
        a_json = {"result": "没有文件部分", "status": 400}
        print(a_json)
        return json.dumps(a_json, ensure_ascii=False)
    file = request.files['file']
    # 检查用户有没有选择文件
    if file.filename == '':
        a_json = {"result": "没有选择文件", "status": 400}
        print(a_json)
        return json.dumps(a_json, ensure_ascii=False)
    file_content = file.read()
    workbook = load_workbook(io.BytesIO(file_content), data_only=True)

    # 假设我们只关心第一个工作表
    sheet = workbook.active
    # 准备CSV文件名（可以根据需要修改）
    csv_filename = os.path.join('output.csv')

    # 写入CSV文件
    with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        # 写入数据行
        for row in sheet.iter_rows(values_only=True):
            writer.writerow(row)

    hdfs_path = os.path.join(HDFS_UPLOAD_DIR, file.filename)
    url = f"{HDFS_NAMENODE_HOST}{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
    headers = {'Content-Type': 'application/octet-stream'}
    response = requests.put(url, data=file_content, headers=headers)
    if response.status_code == 201:
        a_json = {"result": "上传成功", "status": 200}
        print(a_json)
        return json.dumps(a_json, ensure_ascii=False)
    else:
        a_json = {"result": "上传失败", "status": 200}
        print(a_json)
        return json.dumps(a_json, ensure_ascii=False)


# 测试连接将数据导入Hive
def uploadHive():
    # 1.连接hive
    conn = connect(host="121.36.46.125", port=10000, user="root", password="Hadoop123456",
                   auth_mechanism="PLAIN")
    # 2.使用pandas读取文件
    df = pd.read_excel('属性表.xlsx')
    # 3。将数据保存为CSV文件
    df.to_csv('属性表.csv', index=False)
    # 3.将文件上传到HDFS
    # 创建HDFS客户端
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    # 上传文件到HDFS
    client.upload('/user/data/clean_data.csv', '属性表.csv')
    # 4.创建表（我已经创建好了）
    cursor = conn.cursor()
    cursor.execute("LOAD DATA INPATH '/user/data/clean_data.csv' INTO TABLE mytest2")
    return "ok"


# 测试连接hive
@app.route("/linkHive")
def link():
    # 连接hive2的服务器
    conn = connect(host="121.36.46.125", port=10000, user="root", password="Hadoop123456", auth_mechanism="PLAIN")
    # 连接后，获取游标，标志指示数据库的当前记录
    cursor = conn.cursor()
    # 游标帮助我们执持HQL语句
    # cursor.execute('create table test1(id int,name string)')
    # 查询的结果在cursor.fetchall方法中
    # cursor.execute("select * from default.mytest")
    cursor.execute("select * from  cleanData")
    results = cursor.fetchall()
    a_json = {"result": results, "status": 200}
    print(results)
    return json.dumps(a_json, ensure_ascii=False)
    # return "ok"
    # return json.dumps({"result": "网络连接错误", "status": 404}, ensure_ascii=False)


if __name__ == '__main__':
    app.run(debug=True, port=5200)
