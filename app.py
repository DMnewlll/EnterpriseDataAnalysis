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

HDFS_HOST = 'http://121.36.46.125:9870'
HDFS_NAMENODE_HOST = 'http://121.36.46.125:9870/webhdfs/v1'
HDFS_USER = 'root'
HDFS_UPLOAD_DIR = '/user/data/'

@app.route("/", methods=["POST", "GET"])
def index():
    return render_template('upload.html')

# 上传文件到HDFS，从HDFS获取文件进行清洗后导入hive数据仓库
@app.route("/upload", methods=["POST", "GET"])
def upload():
    if request.method == 'POST':
        # 文件上传处理
        if 'file' not in request.files:
            return render_template('upload.html', error="没有文件部分")

        file = request.files['file']

        if file.filename == '':
            return render_template('upload.html', error="没有选择文件")

        try:
            file_content = file.read()
            workbook = load_workbook(io.BytesIO(file_content), data_only=True)
            sheet = workbook.active

            # CSV转化处理
            csv_filename = os.path.join('高企源数据.csv')
            with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                header = [cell.value for cell in sheet[1]]
                writer.writerow(header)
                for row in sheet.iter_rows(min_row=2, values_only=True):
                    writer.writerow(row)

            # 上传文件到HDFS
            hdfs_path = os.path.join(HDFS_UPLOAD_DIR, file.filename)
            url = f"{HDFS_NAMENODE_HOST}{hdfs_path}?op=CREATE&user.name={HDFS_USER}&overwrite=true"
            headers = {'Content-Type': 'application/octet-stream'}
            response = requests.put(url, data=file_content, headers=headers)
            if response.status_code != 201:
                raise Exception("上传到HDFS失败")

            # 数据清洗
            df = pd.read_csv(csv_filename)
            df.fillna(-2147483640, inplace=True)
            df.rename(columns=lambda col: re.sub(r'[，：.:,（）()]', '', col), inplace=True)
            #保存一个新的csv文件
            cleanDataTableName = 'clean_data.csv'
            df.to_csv(cleanDataTableName, index=False)

            # 导入数据到Hive
            importHive(cleanDataTableName)

            return render_template('upload.html', ok="上传成功！")

        except Exception as e:
            return render_template('upload.html', error=f"上传失败: {str(e)}")


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
    print(result)

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
    # 删除表头和前三行
    df.to_csv(tableName)
    client = InsecureClient(HDFS_HOST, user=HDFS_USER)
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
    client = InsecureClient(HDFS_HOST, user=HDFS_USER)
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
