# MYSQL_DB_DDL_DIFF
一个实现mysql 数据表字段 diff的脚本实现
可用于快速对比测试环境与正式环境数据库ddl差异

# 使用方式
1. 安装pymysql(如果没有安装的话):
    ```pip install pymysql```
    
2. 更改main函数中的db地址:
    ```python3
    if __name__ == '__main__':

    db1 = '127.0.0.1:3306/sc_auth?user=root&password=ZJK15626933724'
    db2 = '127.0.0.1:3306/sc_auth2?user=root&password=ZJK15626933724'
    ret = diffDbDDL(db1, db2)
    ```
    
3. 执行应用：
    ``` python -u sqldiff.py```

4. 查看输出:

    ![输出](img1.png)

