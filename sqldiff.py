
import pymysql


def parseDbAddress(addr):
    # db1='127.0.0.1:3306/db1?user=root&password=123456'
    ip = addr[:addr.find(':')]
    port = addr[addr.find(':')+1:addr.find('/')]
    dbName = addr[addr.find('/')+1:addr.find('?')]
    user = addr[addr.find('user=')+5:addr.find('&')]
    password = addr[addr.find('password=')+9:]
    return ip, port, dbName, user, password


class TableDDL:
    def __init__(self, field, type, null, default):
        self.field = field
        self.type = type
        self.null = null
        self.default = default


def getDbddl(dbaddr):
    ip, port, dbName, user, password = parseDbAddress(dbaddr)

    db = pymysql.connect(host=ip, port=int(port), user=user,
                         passwd=password, database=dbName)
    cursor = db.cursor()
    cursor.execute("show tables")
    tables = cursor.fetchall()
    tableDict = {}
    for i in tables:
        fieldInfos = {}
        cursor.execute('desc '+i[0])
        tablefields = cursor.fetchall()
        for j in tablefields:
            #  Field|Type|Null|Key|Default|Extra|
            fieldInfos[j[0]] = TableDDL(
                field=j[0], type=j[1], null=j[2], default=j[4])
        tableDict[i[0]] = fieldInfos

    db.close()

    return dbName, tableDict


def diffDbDDL(dbaddr1, dbaddr2):

    dbNameA, ddlA = getDbddl(dbaddr1)
    dbNameB, ddlB = getDbddl(dbaddr2)

    tablesa, tablesb = set(ddlA.keys()), set(ddlB.keys())
    # 处理表是否存在问题
    tableNotExistInB = list(tablesa-tablesb)
    tableNotExistInA = list(tablesb-tablesa)
    print("\033[0;32;40m -------------------------------------table if exist-------------------------------------\033[0m \n\n")
    tmp="""tables %s :
                            exist in %s
                            not   in %s\n\n"""
    tmp=tmp.replace('%s','\033[1;31;40m%s\033[0m')
    if tableNotExistInA:
        print(tmp % (tableNotExistInA, dbNameB, dbNameA))
    if tableNotExistInB:
        print(tmp % (tableNotExistInB, dbNameA, dbNameB))

    print("\n\033[0;32;40m -------------------------------------table ddl diff-------------------------------------\033[0m \n")

    tableDiffData = {}
    for table in ddlA:
        if table not in ddlB:
            continue
        diffPoints = []
        tableDiffData[table] = diffPoints
        d1, d2 = ddlA[table], ddlB[table]

        tableAName = dbNameA+'.'+table
        tableBName = dbNameB+'.'+table
        for field in d1:
            fieldExistTmp="""field %s :
                                    exist in %s
                                    not   in %s"""
            fieldExistTmp=fieldExistTmp.replace('%s','\033[1;31;40m%s\033[0m')
            if field not in d2:
                diffPoints.append(fieldExistTmp %
                                  (field, tableAName, tableBName))
                continue
            p1, p2 = d1[field], d2[field]
            tmp ="""field %s %s :
                                %s in %s
                                %s in %s"""
            tmp=tmp.replace('%s','\033[1;31;40m%s\033[0m')
            if p1.type != p2.type:
                diffPoints.append(
                    tmp % (field, 'type', p1.type, tableAName, p2.type, tableBName))
            if p1.null != p2.null:
                diffPoints.append(
                    tmp % (field, 'null', p1.null, tableAName, p2.null, tableBName))
            if p1.default != p2.default:
                diffPoints.append(
                    tmp % (field, 'default', p1.default, tableAName, p2.default, tableBName))

        for field in d2:
            if field not in d1:
                diffPoints.append(fieldExistTmp %
                                  (field, tableBName, tableAName))
                continue

    for i in tableDiffData:
        if not tableDiffData[i]:
            continue
        print('table-------------->', i)
        for j in tableDiffData[i]:
            print('\t\t', j, '\n')

    return tableDiffData


if __name__ == '__main__':

    db1 = '127.0.0.1:3306/sc_auth?user=root&password=ZJK15626933724'
    db2 = '127.0.0.1:3306/sc_auth2?user=root&password=ZJK15626933724'
    ret = diffDbDDL(db1, db2)
    # print(ret)
