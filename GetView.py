# -*- coding: Utf-8 -*-
# Author: Ruijia Mao
# Description:
#       This Program is to collect all the DDLs for an Oracle database
# Date: 2016/06/07
import os
import cx_Oracle
from time import localtime, strftime, sleep
from json import load
import sys
import traceback
import glob

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
config = {}
dirName = "Data_" + strftime("%Y%m%d_%H%M%S", localtime())
os.mkdir(dirName)

# log = open(dirName + "/log", "w")
# log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Getting" + dirName + "\n")

if os.path.exists("Config.json"):
    fp = open("Config.json", "r")
    config = load(fp)
    fp.close()
else:
    print "Config.json doesn't exist.\n"
    # log.write("Error " + strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Config.json doesn't exist.\n")
    sleep(2)
    exit(0)

last = config["Server"] + "/" + config["SID"]
con = []
try:
    con = cx_Oracle.connect(config["UserName"], config["Password"], last)
except Exception as e:
    print "Error: "
    print e
    # tb = sys.exc_info()[2]
    # tbinfo = traceback.format_tb(tb)[0]
    # pymsg = "PYTHON ERRORS " + strftime("%Y/%m/%d %H:%M:%S",
    #                                     localtime()) + ":\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(
    #     sys.exc_info()[1])
    # log.write(pymsg)
    # print tbinfo

print "Connection Established."
# log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Connection established.\n")

# expression = '^XIE|^CUX'
# expression = 'XIE_ERP_GL_INT_T'
expression = config['expression']

os.chdir(dirName)

findObjectInfo = "select OBJECT_TYPE, OBJECT_NAME, OWNER from ALL_OBJECTS where regexp_like(OBJECT_NAME ,:expression) AND OBJECT_TYPE='VIEW'"
# findCreation = "SELECT DBMS_METADATA.GET_DDL('TABLE','XIE_ERP_GL_INT_T','XIE') FROM DUAL"
# findTableInfo = "select TABLE_NAME, OWNER from ALL_TABLES where regexp_like(TABLE_NAME , :expression)"
findComment = "SELECT DBMS_METADATA.GET_DEPENDENT_DDL('COMMENT',:1,:2) FROM DUAL"
findConstraint = "SELECT DBMS_METADATA.GET_DEPENDENT_DDL('CONSTRAINT',:1,:2) FROM DUAL"
getTableComment = "SELECT comment FROM all_tab_comments WHERE table_name = :tableName"
findCreation = "SELECT DBMS_METADATA.GET_DDL(:1,:2,:3) FROM DUAL"
tableCommentExist = "SELECT COUNT(*) FROM all_tab_comments WHERE table_name = :tableName AND comments IS NOT NULL"
constraintExist = "SELECT COUNT(*) FROM all_constraints WHERE table_name = :tableName "
columnNum = "SELECT COUNT(*) FROM all_tab_columns WHERE table_name = :tableName"

colComNum = "SELECT COUNT(*) FROM all_col_comments WHERE table_name = :tableName"
colComment = "SELECT column_name, comments FROM all_col_comments WHERE table_name = :tableName"

getPrimaryKey = '''SELECT cols.column_name
FROM all_constraints cons, all_cons_columns cols
WHERE cols.table_name = :tableName
AND cons.constraint_type = 'P'
AND cons.constraint_name = cols.constraint_name
AND cons.owner = cols.owner
ORDER BY cols.table_name, cols.position'''
primaryExist = '''SELECT count(constraint_name) FROM all_constraints
                WHERE table_name = :tableName AND constraint_type = 'P' '''
getForeignKey = "SELECT constraint_name FROM all_constraints WHERE table_name = :tableName AND constraint_type = 'R'"
foreignExist = '''SELECT count(constraint_name) FROM all_constraints
    WHERE table_name = :tableName AND constraint_type = 'R' '''

viewCols = "select column_name from all_tab_columns where table_name = :viewName"
cur = con.cursor()

cur.execute(findObjectInfo, {'expression': str(expression)})
objectInfo = cur.fetchall()

table = open("Table.txt", 'w')
view = open("View.txt", 'w')
pkg = open("pkg.txt", 'w')
trigger = open('trigger.txt', 'w')

for res in objectInfo:
    if res[0] == 'JAVA SOURCE':
        continue
    if res[0] == 'DIRECTORY':
        continue
    print res
    if res[0] == 'PACKAGE BODY':
        res0 = ('PACKAGE_BODY',)
        res1 = res[1:3]
        del res
        res = res0 + res1
        cur.execute(tableCommentExist, {'tableName': str(res[1])})
        tableCommentFlag = cur.fetchall()

    if res[0] == 'VIEW':
        view_res = []
        cur.execute(viewCols, {'viewName': str(res[1])})
        viewContent = cur.fetchall()
        cur.execute(findCreation, res)
        viewCreationInfo = cur.fetchall()[0][0].read().strip()
        print viewCreationInfo
        viewCreationInfo = viewCreationInfo.split('\n')
        viewCreationInfo = viewCreationInfo[1:]
        for i in range(0, len(viewCreationInfo)):
            viewCreationInfo[i].strip()

        viewCreationInfo[0] = viewCreationInfo[0][8:]
        print viewContent
        print "------------------View Create Info -----------------"
        print viewCreationInfo
        print "------------------------------len -----------------"
        print len(viewCreationInfo)

        for i in range(0, min(len(viewContent), len(viewCreationInfo))):
            #viewColName = viewContent[i][0]
            print "------------------i--------------"
            print i
            print len(viewContent)
            viewColName = viewCreationInfo[i]
            print "------------viewColName-----------------"
            print viewColName
            print "------------------split,---specific--------"
            specific_view_col = viewColName.split(',')
            print specific_view_col
            source_table_flag = 0
            for j in range(0, len(specific_view_col)):
                if str(specific_view_col[j]) == '':
                    continue
                view_col_name = specific_view_col[j].strip().split()
                print "----------view_col_name-------------------"
                print view_col_name
                print "-----------------------------"
                if str(view_col_name[0]).lower() == 'from':
                    source_table_flag = 1
                    view_col_name = view_col_name[1:]
                    source_table = "Source Table|" + str(view_col_name[0]) + " AS " + str(view_col_name[1] + '\n')
                    view_res.insert(0, source_table)
                    continue
                if 'from' in view_col_name:
                    view_col_name = specific_view_col[j].strip().split('from')
                    if len(view_col_name) == 2:
                        view_res.append(view_col_name[0] + '|' + view_col_name[0] + '\n')
                        view_res.insert(0, "Source Table|" + str(view_col_name[1]) + '\n')
                    if len(view_col_name) == 3:
                        view_res.append(str(view_col_name[1]) + '|' + str(view_col_name[0]) + '\n')
                        view_res.insert(0, "Source Table| " + str(view_col_name[2]) + '\n')
                        continue
                if source_table_flag == 1:
                    view_res.insert(0, "Source Table|" + str(view_col_name[0]) + " AS " + str(view_col_name[1]) + '\n')
                    continue
                if len(view_col_name) == 1:
                    view_res.append(str(view_col_name[0]) + '|' + str(view_col_name[0]) + '\n')
                    continue

                view_res.append(str(view_col_name[0]) + '|' + str(view_col_name[1]) + '\n')
            if source_table_flag == 1:
                break

        view_res.insert(1, "View Column|Source Table Column")
        for item in view_res:
            view.write("%s\n" % item)

    try:
        if res[0] == 'TABLE':
            # print res
            # Table Creation Query
            cur.execute(tableCommentExist, {'tableName': str(res[1])})
            tableCommentFlag = cur.fetchall()

            if not tableCommentFlag:
                cur.execute(getTableComment, {'tableName': str(res[1])})
                tableComment = cur.fetchall()
            else:
                tableComment = ""

            cur.execute(columnNum, {'tableName': str(res[1])})
            colNum = cur.fetchall()
            table.write('\n' + res[1] + '|' + tableComment + '\n')
            table.write('Column Name|PK|Type|Size|Scale|Null allowed|Default|Description\n')
            cur.execute(findCreation, res)
            createInfo = cur.fetchall()[0][0].read().strip()
            # print res[1]
            # print "00000000000000000"
            if str(res[1]) == 'AP_CHECK_INTEGERS':
                print "Continue"
                sleep(3)
                continue
            if str(res[1]) == 'FND_DUAL':
                print "Continue"
                sleep(3)
                continue
            print createInfo
            print "-----------------------------"
            createInfo = createInfo.split('\n')
            createInfo = createInfo[1:colNum[0][0]]
            for i in range(0, len(createInfo)):
                createInfo[i] = createInfo[i].strip()
            print createInfo
            createInfo[0] = createInfo[0][2:]
            print createInfo
            print "-----------------------------"

            cur.execute(colComNum, {'tableName': str(res[1])})
            colComFlag = cur.fetchall()[0][0]
            colComments = []
            if colComFlag:
                cur.execute(colComment, {'tableName': str(res[1])})
                colComments = cur.fetchall()

            cur.execute(primaryExist, {'tableName': str(res[1])})
            primaryFlag = cur.fetchall()
            primaryName=""
            if primaryFlag[0][0]:
                cur.execute(getPrimaryKey, {'tableName': str(res[1])})
                primaryName = cur.fetchall()[0][0]
            print "-----------Primary------"
            print primaryFlag[0][0]
            print primaryName

            cur.execute(foreignExist, {'tableName': str(res[1])})
            foreignFlag = cur.fetchall()
            if foreignFlag[0][0]:
                cur.execute(getForeignKey, {'tableName': str(res[1])})
                foreignName = cur.fetchall()[0][0]

            for eve in createInfo:
                nullAllowed = 'Y'
                defaultVal = ' '
                size = ' '
                scale = ' '
                comment = ' '
                primary = 'N'
                if "NOT NULL ENABLE" in eve:
                    nullAllowed = 'N'
                eve = eve.split()
                colName = eve[0][1:-1]
                if '(' in eve[1]:
                    typeContent = eve[1].split('(')[0]
                    typeSize = eve[1].split(',')
                    if len(typeSize) == 1:
                        size = typeSize[0][typeSize[0].index('(')+1:-1]
                    else:
                        size = typeSize[0][typeSize[0].index('(')+1:-1]
                        scale = typeSize[1][:-1]
                else:
                    typeContent = eve[1]
                for i in range(0, len(colComments)):
                    if colName == colComments[i][0]:
                        comment = str(colComments[i][1])

                if primaryName == colName:
                    print "Primary Key " + colName
                    primary = 'Y'

                if "DEFAULT" in eve:
                    defaultVal = eve[eve.index("DEFAULT") + 1]
                table.write(colName + "|" +
                            primary + '|' +
                            typeContent + "|" +
                            size + "|" +
                            scale + "|" +
                            nullAllowed + "|" +
                            defaultVal + "|" +
                            comment + '\n')
    except IndexError:
        print IndexError
        continue
#         tableDef.write(createInfo)
#
#         # Constraints Query
#         cur.execute(constraintExist, {'tableName': str(res[1])})
#         constraintFlag = cur.fetchall()
#         # print constraintFlag[0][0]
#         if constraintFlag[0][0] != 0:
#             cur.execute(findConstraint, res[1:3])
#             constraintInfo = cur.fetchall()
#             fh.write(constraintInfo[0][0].read())
#     else:
#         # print res
#         cur.execute(findCreation, res)
#         createInfo = cur.fetchall()
#         fh.write(createInfo[0][0].read())
#
#     fh.close()
#     os.chdir("..")
#
# print "Object Type Succeeds"
# # log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Object Name Succeeds\n")
#
# dirNames = glob.glob(r'.\*')
# # print dirNames
# for dirN in dirNames:
#     if os.path.isdir(dirN):
#         os.chdir(dirN)
#         fileNames = glob.glob(r'*.sql')
#         while fileNames:
#             fileN = fileNames[0]
#             category = str(fileN).split('_')[0]
#             catFileNames = glob.glob(category+'*.sql')
#             outName = str(category) + '_' + str(dirN[2:]) + '.sql'
#             with open('..\\' + outName, 'w') as outfile:
#                 for fname in catFileNames:
#                     with open(fname) as infile:
#                         outfile.write(infile.read())
#                         outfile.write('\n')
#                         infile.close()
#                 outfile.close()
#             fileNames = list(set(fileNames) - set(catFileNames))
#         os.chdir('..')
#
# print "Object Type Succeeds"
# log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Object Type Succeeds\n")

cur.close()
table.close()
view.close()
pkg.close()
trigger.close()

con.close()
# log.close()
