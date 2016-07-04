import os
import cx_Oracle
from time import localtime, strftime, sleep
from json import load
from docx import Document
from docx.oxml.ns import nsdecls
from docx.oxml import parse_xml

def init():
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    config = {}
    dir_name = "Data_" + strftime("%Y%m%d_%H%M%S", localtime())
    os.mkdir(dir_name)

    # log = open(dir_name + "/log", "w")
    # log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Getting" + dir_name + "\n")

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
        exit(0)

    print "Connection Established."
    # log.write(strftime("%Y/%m/%d %H:%M:%S", localtime()) + ": Connection established.\n")

    # expression = '^XIE|^CUX'
    # expression = 'XIE_ERP_GL_INT_T'
    expression = config['expression']

    os.chdir(dir_name)
    return con

def get_table_name(expression, con, shading_elem):
    find_object_info = "select OBJECT_TYPE, OBJECT_NAME, OWNER from ALL_OBJECTS where regexp_like(OBJECT_NAME ,:expression) AND OBJECT_TYPE='TABLE'"
    table_comment_exist = "SELECT COUNT(*) FROM all_tab_comments WHERE table_name = :tableName AND comments IS NOT NULL"
    get_table_comment = "SELECT comment FROM all_tab_comments WHERE table_name = :tableName"
    column_num = "SELECT COUNT(*) FROM all_tab_columns WHERE table_name = :tableName"
    col_com_num = "SELECT COUNT(*) FROM all_col_comments WHERE table_name = :tableName"
    col_comment = "SELECT column_name, comments FROM all_col_comments WHERE table_name = :tableName"

    get_primary_key = '''SELECT cols.column_name
    FROM all_constraints cons, all_cons_columns cols
    WHERE cols.table_name = :tableName
    AND cons.constraint_type = 'P'
    AND cons.constraint_name = cols.constraint_name
    AND cons.owner = cols.owner
    ORDER BY cols.table_name, cols.position'''
    primary_exist = '''SELECT count(constraint_name) FROM all_constraints
                    WHERE table_name = :tableName AND constraint_type = 'P' '''
    get_foreign_key = "SELECT constraint_name FROM all_constraints WHERE table_name = :tableName AND constraint_type = 'R'"
    foreign_exist = '''SELECT count(constraint_name) FROM all_constraints
        WHERE table_name = :tableName AND constraint_type = 'R' '''
    find_creation = "SELECT DBMS_METADATA.GET_DDL(:1,:2,:3) FROM DUAL"

    cur = con.cursor()

    cur.execute(find_object_info, {'expression': str(expression)})
    object_info = cur.fetchall()

    table = Document()
    table_name = Document()
    for res in object_info:
        try:
            # print res
            # Table Creation Query
            cur.execute(table_comment_exist, {'tableName': str(res[1])})
            table_comment_flag = cur.fetchall()

            if not table_comment_flag:
                cur.execute(get_table_comment, {'tableName': str(res[1])})
                table_comment = cur.fetchall()
            else:
                table_comment = ""

            cur.execute(column_num, {'tableName': str(res[1])})
            col_num = cur.fetchall()

            p = table_name.add_paragraph(res[1] + '   ' + table_comment)

            docx_table = table.add_table(rows = 1, cols = 8)
            first_row_cells = docx_table.row(0).cells
            first_row_cells[0] = 'Column Name'
            first_row_cells[0]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[1] = 'PK'
            first_row_cells[1]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[2] = 'Type'
            first_row_cells[2]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[3] = 'Size'
            first_row_cells[3]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[4] = 'Scale'
            first_row_cells[4]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[5] = 'Null allowed'
            first_row_cells[5]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[6] = 'Default'
            first_row_cells[6]._tc.get_or_add_tcPr().append(shading_elm)
            first_row_cells[7] = 'Description'
            first_row_cells[7]._tc.get_or_add_tcPr().append(shading_elm)

            cur.execute(find_creation, res)
            create_info = cur.fetchall()[0][0].read().strip()
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
            print create_info
            print "-----------------------------"
            create_info = create_info.split('\n')
            create_info = create_info[1:col_num[0][0]]
            for i in range(0, len(create_info)):
                create_info[i] = create_info[i].strip()
            print create_info
            create_info[0] = create_info[0][2:]
            print create_info
            print "-----------------------------"

            cur.execute(col_com_num, {'tableName': str(res[1])})
            col_com_flag = cur.fetchall()[0][0]
            col_comments = []
            if col_com_flag:
                cur.execute(col_comment, {'tableName': str(res[1])})
                col_comments = cur.fetchall()

            cur.execute(primary_exist, {'tableName': str(res[1])})
            primary_flag = cur.fetchall()
            primary_name = ""
            if primary_flag[0][0]:
                cur.execute(get_primary_key, {'tableName': str(res[1])})
                primary_name = cur.fetchall()[0][0]
            print "-----------Primary------"
            print primary_flag[0][0]
            print primary_name

            cur.execute(foreign_exist, {'tableName': str(res[1])})
            foreign_flag = cur.fetchall()
            if foreign_flag[0][0]:
                cur.execute(get_foreign_key, {'tableName': str(res[1])})
                foreign_name = cur.fetchall()[0][0]

            for eve in create_info:
                null_allowed = 'Y'
                default_value = ' '
                size = ' '
                scale = ' '
                comment = ' '
                primary = 'N'
                if "NOT NULL ENABLE" in eve:
                    null_allowed = 'N'
                eve = eve.split()
                col_name = eve[0][1:-1]
                if '(' in eve[1]:
                    type_content = eve[1].split('(')[0]
                    type_size = eve[1].split(',')
                    if len(type_size) == 1:
                        size = type_size[0][type_size[0].index('(') + 1:-1]
                    else:
                        size = type_size[0][type_size[0].index('(') + 1:-1]
                        scale = type_size[1][:-1]
                else:
                    type_content = eve[1]
                for i in range(0, len(col_comments)):
                    if col_name == col_comments[i][0]:
                        comment = str(col_comments[i][1])

                if primary_name == col_name:
                    print "Primary Key " + col_name
                    primary = 'Y'

                if "DEFAULT" in eve:
                    default_value = eve[eve.index("DEFAULT") + 1]
                
                cells = table.add_row().cells
                cells[0] = col_name
                cells[1] = primary 
                cells[2] = type_content 
                cells[3] = size 
                cells[4] = scale 
                cells[5] = null_allowed 
                cells[6] = default_value 
                cells[7] = comment 
                
        except IndexError:
            print IndexError
            continue
        table.save('Table.docx')
        table_name.save('TableName.docx')
        cur.close()

con = init()
# Set a cell background (shading) color to RGB A0A0A0(Gray).
shading_elm = parse_xml(r'<w:shd {} w:fill="A0A0A0"/>'.format(nsdecls('w')))
get_table_name('^XIE', con, shading_elm)
