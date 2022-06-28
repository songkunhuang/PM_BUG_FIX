import re
import pandas as pd
import records
import json
import jsonpath
import cx_Oracle
from jsonpath_ng import parse
# from pandarallel import pandarallel




def query_grade(db, df_cum, member_num):
    quart_json = json.loads(df_cum.quart_json[member_num])

    quart_data = pd.DataFrame(quart_json['data'])
    print(quart_data.shape)
    print(quart_data.iloc[:,500:-1])

    # 考核评分列的处理
    self_grade_r = df_cum.grade_position[member_num].split('$')[1]
    self_grade_c = df_cum.grade_position[member_num].split('$')[2]
    leader_grade_r = df_cum.grade_position[member_num].split('$')[3]
    leader_grade_c = df_cum.grade_position[member_num].split('$')[4]

    print(self_grade_r,self_grade_c,leader_grade_r,leader_grade_c)

    self_grade = quart_data.iloc[int(self_grade_r), int(self_grade_c)]

    leader_grade = quart_data.iloc[int(leader_grade_r), int(leader_grade_c)]

    print(self_grade['v'],leader_grade['v'])


    # 创建数据库连接

    # oracle://prdmart:oracle@10.1.1.121:1521/orcl"
    # 创建游标对象
    cursor = db.cursor()

    # 转化为clob对象
    # 插入语句

    final_sql = "delete from prdsys.pm_excel_parser_grade where cont_id = :cont_id"
                # "insert into prdsys.pm_excel_parser_grade values (:cont_id, :self_grade, :leader_grade);"

    # 插入
    cursor.prepare(final_sql)

    row = cursor.execute(None, {'cont_id': df_cum.cont_id[member_num]})
    db.commit()

    next_sql = "insert into prdsys.pm_excel_parser_grade values (:cont_id, :self_grade, :leader_grade)"
    # "insert into prdsys.pm_excel_parser_grade values (:cont_id, :self_grade, :leader_grade);"

    # 插入
    cursor.prepare(next_sql)

    row = cursor.execute(None, {'cont_id': df_cum.cont_id[member_num], 'self_grade': self_grade['v'], 'leader_grade': leader_grade['v'] })
    db.commit()



if __name__ == '__main__':

    cx_Oracle.init_oracle_client(lib_dir=r"instantclient_19_10")

    # 绩效考核内容表
    conn_info = "oracle://prdmart:oracle@10.1.1.109:1521/orcl"
    db = records.Database(conn_info)
    sql = """  SELECT a.user_id, a.cont_id, replace(e.json_str2,',','') AS grade_position, a.json_str AS quart_json,a.parent_id as a_parent_id,b.parent_id, a.create_time, a.update_time FROM prdsys.pm_perf_excel_content a
INNER JOIN prdsys.pm_perf_excel_content b ON a.parent_id = b.cont_id
INNER JOIN prdsys.pm_eval_form c ON a.cont_id = c.cont_id 
INNER JOIN prdsys.ums_user d ON a.user_id = d.user_id
INNER JOIN prdsys.pm_template e ON a.template_id = e.template_id where d.nick_name = '黄燕华'"""
    pre_df_data = db.query(sql).export('df')
    print(pre_df_data.quart_json)
    db.close()
    print("Read Data Complete!")

    for i in range(pre_df_data.shape[0]):
        try:
            write_db_connect = cx_Oracle.connect('prdsys', 'oracle', '10.1.1.109:1521/orcl')
            query_grade(db=write_db_connect, df_cum=pre_df_data, member_num=i)
            write_db_connect.close()

        except Exception as e:
            print("错误：", str(e))
            continue
        finally:
            print(pre_df_data.cont_id[i])
            print("="*10)

    print('complete')



