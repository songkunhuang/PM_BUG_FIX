from pandarallel import pandarallel
import re
import pandas as pd
import records
import jsonpath
import cx_Oracle
from jsonpath_ng import parse
from flask import Flask, request, jsonify
import json

pandarallel.initialize(progress_bar=True)



def generate_quart_from_year(db, df_cum, member_num):

    quart_json = json.loads(df_cum.quart_json[member_num])
    year_json = json.loads(df_cum.year_json[member_num])
    quart_template_json = json.loads(df_cum.quart_template_json[member_num])

    quart_data = pd.DataFrame(quart_json['data'])

    quart_data = quart_data.iloc[:, :30]

    self_grade_r = df_cum.grade_position[member_num].split('$')[1]
    self_grade_c = df_cum.grade_position[member_num].split('$')[2]
    leader_grade_r = df_cum.grade_position[member_num].split('$')[3]
    leader_grade_c = df_cum.grade_position[member_num].split('$')[4]

    try:
        formular_res_position = jsonpath.jsonpath(quart_json, "$..celldata[*].v.f", result_type="IPATH")
        formular_res = jsonpath.jsonpath(quart_json, "$..celldata[*].v.f")
        print(formular_res_position, formular_res)
        #
        # 匹配引用公式中的列
        pattern = re.compile(r"!(.+?)[0-9]")
        source_column = [re.findall(pattern, formular_res[i])[0] for i in range(len(formular_res)) if
                         '!' in formular_res[i]]
        source_index = list(set([ord(i) - ord('A') for i in source_column]))

        mid_index = [i for i in range(len(formular_res)) if '!' in formular_res[i]]
        mid_df = pd.DataFrame([formular_res_position[i] for i in mid_index])

        # mid_df['col_num'] = mid_df[1].apply(lambda n: jsonpath.jsonpath(quart_json, "".join(["$..celldata[", str(n), "].c"]))[0])

        # # Initialization

        # #
        mid_df['col_num'] = mid_df[1].parallel_apply(
            lambda n: jsonpath.jsonpath(quart_json, "".join(["$..celldata[", str(n), "].c"]))[0])
        #
        # mid_df['row_num'] = mid_df[1].apply(lambda n: jsonpath.jsonpath(quart_json, "".join(["$..celldata[",str(n),"].r"][0])))

        target_col = list(set(mid_df.col_num.to_list()))

        # 考核评分列的处理

        self_grade = quart_data.iloc[int(self_grade_r), int(self_grade_c)]

        leader_grade = quart_data.iloc[int(leader_grade_r), int(leader_grade_c)]

        print(self_grade_r, self_grade_c, leader_grade_r, leader_grade_c)


        # 考核评分列数据
        eval_data = quart_data.iloc[:, [int(self_grade_c), int(leader_grade_c)]]
        # 考核评分行数据
        eval_row = quart_data.iloc[[int(self_grade_r), int(leader_grade_r)], :]
        print("考核评分行列处理完成")
        year_data = pd.DataFrame(year_json['data'])

        print('source_index:', [chr(i + ord('A')) for i in source_index])
        print('target_col:', [chr(i + ord('A')) for i in target_col])

        # 更新季度data数据
        quart_data.iloc[:, target_col] = year_data.iloc[:, source_index]
        print("季度数据更新完成")
        # 换回考核评分行数据
        quart_data.iloc[[int(self_grade_r), int(leader_grade_r)], :] = eval_row
        # 换回考核评分列数据
        quart_data.iloc[:, [int(self_grade_c), int(leader_grade_c)]] = eval_data
    except Exception as e:
        print('年度模板替换失败 %s' % (e))

    # 换回考核评分模板公式
    finally:
        # 考核评分模板公式
        quart_template_data = pd.DataFrame(quart_template_json['data'])
        self_grade_func = quart_template_data.iloc[[int(self_grade_r)], [int(self_grade_c)]]
        leader_grade_func = quart_template_data.iloc[[int(leader_grade_r)], [int(leader_grade_c)]]

        print("self_grade_func:", self_grade_func)
        print("leader_grade_func", leader_grade_func)

        # #
        quart_data.iloc[[int(self_grade_r)], [int(self_grade_c)]] = self_grade_func
        # #
        quart_data.iloc[[int(leader_grade_r)],  [int(leader_grade_c)]] = leader_grade_func

        parser = parse('$.data')

        # parser.update(quart_json, quart_data.to_json(orient='values', force_ascii=False))
        parser.update(quart_json, quart_data.values.tolist())

        # res = str(quart_json).replace("'",'"').replace('null', '{}')

        res = json.dumps(quart_json, indent=2, sort_keys=True, ensure_ascii=False).replace('null', '{}')

        # 创建数据库连接

        # oracle://prdmart:oracle@10.1.1.121:1521/orcl"
        # 创建游标对象
        cursor = db.cursor()

        # 转化为clob对象
        clob_data = cursor.var(cx_Oracle.CLOB)
        clob_data.setvalue(0, res)
        # 插入语句
        final_sql = "UPDATE prdsys.pm_perf_excel_content set json_str= :res_json, refresh_time = sysdate, refresh_status = 1 where cont_id = :res_cont_id"
        # 插入
        cursor.prepare(final_sql)
        # print(res)
        row = cursor.execute(None, {'res_json': res, 'res_cont_id': df_cum.cont_id[member_num]})
        db.commit()

        # 更新日志表状态
        log_sql = "UPDATE prdsys.pm_refresh_content_log set status = 'S' where cont_id = :res_cont_id"
        cursor.prepare(log_sql)
        row = cursor.execute(None, {'res_cont_id': df_cum.cont_id[member_num]})
        db.commit()

        db.close()


def main_process(cont_id):
    cx_Oracle.init_oracle_client(lib_dir=r"instantclient_19_10")

    # 绩效考核内容表
    conn_info = "oracle://prdmart:oracle@10.1.1.109:1521/orcl"
    db = records.Database(conn_info)
    sql = """  SELECT a.user_id, a.cont_id, e.json_str as quart_template_json, replace(e.json_str2,',','') AS grade_position, a.json_str AS quart_json,a.parent_id as a_parent_id, b.json_str AS year_json,b.parent_id, a.create_time, a.update_time FROM prdsys.pm_perf_excel_content a
                INNER JOIN prdsys.pm_perf_excel_content b ON a.parent_id = b.cont_id
                INNER JOIN prdsys.pm_eval_form c ON a.cont_id = c.cont_id 
                INNER JOIN prdsys.ums_user d ON a.user_id = d.user_id
                INNER JOIN prdsys.pm_template e ON a.template_id = e.template_id
                inner join PRDSYS.PM_EVAL_FORM f on a.cont_id = f.cont_id
                WHERE /*c.status = 21  AND (a.update_time is null or a.create_time > TRUNC(SYSDATE,'DD')) OR*/
                a.cont_id = '{cont_id}'""".format(cont_id=cont_id)
    pre_df_data = db.query(sql).export('df')
    db.close()

    db = cx_Oracle.connect('prdsys', 'oracle', '10.1.1.109:1521/orcl')
    cursor = db.cursor()
    # 插入语句
    backup_sql = """ insert into prdsys.pm_refresh_content_log 
                     select cont_id, quart_json as origin_json, sysdate, 'P' as status from ({sql})""".format(sql=sql)
    # 插入
    cursor.prepare(backup_sql)
    # print(res)
    row = cursor.execute(None)
    db.commit()

    print(pre_df_data.shape)
    print("Read Data Complete!")
    db.close()

    # 备份刷新前的内容

    erro_list = []
    operated_list = []
    for i in range(pre_df_data.shape[0]):
        try:
           write_db_connect = cx_Oracle.connect('prdsys', 'oracle', '10.1.1.109:1521/orcl')
           generate_quart_from_year(db=write_db_connect, df_cum=pre_df_data, member_num=i)
           operated_list.append({'USER_ID': pre_df_data.user_id[i], "CONT_ID": pre_df_data.cont_id[i], "STATUS": 'completed.'})

        except Exception as e:
            print('转换失败 %s' % (e))
            erro_list.append({'USER_ID': pre_df_data.user_id[i], "CONT_ID": pre_df_data.cont_id[i], "ERROR": e})
            continue
        finally:
            print(pre_df_data.cont_id[i])

    if operated_list:
        print("**" * 15)
        print("处理了以下数据：")
        for i in operated_list:
            print(i)

        print("**" * 15)
    else:
        print("**" * 15)
        print("没有处理的数据.")
        print("**" * 15)

    if erro_list:
        print("##" * 15)
        print("注意以下数据未处理:")
        for i in erro_list:
            print(i)
        print("##" * 15)

    return 'Success'


app = Flask(__name__)
app.debug = True


@app.route('/pm/contentFix/', methods=['post'])
def pm_content_fix():
    if not request.data:  # 检测是否有数据
        message = {"status": "Error", "message": "Request Data Error."}
        return jsonify(message)
    request_data = request.data
    # 获取到POST数据
    content_json = json.loads(request_data)
    # 把区获取到的数据转为JSON格式。

    try:
        content_id = content_json['content_id']
        res = main_process(content_id)
        message = {"status": "Success", "message": content_id}
        return jsonify(message)
    except Exception as e:
        message = {"status": "Error", "message": e}
        return jsonify(message)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=19998)
    # 这里指定了地址和端口号。


