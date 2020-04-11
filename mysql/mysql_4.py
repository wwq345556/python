#!usr/bin/python

import pymysql
import json

primary = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
junior = (20,21,22,23,24,25,26,27,28,29,30,31,32,53,54,55)
senior = (33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,52,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79)
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3003, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="exameveryday",charset="utf8")

cursor = conn.cursor()

sql = """
select id,parent_id,knowledge_num,type_id,mode_id,difficulty,grade_id,subject_id,edition_id,source_name,score,audio_url,question,`options`,answer,solution,analysis,`comment`,video_num,quote_num,state,0 as sort_id,category_id,is_objective from t_question_2;
"""

cursor.execute(sql)
alldata = cursor.fetchall()

#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
#连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
#连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()


# print(alldata)
for data in alldata:
        print(data)
    # try:
        sql = "insert into tifen_question_2 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # print(sql)
        total_sql = "select * from t_question_id where id = %s"
        cursor.execute(total_sql,str(data[0]))
        qusetion_data = cursor.fetchone()
        id_sql = "insert into tifen_question_id values(%s,%s,%s,%s,%s,%s)" 

        #清洗数据
        if data[4] in (1,2,3,4,5):
            # dataList = eval(data[13])
            dataList = json.loads(data[13])
            # print(type(dataList))
            optionList = []
            # print(dataList)
            for content in dataList:
                if isinstance(content,dict):
                    optionList.append(content['content'])

            optionList = str(json.dumps(optionList))
            
    
        if int(data[4]) in (6,):
            # print(data[0])
            optionList = []

            # for t6_list in eval(data[13]):
            for t6_list in json.loads(data[13]):
                # print(t6_list)
                t6_list2 = []
                for t6_answer_list in t6_list:

                    if isinstance(t6_answer_list,dict):
                        # print(t6_answer_list['content'])
                        t6_list2.append(t6_answer_list['content'])
                # t6_list2 = json.dumps(t6_list2)   
                # print(t6_list2)
                if not len(t6_list2):
                    continue   
                optionList.append(t6_list2)
            optionList = json.dumps(optionList)
            # print(optionList)
        if int(data[4]) not in (1,2,3,4,5,6):
            optionList = ''

        # answer = '[' + str(data[15]) + ']'

        # print(optionList)
        if data[6] in primary:
        
            
            cursor1.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[23]),str(data[22]),str(data[5]),str(data[6]),str(data[7]),str(data[8]),str(data[9]),str(data[10]),str(data[11]),str(data[12]),str(optionList),str(data[14]),str(data[15]),str(data[16]),str(data[17]),str(data[18]),str(data[19]),str(data[20]),str(data[21])))
            conn1.commit()
            cursor1.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn1.commit()
        
        if data[6] in junior:
        

            cursor2.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[23]),str(data[22]),str(data[5]),str(data[6]),str(data[7]),str(data[8]),str(data[9]),str(data[10]),str(data[11]),str(data[12]),str(optionList),str(data[14]),str(data[15]),str(data[16]),str(data[17]),str(data[18]),str(data[19]),str(data[20]),str(data[21])))

            conn2.commit()
            cursor2.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn2.commit()
        
        if data[6] in senior:
            
            cursor3.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[23]),str(data[22]),str(data[5]),str(data[6]),str(data[7]),str(data[8]),str(data[9]),str(data[10]),str(data[11]),str(data[12]),str(optionList),str(data[14]),str(data[15]),str(data[16]),str(data[17]),str(data[18]),str(data[19]),str(data[20]),str(data[21])))
            conn3.commit()

            cursor3.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn3.commit()
    # except:
    #     # print(1111)
    #     continue
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()

