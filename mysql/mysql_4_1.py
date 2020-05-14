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
select id,parent_id,knowledge_num,type_id,mode_id,difficulty,grade_id,subject_id,edition_id,source_name,score,audio_url,question,`options`,answer,solution,analysis,`comment`,video_num,quote_num,state,0 as sort_id,category_id,is_objective,is_read,plate_id from t_question_1 where plate_id = 0 and grade_id = 0;
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
    try:
        parentSql = "select plate_id,grade_id from t_question_1 where id = %s"
        cursor.execute(parentSql, str(data[1]))
        parentData = cursor.fetchone()
        print(parentData)
        print(data)
        id = data[0]
        # company_id = 0
        parent_id = data[1]
        knowledge_num = data[2]
        type_id = data[3]
        mode_id = data[4]
        is_objective = data[23]
        env_id = data[22]
        difficulty = data[5]
        grade_id = parentData[1]
        subject_id = data[7]
        edition_id = data[8]
        source_name= data[9]
        score = data[10]
        audio_url = data[11]
        contents = data[12]
        options = data[13]
        answer = data[14]
        solution = data[15]
        analysis = data[16]
        comment = data[17]
        video_num = data[18]
        state = data[20]
        hits = data[19]
        is_read = data[24]
        sort_id = 0

        sql = "insert into tifen_question_1 (id,parent_id,knowledge_num,type_id,mode_id,is_objective,env_id,difficulty,grade_id,subject_id,edition_id,source_name,score,audio_url,content,`options`,answer,solution,analysis,`comment`,video_num,hits,is_read,state,sort_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # print(sql)
        total_sql = "select * from t_question_id where id = %s"
        cursor.execute(total_sql,str(data[0]))
        qusetion_data = cursor.fetchone()
        id_sql = "insert into tifen_question_id values(%s,%s,%s,%s,%s,%s)" 
        print(data[6])
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
        if int(parentData[1]) in primary or int(parentData[0]) == 1:


            cursor1.execute(sql,(str(id),str(parent_id),str(knowledge_num),str(type_id),str(mode_id),str(is_objective),str(env_id),str(difficulty),str(grade_id),str(subject_id),str(edition_id),str(source_name),str(score),str(audio_url),str(contents),str(optionList),str(answer),str(solution),str(analysis),str(comment),str(video_num),str(hits),str(is_read),str(state),str(sort_id)))
            conn1.commit()
            cursor1.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn1.commit()
        
        if int(parentData[1]) in junior or int(parentData[0]) == 20:
        

            cursor2.execute(sql,(str(id),str(parent_id),str(knowledge_num),str(type_id),str(mode_id),str(is_objective),str(env_id),str(difficulty),str(grade_id),str(subject_id),str(edition_id),str(source_name),str(score),str(audio_url),str(contents),str(optionList),str(answer),str(solution),str(analysis),str(comment),str(video_num),str(hits),str(is_read),str(state),str(sort_id)))

            conn2.commit()
            cursor2.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn2.commit()
        
        if int(parentData[1]) in senior or int(parentData[0]) == 33:

            cursor3.execute(sql,(str(id),str(parent_id),str(knowledge_num),str(type_id),str(mode_id),str(is_objective),str(env_id),str(difficulty),str(grade_id),str(subject_id),str(edition_id),str(source_name),str(score),str(audio_url),str(contents),str(optionList),str(answer),str(solution),str(analysis),str(comment),str(video_num),str(hits),str(is_read),str(state),str(sort_id)))
            conn3.commit()

            cursor3.execute(id_sql,(str(qusetion_data[0]),str(qusetion_data[1]),str(qusetion_data[2]),str(qusetion_data[3]),str(qusetion_data[4]),str(qusetion_data[5])))
            conn3.commit()

    except:
        continue
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()

