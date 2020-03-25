import pymysql

#连接yxy_education
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")
cursor = conn.cursor()

#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
#连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
#连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()

#knowledgeBook
knowledgeBookDataSql = "select id,pid,plate_id,name,subject_id,sort_id,create_time,knowledge_id from t_knowledge_book where is_del = 0"
cursor.execute(knowledgeBookDataSql)
knowledgeBookData = cursor.fetchall()

clean = set()
#循环

for knowledgeBook in knowledgeBookData:
    try:
        id = knowledgeBook[0] + 50000
        vid = id
        if int(knowledgeBook[1]) == 0 :
            pid = 0
        else:
            pid = knowledgeBook[1] + 50000
        full_id = ""
        subject_id = knowledgeBook[4]
        is_folder = 1
        name = knowledgeBook[3]
        difficulty = 0
        video_num = 0
        question_num = 0
        chapter_num = 0
        sort_id = knowledgeBook[5]
        is_del = 0
        creator = 1
        create_time = knowledgeBook[6]
        is_review = 0
        print(id,pid,subject_id,is_folder,difficulty,sort_id,create_time)
        #分库
        knowledgeBookDataInsertSql = "insert into tifen_knowledge values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        if int(knowledgeBook[2]) == 1:
            cursor1.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
            conn1.commit()

        if int(knowledgeBook[2]) == 20:
            cursor2.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
            conn2.commit()

        if int(knowledgeBook[2]) == 33:
            cursor3.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
            conn3.commit()

        if knowledgeBook[7]:

            knowledge = knowledgeBook[7].split(',')
            # print(knowledge)
            for know in knowledge:
                id = know
                knowledgeDataSql = "select id,pid,plate,name,subject,sort_id,video_num,question_num,difficulty from t_knowledge_new where is_del = 0 and id = %s"
                cursor.execute(knowledgeDataSql,str(id))
                knowledgeData = cursor.fetchone()
                print(knowledgeData)
                if knowledgeData:
                    pid = vid
                    full_id = ""
                    subject_id = knowledgeData[4]
                    is_folder = 0
                    name = knowledgeData[3]
                    difficulty = knowledgeData[8]
                    video_num = knowledgeData[6]
                    question_num = knowledgeData[7]
                    chapter_num = 0
                    sort_id = knowledgeData[5]
                    is_del = 0
                    creator = 1
                    create_time = knowledgeBook[6]
                    is_review = 0
                    print(id, pid, subject_id, is_folder, difficulty, sort_id, create_time)
                    #分库
                    knowledgeBookDataInsertSql = "insert into tifen_knowledge values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    if int(knowledgeBook[2]) == 1:
                        cursor1.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
                        conn1.commit()

                    if int(knowledgeBook[2]) == 20:
                        cursor2.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
                        conn2.commit()

                    if int(knowledgeBook[2]) == 33:
                        cursor3.execute(knowledgeBookDataInsertSql,(str(id),str(pid),str(full_id),str(subject_id),str(is_folder),str(name),str(difficulty),str(video_num),str(question_num),str(chapter_num),str(sort_id),str(is_del),str(creator),str(create_time),str(is_review)))
                        conn3.commit()
    except:
        clean.add(id)
        continue

print(clean)

cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()