import pymysql
#连接原始数据库
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

conn4 = pymysql.connect(host="192.168.1.176",port=3003, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="exameveryday",charset="utf8")
cursor4 = conn4.cursor()

primary = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
junior = (20,21,22,23,24,25,26,27,28,29,30,31,32,53,54,55)
senior = (33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,52,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79)

courseCount = "select * from yxy_school_info_tb where `del` = 1 and review_status=2 and status=1 and disable=0 and display = 0 and keywords='格灵视频' and type in (2,4) and topic in (115,117)"
courseCount = cursor.execute(courseCount)
mainsort = 0
for num in range(courseCount):
    try:
        courseMainSql = "select * from yxy_school_info_tb where `del` = 1 and review_status=2 and status=1 and disable=0 and display = 0 and type in (2,4) and keywords='格灵视频' and topic in (115,117) order by order_id desc limit %d,%d" % (num,1)
        cursor.execute(courseMainSql)
        cursorDataMain = cursor.fetchone()

    #主表id
        id = cursorDataMain[0]

        grade_id = 0
        courseInfoSql = "select * from yxy_info_detail_tb where info_id = %d" % id
        cursor.execute(courseInfoSql)
        cursorDataInfo = cursor.fetchone()

        if cursorDataMain[15]:

            data = cursorDataMain[15].split(":")
            plate = data[1]
        elif int(cursorDataMain[13]) == 115 :

            plateIdSql = 'select plate from t_knowledge_new where id = %s'

            cursor4.execute(plateIdSql,str(cursorDataMain[18]))
            plate = cursor4.fetchone()
            plate = plate[0]
        else:
            plateIdSql = 'select grade_id from t_chapter_new where id = %s'
            cursor.execute(plateIdSql, str(cursorDataMain[16]))
            plate = cursor.fetchone()
            grade_id = plate[0]
            if grade_id in primary:
                plate = 1
            elif grade_id in junior:
                plate = 20
            else:
                plate = 33


        file_source = '格灵视频'
    #print(qcid)
        # qc_state = cursorDataInfo[14]


        sd_url = ''
        state = 1
        is_del = 0
        create_time = cursorDataMain[32]
        type = 1
        cover_url = cursorDataMain[3]

        subject_id = cursorDataMain[14]
        edition_id = cursorDataMain[12]
        chapter_id = cursorDataMain[16]
        knowledge_id = cursorDataMain[18]
        title = cursorDataMain[4]
        teacher_name = cursorDataMain[9]
        file_size = 0
        file_md5 = ''
        file_ext = ''
        src_name = ''
        src_id = ''

        bookIdSql = "select id from tifen_textbook where subject_id = %s and edition_id = %s and grade_id = %s"
        if int(plate) == 1:

            cursor1.execute(bookIdSql,(str(subject_id),str(edition_id),str(grade_id)))
            book_id = cursor1.fetchone()
            if book_id:
                book_id = book_id[0]
            else:
                book_id = 0
        #question_id top = cursorDataMain[13]
            if int(cursorDataMain[13]) == 116:
                questionIdSql = "select id from question_video where video_id = %s AND platform = ''"
                cursor4.execute(questionIdSql,str(id))
                question_Id = cursor4.fetchone()
                if question_Id:
                    question_Id = question_Id[0]
                else:
                    question_Id = 0
            else:
                question_Id = 0

        #is_folder type = cursorDataMain[11]

                is_folder = 1


            parent_id = 0

        #type_id
            if int(cursorDataMain[13]) == 115:
                type_id = 2
            elif int(cursorDataMain[13]) == 116:
                type_id = 3
            elif int(cursorDataMain[13]) == 117:
                type_id = 1
            else:
                type_id = 4

            file_id = 0

            is_review = 1

            creator = 0

            mainsort = mainsort + 1

            hits = cursorDataMain[22]
            description = cursorDataMain[6]

            mainInsertSql = "insert into tifen_course values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor1.execute(mainInsertSql,(str(id),str(grade_id),str(subject_id),str(book_id),str(chapter_id),str(knowledge_id),str(question_Id),str(title),str(is_folder),str(parent_id),str(type_id),str(cover_url),str(file_source),str(file_id),str(mainsort),str(is_del),str(is_review),str(creator),str(create_time),str(hits),str(description)))
            conn1.commit()

        if int(plate) == 20:

            cursor1.execute(bookIdSql, (str(subject_id), str(edition_id), str(grade_id)))
            book_id = cursor1.fetchone()
            if book_id:
                book_id = book_id[0]
            else:
                book_id = 0
        # question_id top = cursorDataMain[13]
            if int(cursorDataMain[13]) == 116:
                questionIdSql = "select id from question_video where video_id = %s AND platform = ''"
                cursor4.execute(questionIdSql, str(id))
                question_Id = cursor4.fetchone()
                if question_Id:
                    question_Id = question_Id[0]
                else:
                    question_Id = 0
            else:
                question_Id = 0

        # is_folder type = cursorDataMain[11]

                is_folder = 1


            parent_id = 0

        # type_id
            if int(cursorDataMain[13]) == 115:
                type_id = 2
            elif int(cursorDataMain[13]) == 116:
                type_id = 3
            elif int(cursorDataMain[13]) == 117:
                type_id = 1
            else:
                type_id = 4

            file_id = cursorDataMain[55]

            is_review = 1

            creator = 0

            mainsort = mainsort + 1

            hits = cursorDataMain[22]
            description = cursorDataMain[6]

            mainInsertSql = "insert into tifen_course values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor2.execute(mainInsertSql, (
            str(id), str(grade_id), str(subject_id), str(book_id), str(chapter_id), str(knowledge_id), str(question_Id),
            str(title), str(is_folder), str(parent_id), str(type_id), str(cover_url),str(file_source), str(file_id), str(mainsort),
            str(is_del), str(is_review),str(creator), str(create_time), str(hits), str(description)))
            conn2.commit()
        if int(plate) == 33:

            cursor1.execute(bookIdSql, (str(subject_id), str(edition_id), str(grade_id)))
            book_id = cursor1.fetchone()
            if book_id:
                book_id = book_id[0]
            else:
                book_id = 0
        # question_id top = cursorDataMain[13]
            if int(cursorDataMain[13]) == 116:
                questionIdSql = "select id from question_video where video_id = %s AND platform = ''"
                cursor4.execute(questionIdSql, str(id))
                question_Id = cursor4.fetchone()
                if question_Id:
                    question_Id = question_Id[0]
                else:
                    question_Id = 0
            else:
                question_Id = 0

        # is_folder type = cursorDataMain[11]

                is_folder = 1


            parent_id = 0

        # type_id
            if int(cursorDataMain[13]) == 115:
                type_id = 2
            elif int(cursorDataMain[13]) == 116:
                type_id = 3
            elif int(cursorDataMain[13]) == 117:
                type_id = 1
            else:
                type_id = 4

            file_id = 0

            is_review = 1

            creator = 0

            mainsort = mainsort + 1

            hits = cursorDataMain[22]
            description = cursorDataMain[6]

            mainInsertSql = "insert into tifen_course values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor3.execute(mainInsertSql, (
            str(id), str(grade_id), str(subject_id), str(book_id), str(chapter_id), str(knowledge_id), str(question_Id),
            str(title), str(is_folder), str(parent_id), str(type_id), str(cover_url),str(file_source), str(file_id), str(mainsort),
            str(is_del), str(is_review),str(creator), str(create_time), str(hits), str(description)))
            conn3.commit()
        print(str(grade_id), str(subject_id), str(edition_id), str(subject_id), str(chapter_id), str(knowledge_id))
    except:
        continue
