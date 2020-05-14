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

courseCount = "select * from yxy_school_info_tb where `del` = 1 and review_status=2 and status=1 and disable=0 and display = 0 and type = 4 and topic = 117 and keywords <> '格灵视频'"

courseCount = cursor.execute(courseCount)
mainsort = 0
for num in range(courseCount):
    try:
        courseMainSql = "select * from yxy_school_info_tb where `del` = 1 and review_status=2 and status=1 and disable=0 and display = 0 and type = 4 and topic = 117 and keywords <> '格灵视频' order by order_id desc limit %d,%d" % (num,1)
        cursor.execute(courseMainSql)
        cursorDataMain = cursor.fetchone()
    #print(cursorDataMain[15])
    #主表id
        id = cursorDataMain[0]
        #id => info_id
        childIdSql = "select info_id from yxy_data_group_detail_tb where `group`= %d order by order_id asc" % id
        cursor.execute(childIdSql)
        childId = cursor.fetchall()
        print(childId)
        for child in childId:
            id = child[0]
            courseInfoSql = "select * from yxy_info_detail_tb where info_id = %d" % id
            cursor.execute(courseInfoSql)
            cursorDataInfo = cursor.fetchone()

            # 反查title
            titleSql = "select title from yxy_school_info_tb where id = %d" % id
            cursor.execute(titleSql)
            titleData = cursor.fetchone()

            # 通过知识点查询plate grade
            chapter_id = cursorDataMain[16]
            gradeIdSql = "select grade_id from t_chapter_new where id = %d" % chapter_id
            cursor.execute(gradeIdSql)
            gradeId = cursor.fetchone()
            grade_id = gradeId[0]
            print(grade_id)



            if 'lele' in str(cursorDataInfo[10]):
                qcid = cursorDataInfo[5]
            else:
                qcid = cursorDataInfo[10]
            file_source = ''
    #print(qcid)
            qc_state = cursorDataInfo[14]

            urlSql = "select `url` from yxy_info_qcvideo_tb where qcid = %s and definition=0"
            cursor.execute(urlSql,str(qcid))
            url = cursor.fetchone()
            if url:
                file_url = url[0]
            else:
                file_url = ''

            urlSql = "select `url` from yxy_info_qcvideo_tb where qcid = %s and definition=20"
            cursor.execute(urlSql,str(qcid))
            url = cursor.fetchone()
            if url:
                md_url = url[0]
            else:
                urlSql = "select `url` from yxy_info_qcvideo_tb where qcid = %s and definition=230"
                cursor.execute(urlSql,str(qcid))
                url = cursor.fetchone()
                if url :
                    md_url = url[0]
                else:
                    md_url = ''

            urlSql = "select `url` from yxy_info_qcvideo_tb where qcid = %s and definition=30"
            cursor.execute(urlSql,str(qcid))
            url = cursor.fetchone()
            if url:
                hd_url = url[0]
            else:
                urlSql = "select `url` from yxy_info_qcvideo_tb where qcid = %s and definition=220"
                cursor.execute(urlSql,str(qcid))
                url = cursor.fetchone()
                if url:
                    hd_url = url[0]
                else:
                    hd_url = ''

            sd_url = ''
            state = 1
            is_del = 0
            create_time = cursorDataMain[32]
            type = 1
            cover_url = cursorDataMain[3]
            sha1_code = cursorDataInfo[17]
            subject_id = cursorDataMain[14]
            edition_id = cursorDataMain[12]
            chapter_id = cursorDataMain[16]
            knowledge_id = cursorDataMain[18]
            title = titleData[0]
            teacher_name = cursorDataMain[9]
            file_size = 0
            file_md5 = ''
            file_ext = ''
            src_name = ''
            src_id = ''

            durationSql = "select max(`duration`) as duration from yxy_info_qcvideo_tb where qcid = %s and (definition=0 or definition=20 or definition=30)"
            cursor.execute(durationSql,str(qcid))
            url = cursor.fetchone()
            if url:
                duration = url[0]
            else:
                duration = 0


            infoInsertSql = "insert into tifen_file_detail values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            infoInsertIdSql = "select id from tifen_file_detail order by id DESC limit 1"
            bookIdSql = "select id from tifen_textbook where subject_id = %s and edition_id = %s and grade_id = %s"
            if grade_id in primary:
                cursor1.execute(infoInsertSql, (
                str(grade_id), str(subject_id), str(edition_id), str(chapter_id), str(knowledge_id), str(type), str(title),
                str(teacher_name), str(file_size), str(file_md5), str(sha1_code), str(file_ext), str(qcid), str(qc_state),
                str(cover_url), str(file_url), str(md_url), str(hd_url), str(sd_url), str(duration), str(state),
                str(is_del), str(create_time), str(src_name), str(src_id)))
                conn1.commit()
                cursor1.execute(infoInsertIdSql)
                info_id = cursor1.fetchone()
                info_id = info_id[0]

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
                    is_folder = 0

                parent_id = cursorDataMain[0]

        #type_id
                if int(cursorDataMain[13]) == 115:
                    type_id = 2
                elif int(cursorDataMain[13]) == 116:
                    type_id = 3
                elif int(cursorDataMain[13]) == 117:
                    type_id = 1
                else:
                    type_id = 4

                file_id = info_id

                is_review = 1

                creator = 0

                mainsort = mainsort + 1

                hits = cursorDataMain[22]
                description = cursorDataMain[6]

                mainInsertSql = "insert into tifen_course values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor1.execute(mainInsertSql,(str(id),str(grade_id),str(subject_id),str(book_id),str(chapter_id),str(knowledge_id),str(question_Id),str(title),str(is_folder),str(parent_id),str(type_id),str(cover_url),str(file_source),str(file_id),str(mainsort),str(is_del),str(is_review),str(creator),str(create_time),str(hits),str(description)))
                conn1.commit()

            if grade_id in junior:
                cursor2.execute(infoInsertSql, (
                    str(grade_id), str(subject_id), str(edition_id), str(chapter_id), str(knowledge_id),
                    str(type), str(title),
                    str(teacher_name), str(file_size), str(file_md5), str(sha1_code), str(file_ext), str(qcid), str(qc_state),
                    str(cover_url), str(file_url), str(md_url), str(hd_url), str(sd_url), str(duration), str(state),
                    str(is_del), str(create_time), str(src_name), str(src_id)))
                conn3.commit()
                cursor2.execute(infoInsertIdSql)
                info_id = cursor2.fetchone()
                info_id = info_id[0]

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
                    is_folder = 0

                parent_id = cursorDataMain[0]

        # type_id
                if int(cursorDataMain[13]) == 115:
                    type_id = 2
                elif int(cursorDataMain[13]) == 116:
                    type_id = 3
                elif int(cursorDataMain[13]) == 117:
                    type_id = 1
                else:
                    type_id = 4

                file_id = info_id

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
            if grade_id in senior:
                cursor3.execute(infoInsertSql, (
                    str(grade_id), str(subject_id), str(edition_id), str(chapter_id), str(knowledge_id),
                    str(type), str(title),
                    str(teacher_name), str(file_size), str(file_md5), str(sha1_code), str(file_ext), str(qcid), str(qc_state),
                    str(cover_url), str(file_url), str(md_url), str(hd_url), str(sd_url), str(duration), str(state),
                    str(is_del), str(create_time), str(src_name), str(src_id)))
                conn3.commit()
                cursor3.execute(infoInsertIdSql)
                info_id = cursor3.fetchone()
                info_id = info_id[0]

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

                    is_folder = 0

                parent_id = cursorDataMain[0]

        # type_id
                if int(cursorDataMain[13]) == 115:
                    type_id = 2
                elif int(cursorDataMain[13]) == 116:
                    type_id = 3
                elif int(cursorDataMain[13]) == 117:
                    type_id = 1
                else:
                    type_id = 4

                file_id = info_id

                is_review = 1

                creator = 0

                mainsort = mainsort + 1

                hits = cursorDataMain[22]
                description = cursorDataMain[6]

                mainInsertSql = "insert into tifen_course values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor3.execute(mainInsertSql, (
                str(id), str(grade_id), str(subject_id), str(book_id), str(chapter_id), str(knowledge_id), str(question_Id),
                str(title), str(is_folder), str(parent_id), str(type_id), str(cover_url),str(file_source), str(file_id),
                str(mainsort),str(is_del), str(is_review),str(creator), str(create_time), str(hits), str(description)))
                conn3.commit()
            print(str(grade_id), str(subject_id), str(edition_id), str(subject_id), str(chapter_id), str(knowledge_id))
    except:
        continue

