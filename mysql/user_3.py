import pymysql

#连接tongyideu
conn = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tongyiedu",charset="utf8")
cursor = conn.cursor()

#连接yxy_basic
conn1 = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_basic",charset="utf8")
cursor1 = conn1.cursor()

#查询所有人
userMemberIdDataSql = "select id,member_id from tifen_user"
cursor.execute(userMemberIdDataSql)
userMemberIdData = cursor.fetchall()

#循环
for userMemberId in userMemberIdData:

    #数据
    user_id = userMemberId[0]
    print(user_id)
    #通过member_id查询yxy_member_tb,有可能有多条，school_id,class_id 同时为0的不要,class_id < 0 的不要,合并排重
    userInfoSql = "select school_id,class_id from yxy_member_tb where member_id = %s and class_id >= 0 and not (school_id = 0 and class_id = 0) group by school_id,class_id"
    cursor1.execute(userInfoSql,str(userMemberId[1]))
    userInfos = cursor1.fetchall()

    if userInfos:

        # 每个人可能有多个学校，多个班级
        for userInfo in userInfos:

            # 公用参数
            school_id = userInfo[0]

            is_ban = 0
            sort_id = 0
            create_time = 0
            active_time = 0

            #class_id = 0 为老师
            if int(userInfo[1]) == 0:
                #查询 yxy_school_class_teacher
                teacherInfosSql = "select class_id,subject_id,max(type) from yxy_school_class_teacher where member_id = %s and school_id = %s group by class_id,subject_id"
                cursor1.execute(teacherInfosSql,(str(userMemberId[1]),str(school_id)))
                teacherInfos = cursor1.fetchall()

                #可能存在多条，也可能没有
                if teacherInfos:
                    for teacherInfo in teacherInfos:
                        if int(teacherInfo[2]) == 3:
                            is_manager = 0
                        else:
                            is_manager = 1
                        class_id = teacherInfo[0]
                        subjects = teacherInfo[1]
                        is_teacher = 1

                        #存入数据库
                        classMateSql = "insert into tifen_school_classmate values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(classMateSql,(str(user_id),str(school_id),str(class_id),str(is_teacher),str(is_manager),str(subjects),str(is_ban),str(sort_id),str(create_time),str(active_time)))
                        conn.commit()
                else:
                    continue
            #否则为学生
            else:
                class_id = userInfo[1]
                is_teacher = 0
                is_manager = 0
                subjects = ""
                # 存入数据库
                classMateSql = "insert into tifen_school_classmate values(null,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                cursor.execute(classMateSql, (
                str(user_id), str(school_id), str(class_id), str(is_teacher), str(is_manager), str(subjects),
                str(is_ban), str(sort_id), str(create_time), str(active_time)))
                conn.commit()
            print(user_id,school_id,class_id,is_teacher,is_manager,subjects)
    #如果没查到跳出循环
    else:
        continue

#关闭数据库
cursor1.close()
conn1.close()
cursor.close()
conn.close()
