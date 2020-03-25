import pymysql

#连接tongyideu
conn = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tongyiedu",charset="utf8")
cursor = conn.cursor()

#连接yxy_basic
conn1 = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_basic",charset="utf8")
cursor1 = conn1.cursor()

#连接vsdeu
conn2 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="vsedu",charset="utf8")
cursor2 = conn2.cursor()



#连接yxy_education
conn4 = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")
cursor4 = conn4.cursor()

#查询所有tifen_user 里的学校id
schoolIdSql = "select school_id from tifen_user where school_id <> 0 group by school_id"
cursor.execute(schoolIdSql)
schoolId = cursor.fetchall()

#整理一下数据结构
school_ids = set()
for school_id in schoolId:
    school_ids.add(school_id[0])

# print(school_ids)
for school in school_ids:
    print(school)
    userMemberIdSql = "select member_id from yxy_member_tb where class_id >= 0 and school_id = %s"
    cursor1.execute(userMemberIdSql,str(school))
    userMemberId = cursor1.fetchall()

    #整理数据结构
    MemberIdSet = set()
    for MemberId in userMemberId:
        MemberIdSet.add(MemberId[0])

    for member_id in MemberIdSet:
        memberExistSql = "select id from tifen_user where member_id = %s"
        cursor.execute(memberExistSql,str(member_id))
        userId = cursor.fetchone()
        if userId:
            continue
        else:

            # vsedu.vs_member_tb member_id,member_pwd,member_name真实姓名,create_time,enable
            # enable 1=>0 其余=>1

            vseduSql = "select member_id,member_pwd,member_name,UNIX_TIMESTAMP(create_time) as create_time,enable,id from vs_member_tb where member_id = %s"
            cursor2.execute(vseduSql, (str(member_id)))
            vsedu = cursor2.fetchone()


            # 构造数据
            id = vsedu[5]
            member_id = vsedu[0]
            member_pwd = vsedu[1]
            realname = vsedu[2]
            create_time = vsedu[3]
            if int(vsedu[4]) == 1:
                disabled = 0
            else:
                disabled = 1
            sex = 0
            source_id = 0
            photo = ""
            nickname = ""
            phone_no = ""
            phone_bind = 0


            # yxy_basic.yxy_member_tb school_id,判断是否是教师class_id = 0 => 教师 在A学校是老师，B学校是学生他就是学生
            yxyMemberSql = "select school_id,class_id from yxy_member_tb where member_id = %s"
            cursor1.execute(yxyMemberSql, (str(member_id)))
            yxyMember = cursor1.fetchall()

            # 如果学校，班级为空，为学生
            if yxyMember:
                # 集合运算
                classId = set()
                for member in yxyMember:
                    classId.add(member[1])

                # 班级集合，只有不为0的就是学生
                classId.add(0)
                if max(classId) != 0:
                    is_teacher = 0
                else:
                    is_teacher = 1

            else:

                is_teacher = 0

            # 判断是老师还是学生(学生subject = 0,教师plate_id = 0)
            if is_teacher:
                # yxy_basic.t_teacher_config subjects is null subjects = subject_id(学生subjects = 0)
                teacherConfigSql = "select subject_id,subjects from t_teacher_config where user_id = %s"
                cursor1.execute(teacherConfigSql, (str(id)))
                teacherConfig = cursor1.fetchone()

                if teacherConfig:
                    # 如果subjects存在取subjects，否则取subject_id
                    if teacherConfig[1]:
                        subjects = teacherConfig[1]
                    else:
                        subjects = teacherConfig[0]
                else:
                    subjects = ""

                plate_id = 0

            else:
                # yxy_education.t_user_config_grade plate_id(教师plate_id = 0)
                userConfigSql = "select plate_id from t_user_config_grade where user_id = %s"
                cursor4.execute(userConfigSql, (str(id)))
                userConfig = cursor4.fetchone()

                if userConfig:
                    plate_id = userConfig[0]
                else:
                    plate_id = 0

                subjects = 0

            # 插入数据库
            userSql = "insert into tifen_user values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            print(str(school),str(member_id),str(nickname),str(realname),str(member_pwd),str(sex),str(is_teacher),str(subjects),str(plate_id),str(photo),str(phone_no),str(phone_bind),str(disabled),str(source_id),str(create_time))
            cursor.execute(userSql, (
            str(id), str(school), str(member_id), str(nickname), str(realname), str(member_pwd), str(sex),
            str(is_teacher), str(subjects), str(plate_id), str(photo), str(phone_no), str(phone_bind), str(disabled),
            str(source_id), str(create_time)))
            conn.commit()

#关闭数据库
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn4.close()
cursor.close()
conn.close()

