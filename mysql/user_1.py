import pymysql

#连接tifen_mall
conn = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tifen_mall",charset="utf8")
cursor = conn.cursor()

#连接tongyideu
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tongyiedu",charset="utf8")
cursor1 = conn1.cursor()

#连接vsdeu
conn2 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="vsedu",charset="utf8")
cursor2 = conn2.cursor()

#连接yxy_basic
conn3 = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_basic",charset="utf8")
cursor3 = conn3.cursor()

#连接yxy_education
conn4 = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")
cursor4 = conn4.cursor()

#查询所有人数据
userIdSql = "select user_id,sign_times,sign_serial_days,sign_day,sign_time,from_unixtime(create_time),point_total,point_balance from user_basic"
cursor.execute(userIdSql)
alldata = cursor.fetchall()

#循环拼凑数据
for data in alldata:
    print(data)
    try:
        #将数据插入签到表
        signSql = "insert into tifen_user_sign values(%s,%s,%s,%s,%s,%s)"
        cursor1.execute(signSql,((str(data[0])),(str(data[1])),(str(data[2])),(str(data[3])),(str(data[4])),(str(data[5]))))
        conn1.commit()

        #vsedu.vs_member_tb member_id,member_pwd,member_name真实姓名,create_time,enable
        #enable 1=>0 其余=>1
        vseduSql = "select member_id,member_pwd,member_name,UNIX_TIMESTAMP(create_time) as create_time,enable,id from vs_member_tb where id = %s"
        cursor2.execute(vseduSql,(str(data[0])))
        vsedu = cursor2.fetchone()

        #如果查不到，丢弃
        if vsedu is None:
            continue
        #构造数据
        id = vsedu[5]
        member_id = vsedu[0]
        member_pwd = vsedu[1]
        realname = vsedu[2]
        create_time = vsedu[3]
        credits_total = int(data[6])
        credits_balance = int(data[7])
        if int(vsedu[4]) == 1:
            disabled = 0
        else:
            disabled = 1
        sex = 0
        source_id = 0

        #vsedu.ty_basic_info photo,member_name昵称
        tyBasicInfoSql = "select photo,member_name from ty_basic_info where member_id = %s"
        cursor2.execute(tyBasicInfoSql,(str(member_id)))
        tyBasicInfo = cursor2.fetchone()

        #photo可能为空
        #构造数据
        if tyBasicInfo:
            photo = tyBasicInfo[0]
            nickname = tyBasicInfo[1]
        else:
            photo = ""
            nickname = ""

        #vsedu.vs_protect_member_tb phone
        vsProtectMemberSql = "select phone_no,enable from vs_protect_member_tb where member_id = %s"
        cursor2.execute(vsProtectMemberSql,(str(member_id)))
        vsProtectMember = cursor2.fetchone()

        #phone可能为空 phone_bind 0 为 未绑定
        if vsProtectMember:
            phone_no = vsProtectMember[0]
            phone_bind = vsProtectMember[1]
        else:
            phone_no = ""
            phone_bind = 0

        #yxy_basic.yxy_member_tb school_id,判断是否是教师class_id = 0 => 教师 在A学校是老师，B学校是学生他就是学生
        yxyMemberSql = "select school_id,class_id from yxy_member_tb where user_id = %s"
        cursor3.execute(yxyMemberSql,(str(id)))
        yxyMember = cursor3.fetchall()

        #如果学校，班级为空，为学生
        if yxyMember:

            #2个集合运算
            schoolId = set()
            classId = set()
            for member in yxyMember:
                schoolId.add(member[0])
                classId.add(member[1])

            #school_id 取最大id 除去2,57
            schoolId.discard(2)
            schoolId.discard(57)
            # print(schoolId)
            if schoolId:
                school_id = max(schoolId)
            else:
                school_id = 0

            #班级集合，只有不为0的就是学生
            classId.add(0)
            if max(classId) != 0:
                is_teacher = 0
            else:
                is_teacher = 1

        else:
            school_id = 0
            is_teacher = 0

        #判断是老师还是学生(学生subject = 0,教师plate_id = 0)
        if is_teacher :
            #yxy_basic.t_teacher_config subjects is null subjects = subject_id(学生subjects = 0)
            teacherConfigSql = "select subject_id,subjects from t_teacher_config where user_id = %s"
            cursor3.execute(teacherConfigSql,(str(id)))
            teacherConfig = cursor3.fetchone()

            if teacherConfig:
                #如果subjects存在取subjects，否则取subject_id
                if teacherConfig[1]:
                    subjects = teacherConfig[1]
                else:
                    subjects = teacherConfig[0]
            else:
                subjects = ""

            plate_id = 0

        else:
            #yxy_education.t_user_config_grade plate_id(教师plate_id = 0)
            userConfigSql = "select plate_id from t_user_config_grade where user_id = %s"
            cursor4.execute(userConfigSql,(str(id)))
            userConfig = cursor4.fetchone()

            if userConfig:
                plate_id = userConfig[0]
            else:
                plate_id = 0

            subjects = 0

        #插入数据库
        userSql = "insert into tifen_user values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #print((str(id),str(school_id),str(member_id),str(nickname),str(realname),str(member_pwd),str(sex),str(is_teacher),str(subjects),str(plate_id),str(photo),str(phone_no),str(phone_bind),str(disabled),str(source_id),str(create_time)))
        cursor1.execute(userSql,(str(id),str(school_id),str(member_id),str(nickname),str(realname),str(member_pwd),str(sex),str(credits_total),str(credits_balance),str(is_teacher),str(subjects),str(plate_id),str('0'),str(photo),str(phone_no),str(phone_bind),str(disabled),str(source_id),str(create_time)))
        conn1.commit()
    except:
        continue

#关闭数据库
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor4.close()
conn4.close()
cursor.close()
conn.close()






