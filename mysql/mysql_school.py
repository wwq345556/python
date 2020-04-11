import pymysql

#连接yxy_basic
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_basic",charset="utf8")
cursor = conn.cursor()

#连接tifen_mall
conn1 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tifen_mall",charset="utf8")
cursor1 = conn1.cursor()

#连接tongyideu
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tongyiedu",charset="utf8")
cursor2 = conn2.cursor()

#连接vsdeu
conn3 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="vsedu",charset="utf8")
cursor3 = conn3.cursor()

#查询所有学校
schoolTotalSql = "select id,school_name,city_code,school_address,lat,lng,stage_tag,create_user,create_time from yxy_school_tb where id not in (2,57) and del = 1"
cursor.execute(schoolTotalSql)
alldata = cursor.fetchall()

# alldatas = {'0'}
#
# for x in alldata:
#     alldatas.add(x)
# print(alldatas)
# exit()
#循环
for data in alldata:
    # print(data)
    #学校数据收集
    #school_id
    school_id = data[0]

    #type_id=>去甜甜表查 有=>1 无=>2 甜甜user_id = manager_id
    #name citycode address lat lng
    tianTianSql = "select user_id,name,citycode,address,lat,lng,balance,points,remark,FROM_UNIXTIME(create_time) from tyd_trialcenter where yxy_school_id = %s"
    cursor1.execute(tianTianSql,str(school_id))
    tianTian = cursor1.fetchone()
    if tianTian:
        type_id = 1
        # manager_id = tianTian[0]
        name = tianTian[1]
        citycode = tianTian[2]
        address = tianTian[3]
        lat = tianTian[4]
        lng = tianTian[5]
        balance = tianTian[6]
        points = tianTian[7]
        remark = tianTian[8]
        create_time = tianTian[9]
        #manager_id
        tianTianUserSql = "select ty_user_id from tyd_user where id = %s"
        cursor1.execute(tianTianUserSql,str(tianTian[0]))
        tianTianUser = cursor1.fetchone()
        if tianTianUser:
            manager_id = tianTianUser[0]
        else:
            manager_id = 0
    else:
        type_id = 2
        name = data[1]
        citycode = data[2]
        address = data[3]
        lat = data[4]
        lng = data[5]
        balance = 0.00
        points = 0
        remark = ""
        create_time = data[8]
        manager_id = 0

    #plates
    if data[6]:
        plates = data[6].split(",")
        plate_set = list()
        if 'elementary' in plates:
            plate_set.append(str(1))
        if 'junior' in plates:
            plate_set.append(str(20))
        if 'senior' in plates:
            plate_set.append(str(33))
        plates_id = ','.join(plate_set)
    else:
        plates_id = 0

    # modules is_del
    modules = ""
    is_del = 0
    teacher_limit = 0
    student_limit = 0
    duoke_limit = 0

    #creator
    if data[7]:
        creator = data[7]
    else:
        creator = ""

    # #学校存入数据库
    schoolSql = "insert into tifen_school values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor2.execute(schoolSql,(str(school_id),str(manager_id),str(type_id),str(name),str(citycode),str(address),str(lat),str(lng),str(plates_id),str(balance),str(points),str(modules),str(remark),str(creator),str(create_time),str(is_del),str(teacher_limit),str(student_limit),str(duoke_limit)))
    conn2.commit()

    #按学校查找班级  未加 作业群
    classSql = "select id,categories_id,classname,UNIX_TIMESTAMP(create_time) from yxy_school_class_tb where school_id = %s and categories_id <> 0"
    cursor.execute(classSql,str(school_id))
    classAllData = cursor.fetchall()

    for classData in classAllData:
        #class_id
        class_id = classData[0]
        #老师manager_id
        teacherManagerSql = "select type from yxy_school_class_teacher where school_id = %s and class_id = %s order by id desc limit 1"
        cursor.execute(teacherManagerSql,(str(school_id),str(class_id)))
        teacherManager = cursor.fetchone()
        if teacherManager:
            if int(teacherManager[0]) == 3:
                manager_id = 1
            else:
                manager_id = 0
        else:
            manager_id = 0

        #plate_id
        categoriesSql = "select name,pid from yxy_categories_tb where id = %s"
        cursor.execute(categoriesSql,str(classData[1]))
        categories = cursor.fetchone()

        #name 班级无名不算脏数据
        if classData[2]:
            name = categories[0] + classData[2]
        else:
            name = categories[0]
        plate_id = categories[1]

        #create_time
        create_time = classData[3]
        #剩下的字段
        graduation_year = 0
        is_ban = 0
        is_del = 0
        last_msg = ""
        last_msg_time = 0
        join_able = 0
        print(class_id,school_id,manager_id,name,plate_id,create_time,graduation_year)

        #班级存入数据库
        classSql = "insert into tifen_school_class values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor2.execute(classSql,(str(class_id),str(manager_id),str(school_id),str(plate_id),str(graduation_year),str(name),str(is_ban),str(is_del),str(create_time),str(last_msg),str(last_msg_time),str(join_able)))
        conn2.commit()

        #tifen_school_classmate
        #先导入同学
