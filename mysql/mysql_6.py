#!usr/bin/python

import pymysql
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3003, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="exameveryday",charset="utf8")

cursor = conn.cursor()

sql = """
select qk.id,qk.knowledge_id,qk.question_id,qk.weight,knw.plate,knw.`subject` from t_question_knowledge qk INNER JOIN t_knowledge_new_wwqcopy knw on qk.knowledge_id = knw.id;
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



for data in alldata:
    # print(data[10])

        #tableSql = "SET NAMES utf8mb4;SET FOREIGN_KEY_CHECKS = 0;CREATE TABLE `tifen_textbook_question_%s` (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,`book_id` int(10) UNSIGNED NOT NULL COMMENT '教材id',`chapter_id` int(10) UNSIGNED NOT NULL COMMENT '章节id',`question_id` bigint(20) UNSIGNED NOT NULL COMMENT '试题id',`sort_id` int(11) NOT NULL,`is_review` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否已审核',`creator` int(10) UNSIGNED NOT NULL COMMENT '创建者',`create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),PRIMARY KEY (`id`) USING BTREE,INDEX `book_id`(`book_id`) USING BTREE,INDEX `chapter_id`(`chapter_id`) USING BTREE,INDEX `creator`(`creator`) USING BTREE,INDEX `is_review`(`is_review`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '教材包含的试题，按科目分表' ROW_FORMAT = Compact;SET FOREIGN_KEY_CHECKS = 1;" % data[3]
    tableSql = "CREATE TABLE `tifen_question_%s_knowledge`  (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,`question_id` bigint(20) UNSIGNED NOT NULL COMMENT '试题id',`knowledge_id` int(10) UNSIGNED NOT NULL COMMENT '知识点id',`weight` int(11) NOT NULL DEFAULT 1 COMMENT '权重占比',PRIMARY KEY (`id`) USING BTREE,INDEX `question_id`(`question_id`) USING BTREE,INDEX `knowledge_id`(`knowledge_id`) USING BTREE,INDEX `weight`(`weight`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '试题知识点' ROW_FORMAT = Compact;" % data[5]
        # print(tableSql)
        
        #print(tableSql)

    sql = "insert into tifen_question_%s_knowledge values(%d,%d,%d,%d)" % (data[5],int(data[0]),int(data[2]),int(data[1]),int(data[3]))
    print(sql)
        # print(sql)
   
    if str(data[4]) == '1':

        tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_primary' and t.TABLE_NAME ='tifen_question_%s_knowledge'" % data[5]
        exist = cursor1.execute(tableExistSql)
            # print(exist)
        if int(exist) != 1:
            cursor1.execute(tableSql)   
            conn1.commit()

        cursor1.execute(sql)
        conn1.commit()
        
    if str(data[4]) == '20':  
        tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_junior' and t.TABLE_NAME ='tifen_question_%s_knowledge'" % data[5]
        exist = cursor2.execute(tableExistSql)
            # print(type(exist))
        if int(exist) != 1:
            cursor2.execute(tableSql)   
            conn2.commit()
      
        cursor2.execute(sql)
        
        conn2.commit()
        
    if str(data[4]) == '33':
        tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_senior' and t.TABLE_NAME ='tifen_question_%s_knowledge'" % data[5]
        exist = cursor3.execute(tableExistSql)
            # print(exist)
        if int(exist) != 1:
            cursor3.execute(tableSql)   
            conn3.commit()
       
        cursor3.execute(sql)
        conn3.commit()
                   
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()