#!usr/bin/python

import pymysql
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")

cursor = conn.cursor()
#连接知识点库
conn4 = pymysql.connect(host="192.168.1.176",port=3003, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="exameveryday",charset="utf8")

cursor4 = conn4.cursor()
# countSql = "select * from t_chapter_qlist"

# count = cursor.execute(countSql)
# alldata = cursor.fetchall()
# print(count)
#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
    #连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
    #连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()


#sql = "select c.id,c.question_id,(select id from t_edition_config where grade_id = cn.grade_id and subject_id = cn.subject_id and edition_id = cn.edition_id) as book_id,cn.subject_id,SUBSTRING((select full_id from yxy_categories_tb WHERE id = cn.grade_id),3,1) as grade_id from (select a.id,substring_index(substring_index(a.qid_list,',',b.help_topic_id+1),',',-1) question_id from (select cq.id,cq.qid_list from t_chapter_qlist cq limit 0,1) a join mysql.help_topic b on b.help_topic_id < (length(a.qid_list) - length(replace(a.qid_list,',',''))+1) order by id) c inner join t_chapter_new cn on c.id = cn.id"

# for num in range(count):
sql = "select c.id,c.question_id,(select id from t_edition_config where grade_id = cn.grade_id and subject_id = cn.subject_id and edition_id = cn.edition_id) as book_id,cn.subject_id,SUBSTRING((select full_id from yxy_categories_tb WHERE id = cn.grade_id),3,1) as grade_id from (select a.id,substring_index(substring_index(a.qid_list,',',b.help_topic_id+1),',',-1) question_id from (select cq.id,cq.qid_list from t_chapter_qlist cq) a join mysql.help_topic b on b.help_topic_id < (length(a.qid_list) - length(replace(a.qid_list,',',''))+1) order by id) c inner join t_chapter_new cn on c.id = cn.id where cn.is_del = 0" 
cursor.execute(sql)
    
alldata = cursor.fetchall()

# print(alldata)

for data in alldata:
    # print(data[10])
    #知识点
    try:
        knowledgeSql = "Select knowledge_id from t_question_%s where id = %s" % (int(data[3]),int(data[1]))
        # print(knowledgeSql)
        cursor4.execute(knowledgeSql)
        knowledgeid = cursor4.fetchone()
        # print(knowledgeid)
        if knowledgeid is None :
            knowledge_id = 0
        else :
            knowledge_id = knowledgeid[0]    
        #tableSql = "SET NAMES utf8mb4;SET FOREIGN_KEY_CHECKS = 0;CREATE TABLE `tifen_textbook_question_%s` (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,`book_id` int(10) UNSIGNED NOT NULL COMMENT '教材id',`chapter_id` int(10) UNSIGNED NOT NULL COMMENT '章节id',`question_id` bigint(20) UNSIGNED NOT NULL COMMENT '试题id',`sort_id` int(11) NOT NULL,`is_review` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否已审核',`creator` int(10) UNSIGNED NOT NULL COMMENT '创建者',`create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),PRIMARY KEY (`id`) USING BTREE,INDEX `book_id`(`book_id`) USING BTREE,INDEX `chapter_id`(`chapter_id`) USING BTREE,INDEX `creator`(`creator`) USING BTREE,INDEX `is_review`(`is_review`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '教材包含的试题，按科目分表' ROW_FORMAT = Compact;SET FOREIGN_KEY_CHECKS = 1;" % data[3]
    #tableSql = "CREATE TABLE `tifen_textbook_question_%s` (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,`book_id` int(10) UNSIGNED NOT NULL COMMENT '教材id',`chapter_id` int(10) UNSIGNED NOT NULL COMMENT '章节id',`question_id` bigint(20) UNSIGNED NOT NULL COMMENT '试题id',`sort_id` int(11) NOT NULL,`is_review` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否已审核',`creator` int(10) UNSIGNED NOT NULL COMMENT '创建者',`create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),PRIMARY KEY (`id`) USING BTREE,INDEX `book_id`(`book_id`) USING BTREE,INDEX `chapter_id`(`chapter_id`) USING BTREE,INDEX `creator`(`creator`) USING BTREE,INDEX `is_review`(`is_review`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '教材包含的试题，按科目分表' ROW_FORMAT = Compact;" % data[3]
        tableSql = "CREATE TABLE `tifen_textbook_question_%s`  (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT,`book_id` int(10) UNSIGNED NOT NULL COMMENT '教材id',`chapter_id` int(10) UNSIGNED NOT NULL COMMENT '章节id',`knowledge_id` int(10) UNSIGNED NOT NULL,`question_id` bigint(20) UNSIGNED NOT NULL COMMENT '试题id',`sort_id` int(11) NOT NULL,`is_review` tinyint(1) NOT NULL DEFAULT 0 COMMENT '是否已审核',`creator` int(10) UNSIGNED NOT NULL COMMENT '创建者',`create_time` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),PRIMARY KEY (`id`) USING BTREE,INDEX `book_id`(`book_id`) USING BTREE,INDEX `chapter_id`(`chapter_id`) USING BTREE,INDEX `creator`(`creator`) USING BTREE,INDEX `is_review`(`is_review`) USING BTREE,INDEX `knowledge_id`(`knowledge_id`) USING BTREE) ENGINE = InnoDB AUTO_INCREMENT = 1592 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '教材包含的试题，按科目分表' ROW_FORMAT = Compact;" % data[3]
        # print(tableSql)
        
        #print(tableSql)
    
        sql = "insert into tifen_textbook_question_%d values(null,%d,%d,%d,%d,0,0,0,0)" % (int(data[3]),int(data[2]),int(data[0]),int(knowledge_id),int(data[1]))
        print(sql)
        # print(sql)
   
        if data[4] == '1':
            tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_primary' and t.TABLE_NAME ='tifen_textbook_question_%s'" % data[3]
            exist = cursor1.execute(tableExistSql)
            # print(exist)
            if int(exist) != 1:
                cursor1.execute(tableSql)   
                conn1.commit()

            cursor1.execute(sql)
            conn1.commit()
        
        if data[4] == '2':  
            tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_junior' and t.TABLE_NAME ='tifen_textbook_question_%s'" % data[3]
            exist = cursor2.execute(tableExistSql)
            # print(type(exist))
            if int(exist) != 1:
                cursor2.execute(tableSql)   
                conn2.commit()
      
            cursor2.execute(sql)
        
            conn2.commit()
        
        if data[4] == '3':
            tableExistSql = "select * from information_schema.TABLES t where t.TABLE_SCHEMA ='tyeducation_senior' and t.TABLE_NAME ='tifen_textbook_question_%s'" % data[3]
            exist = cursor3.execute(tableExistSql)
            # print(exist)
            if int(exist) != 1:
                cursor3.execute(tableSql)   
                conn3.commit()
       
            cursor3.execute(sql)
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