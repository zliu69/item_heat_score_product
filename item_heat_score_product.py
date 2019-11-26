# %%
# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions
import redis
# import json
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == '__main__':
    conf = SparkConf().setAll([ \
        # ('spark.master', 'yarn'), ('spark.app.name', 'liuzimo'), \
        # ('spark.driver.cores', '4'), ('spark.driver.memory', '10g'), ('spark.executor.memory', '10g'), \
        # ('spark.executor.cores', '4'), ('spark.executor.instances', '200'),
        # ('spark.yarn.executor.memoryOverhead', '4096'), \
        # ('spark.sql.shuffle.partitions', '200'), ('spark.rpc.message.maxSize', '2046'),
        # ('spark.network.timeout', '1200s'), \
        # ('spark.default.parallelism', '2000'), ('spark.driver.maxResultSize', '6g'), ('spark.shuffle.manager', 'SORT'), \
        ("hive.metastore.uris", "thrift://10.23.240.71:9083,thrift://10.23.240.72:9083,thrift://10.23.240.58:9083,""thrift://10.23.240.59:9083"), \
        ("datanucleus.schema.autoCreateAll", "false"), \
        ("javax.jdo.option.ConnectionDriverName", "com.mysql.jdbc.Driver"), \
        ("javax.jdo.option.ConnectionUserName", "hive"), \
        ("hive.server2.thrift.port", "10001"), \
        ("hive.metastore.execute.setugi", "true"), \
        ("hive.metastore.warehouse.dir", "hdfs://ns1/user/hive/warehouse"), \
        ("hive.security.authorization.enabled", "false"), \
        ("hive.metastore.client.socket.timeout", "60s"), \
        ("hive.metastore.sasl.enabled", "true"), \
        ("hive.server2.authentication", "KERBEROS"), \
        ("hive.server2.authentication.kerberos.principal", "hive/hive-metastore-240-58.hadoop.lq@HADOOP.LQ2"), \
        ("hive.server2.authentication.kerberos.keytab", "/data/sysdir/hadoop/etc/hadoop/hive.keytab"), \
        ("hive.server2.enable.impersonation", "true"), \
        ("hive.metastore.kerberos.keytab.file", "/data/sysdir/hadoop/etc/hadoop/hive.keytab"), \
        ("hive.metastore.kerberos.principal", "hive/hive-metastore-240-58.hadoop.lq@HADOOP.LQ2"), \
 \
        ])

    spark = SparkSession.builder.master('yarn').config(conf=conf).enableHiveSupport().enableHiveSupport().getOrCreate()

    # %% md

    # 1 取前七天数据并筛选特征cmp_bdm.cmp_bdm_rcmd_user_item_di

    # %%

    # sql_user_item = ("""select * from cmp_bdm.cmp_bdm_rcmd_user_item_di where datediff(date_sub(current_date,1),dt)<7""")
    sql_user_item = ("""select event_id,object_type,object_id,get_json_object(basicid, '$.rway'),dt from cmp_bdm.cmp_bdm_rcmd_user_item_di where datediff(date_sub(current_date,1),dt)<7""")
    df_user_item = spark.sql(sql_user_item)
    df_user_item = df_user_item.withColumnRenamed('get_json_object(basicid, $.rway)', 'rway')
    df_user_item = df_user_item.repartition(200)
    #
    df_user_item = df_user_item.select('event_id', 'object_type', 'object_id', 'rway').where(\
        df_user_item['event_id'] != "article_newest_list_show")

    # 去除滑动曝光
    df_user_item = df_user_item.select('*')
    df_user_item = df_user_item.withColumnRenamed('event_id', 'label')
    df_user_item = df_user_item.replace(['article_list_item_click', 'article_newest_list_sight_show'], \
                                        ['1', '0'], 'label')
    # # df_user_item = df_user_item.fillna('0')
    df_user_item = df_user_item.withColumn('item_id',\
                                           functions.concat(functions.lit('lzmp-'), functions.col('object_type'),\
                                                            functions.lit('-'), functions.col('object_id'))).select(\
        'item_id', 'label', 'rway')
    # #
    # # # %%
    # #
    df_user_item = df_user_item.fillna('0')

    #     #
    #     df_user_item_temp_count = df_user_item.groupby('item_id').count()
    #     df_user_item_temp_cli = df_user_item.where(df_user_item.label == '1').groupby('item_id').count()
    #     df_user_item_temp_cli = df_user_item_temp_cli.withColumnRenamed('count', 'click_num')
    #     df_user_item_ctr = df_user_item_temp_count.join(df_user_item_temp_cli, ['item_id'], 'left')
    #     # 插值
    #     df_user_item_ctr = df_user_item_ctr.fillna(0)
    #     df_user_item_ctr = df_user_item_ctr.withColumn('ctr', (df_user_item_ctr['click_num'] + 100) / (\
    #                 df_user_item_ctr['count'] + 1000))

    # #     #计算age 各个特征
    #     df_user_item_temp_age_count = df_user_item.where(df_user_item['rway'] == 'RecallItemAgeEE').groupby(\
    #         'item_id').count()
    #
    #     df_user_item_temp_age_cli = df_user_item.where( \
    #         (df_user_item['rway'] == 'RecallItemAgeEE') & (df_user_item['label'] == '1')).groupby(\
    #         'item_id').count()
    #     # df_user_item_temp_age_cli = df_user_item_temp_age_cli.sort('count', ascending=False)
    #     df_user_item_temp_age_cli = df_user_item_temp_age_cli.withColumnRenamed('count', 'click_num')
    #     df_user_item_age_ctr = df_user_item_temp_age_count.join(df_user_item_temp_age_cli, ['item_id'], 'left').fillna(0)
    #     df_user_item_age_ctr = df_user_item_age_ctr.withColumn('ctr', (df_user_item_age_ctr['click_num'] + 100) / (\
    #                 df_user_item_age_ctr['count'] + 1000))
    # #
    # #     # 重命名
    #     # mapping.get (key,default)
    #     mapping = dict(zip(['count', 'click_num', 'ctr'], ['age_count', 'age_click_num', 'age_ctr']))
    #     df_user_item_age_ctr = df_user_item_age_ctr.select(\
    #         [functions.col(c).alias(mapping.get(c, c)) for c in df_user_item_age_ctr.columns])

    df_user_item_temp_count = df_user_item.groupby('item_id').count()
    df_user_item_temp_cli = df_user_item.where(df_user_item.label == '1').groupby('item_id').count()
    df_user_item_temp_cli = df_user_item_temp_cli.withColumnRenamed('count', 'click_num')
    df_user_item_temp_count = df_user_item_temp_count.join(df_user_item_temp_cli, ['item_id'], 'left')
    # 插值
    df_user_item_temp_count = df_user_item_temp_count.fillna(0)
    df_user_item_temp_count = df_user_item_temp_count.withColumn('ctr', (df_user_item_temp_count['click_num'] + 100) / (\
                df_user_item_temp_count['count'] + 1000))
    #
    # 计算age 各个特征
    df_user_item_temp_age_count = df_user_item.where(df_user_item['rway'] == 'RecallItemAgeEE').groupby( \
        'item_id').count()

    df_user_item_temp_age_cli = df_user_item.where( \
        (df_user_item['rway'] == 'RecallItemAgeEE') & (df_user_item['label'] == '1')).groupby( \
        'item_id').count()
    # df_user_item_temp_age_cli = df_user_item_temp_age_cli.sort('count', ascending=False)
    df_user_item_temp_age_cli = df_user_item_temp_age_cli.withColumnRenamed('count', 'click_num')
    df_user_item_temp_age_count = df_user_item_temp_age_count.join(df_user_item_temp_age_cli, ['item_id'],\
                                                                   'left').fillna(0)
    df_user_item_temp_age_count = df_user_item_temp_age_count.withColumn('ctr', ( \
                df_user_item_temp_age_count['click_num'] + 100) / (df_user_item_temp_age_count['count'] + 1000))
    #
    #     # 重命名
    # mapping.get (key,default)
    mapping = dict(zip(['count', 'click_num', 'ctr'], ['age_count', 'age_click_num', 'age_ctr']))
    df_user_item_temp_age_count = df_user_item_temp_age_count.select( \
        [functions.col(c).alias(mapping.get(c, c)) for c in df_user_item_temp_age_count.columns])
    #
    #

    #
    #
    # # 生成最终df
    df_user_item_temp_count = df_user_item_temp_count.join(df_user_item_temp_age_count, ['item_id'], 'left')
    df_user_item_temp_count = df_user_item_temp_count.fillna(0)
    # df_user_item_final = df_user_item_final.repartition(200)
    df_user_item_temp_count = df_user_item_temp_count.withColumn('product', \
                                                                 (df_user_item_temp_count['ctr'] *\
                                                                  df_user_item_temp_count['age_ctr'])).withColumn('features', \
                                                                 functions.concat(functions.col('count'), \
                                                                                  functions.lit(','), \
                                                                                  functions.col('click_num'), \
                                                                                  functions.lit(','), \
                                                                                  functions.col('age_count'), \
                                                                                  functions.lit(','), \
                                                                                  functions.col('age_click_num'), \
                                                                                  functions.lit(','), \
                                                                                  functions.col('product'))).select(\
        'item_id', 'features')
    #
    #
    #
    #
    # #4  存入hive redis

    pool = redis.ConnectionPool(host="codis.rcmsys2.lq.autohome.com.cn", port="19099")
    conn = redis.Redis(connection_pool=pool, decode_responses=True)
    dict_heat_score = df_user_item_temp_count.rdd.collectAsMap()

    cnt = 0
    total_num = len(dict_heat_score)
    batch_size = total_num // 2000

    with conn.pipeline(transaction=False) as p:
        for key in dict_heat_score:
            p.set(key, dict_heat_score[key], 90000)  # 6000代表6000秒，可以自己设置
            cnt = cnt + 1
            if cnt == total_num or cnt % batch_size == 0:
                p.execute()
    # #
    # # # %%
    # #
    # #
    # #
    # # # %%
    # #
    # pool = redis.ConnectionPool(host="codis.rcmsys2.lq.autohome.com.cn", port="19099")
    # conn = redis.Redis(connection_pool=pool, decode_responses=True)
    # dict_heat_score = df_user_item_temp_count.rdd.collectAsMap()
    # # #
    # # # # %%
    # # #
    #
    # # #
    # # # # %%
    # # #
    #
    # # #
    # # # # %%
    # # #
    # with conn.pipeline(transaction=False) as p:
    #     for key in dict_heat_score:
    #         p.set(key, dict_heat_score[key], 90000)  # 6000代表6000秒，可以自己设置
    #     p.execute()
    #




    spark.stop()