# mongo_imp
data migration tool(From mysql to mongo)

1,设计目标：

--替代MONGO官方的mongoimport，因其做数据导入时，会将VARCHAR类型的‘343435’或‘92323.333’导入至MONGO时，变成Int32或float类型

--非常适合大数据量单表导入（需要PK设计为自增INT，实在想不通用UUID作PK的人的想法...）

--（FUTURE TARGET）mongo/mysql配置、并行数、表名命令行参数作为命令行参数传入


2，设计要点
  进程级并行处理，按PK(自增INT64)和并发数对PK做范围分区（想法来自使用SQOOP向HDFS导入MYSQL数据）
  

3,启动说明 python mongo_imp.py {TABLE_NAME} {PARRELLEL_CNT}


4,配置说明（db.cfg）
[MYSQL_SOURCE]
mysql_host=192.168.XXX.160
mysql_port=XXXX
user=root
password=XXX
db=XXDB

[MONGO_TARGET]
mongo_url=mongodb://XXX:XXZXX@192.XXX.155.XXXX:25001/
