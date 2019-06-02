import sys
import time
import logging
import datetime
import os
from configparser import ConfigParser
import pymongo
import pymysql
from pymongo import InsertOne, DeleteOne, ReplaceOne
from concurrent.futures import ProcessPoolExecutor

env_dist = os.environ
module_name='MONGO_IMP'
etcPath = env_dist.get('PY_DIR')
if etcPath is None :
    etcPath = "db.cfg"
else:
    etcPath = env_dist.get('PY_DIR')+"/db.cfg"
cp = ConfigParser()
print(etcPath)
cp.read(etcPath)
idx =  cp.sections().index(module_name)
section = cp.sections()[idx]
logPath = cp.get(section, "log_path")
print(logPath)
#offset=cp.getint(section, "offset")
batch_size=cp.getint(section, "batch_size")
mysql_cfg_dict = {}
table_meta_dict = {} #table meta info
mongo_url = None
logger = None

def get_mongo_cfg():
    cp = ConfigParser()
    cp.read(etcPath)
    idx =  cp.sections().index('MONGO_TARGET')
    section = cp.sections()[idx]
    return cp.get(section, "mongo_url")

def get_mysql_cfg(mysql_cfg_dict):
    cp = ConfigParser()
    cp.read(etcPath)
    idx =  cp.sections().index('MYSQL_SOURCE')
    section = cp.sections()[idx]
    mysql_cfg_dict['host'] = cp.get(section, "mysql_host")
    mysql_cfg_dict['port'] = cp.getint(section, "mysql_port")
    mysql_cfg_dict['user'] = cp.get(section, "user")
    mysql_cfg_dict['password'] = cp.get(section, "password")
    mysql_cfg_dict['db'] = cp.get(section, "db")
#    mysql_cfg_dict['database'] = cp.get(section, "db")
    mysql_cfg_dict['charset'] = 'utf8mb4'
    mysql_cfg_dict['cursorclass'] = pymysql.cursors.DictCursor
    return 0

def get_table_metainfo(table_meta_dict,thread_cnt):
    connection = None
    try:
        connection = pymysql.connect(**mysql_cfg_dict)
        logger.debug('conn[%s]',type(connection))
        with connection.cursor() as cursor:
            #1,PK
            sql = "select COLUMN_NAME from information_schema.STATISTICS where table_schema = \'{0}\' \
AND INDEX_NAME = \'PRIMARY\' AND TABLE_NAME = \'{1}\' order by table_name,seq_in_index".format(table_meta_dict['db'],table_meta_dict['table'])
            logger.debug('SQL=%s',sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            pk_list = []
            for aDict in result:
                pk_list.append(aDict['COLUMN_NAME'])
            table_meta_dict['primary_key'] = pk_list
    
            #2,COLUMN INFO
            sql = "select column_name,data_type,column_type,column_comment from information_schema.COLUMNS \
            where table_schema = \'{0}\' and table_name = \'{1}\' \
                order by ordinal_position".format(table_meta_dict['db'],table_meta_dict['table'])
            logger.debug('SQL=%s',sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            for aDict in result:
                value_dict = dict(zip(['data_type','column_type','column_comment'],
        [aDict['data_type'],aDict['column_type'],aDict['column_comment']]))
                table_meta_dict[aDict['column_name']] = value_dict
            #3,MIN/MAX PK
            sql = "select min({0}) as MIN,max({0}) as MAX \
                from {1}.{2}".format(table_meta_dict['primary_key'][0],table_meta_dict['db'],table_meta_dict['table'])
            logger.debug('SQL=%s',sql)
            cursor.execute(sql)
            result = cursor.fetchall()
            logger.debug('result=%s',result)
            table_meta_dict['min_pk'] = result[0].get('MIN',0)
            table_meta_dict['max_pk'] = result[0].get('MAX',0)
            #4,GET SCOPE
            iOff = int((table_meta_dict['max_pk']-table_meta_dict['min_pk'])/thread_cnt)
            scope = []
            for idx in range(thread_cnt):
	            if idx == thread_cnt-1:
		            tup = (table_meta_dict['min_pk']+iOff*idx,table_meta_dict['max_pk'])
	            else:
		            tup = (table_meta_dict['min_pk']+iOff*idx,table_meta_dict['min_pk']+iOff*(idx+1)-1)
	            scope.append( tup )
            table_meta_dict['scope_pk'] = scope    
    except Exception as exc:
        logger.error('Exception[%s]=%s',type(exc),exc)
        return -1
    finally:    
        logger.debug('table_meta_dict=%s',table_meta_dict)
        if connection is not None:
            connection.close()
    return 0

def mysql_2_mongo(iIdx,table_meta_dict,mysql_cfg_dict,mongo_url):
    setLogger()
    logger.debug('Enter in mysql_2_mongo,offset[%d]=%s',iIdx,table_meta_dict['scope_pk'][iIdx])
    sTable = table_meta_dict['table']
    connection = None
    startTime = time.perf_counter()
    iImported = 0
    try:
        #1,mysql conn/mongo db/tb
        connection = pymysql.connect(**mysql_cfg_dict)
        client = pymongo.MongoClient(mongo_url)
        _mongo_db = client[ mysql_cfg_dict['db'] ]
        _mongo_tb = _mongo_db[ sTable ]
        logger.debug('SCOPE=%s',table_meta_dict['scope_pk'][iIdx])
        
        with connection.cursor() as cursor:
            iStart = 0
            while True:
                tup = table_meta_dict['scope_pk'][iIdx]
                if tup[0]+batch_size*iStart > tup[1]:
                    logger.debug('No more data found...')
                    break
                time_1 = time.perf_counter()    
                sql = "select T.* from {0}.{1} as T where {2} >= {3} \
                    and {2} <= {4} limit {5},{6}".format(mysql_cfg_dict['db'],table_meta_dict['table'],
                    table_meta_dict['primary_key'][0],tup[0],tup[1],batch_size*iStart,batch_size)
                logger.debug('SQL=%s',sql)
                cursor.execute(sql)
                result = cursor.fetchall()
                time_2 = time.perf_counter()
                logger.debug('Fetch from MYSQL rownum=%d,rowcount=%d,time=%f',cursor.rownumber,cursor.rowcount,time_2-time_1)
                iStart +=1
                #类型转化
                for aDict in result:
                    for key, value in aDict.items():
                        tmp_dict = table_meta_dict[key]
                        if 'decimal' == tmp_dict['data_type']:
                            #logger.debug('result key=%s,value=%s',key, value)
                            if value is None:
                                aDict[key] = 0
                            else:
                                aDict[key] = float(value) 
                        elif 'date' == tmp_dict['data_type']:
                            #datetime.datetime.combine(value, datetime.time.min)  %H:%M:%S"    
                            aDict[key] =  value.strftime("%Y-%m-%d"+" 00:00:00")
                    #logger.debug('id=%d,account_date=%s',aDict['TFR_DTL_ACCOUNT_ID'], aDict['ACCOUNTING_DATE'])
                    
                time_3 = time.perf_counter()
                logger.debug('Data transform,time=%f',time_3-time_2)
                #ADD TO MONGO
                queries = [InsertOne(doc) for doc in result]
                time_4 = time.perf_counter()
                if cursor.rownumber != 0 :
                    wr_result = _mongo_tb.bulk_write(queries)
                    logger.debug('InsertCnt=%d MatchCnt=%d',wr_result.inserted_count,wr_result.matched_count)
                    iImported += wr_result.inserted_count
                logger.debug('Data import MONGO,time=%f',time_4-time_3)
                    
    except Exception as exc:
        logger.error('Exception[%s]=%s',type(exc),exc)
        return -1
    finally:
        endTime = time.perf_counter()    
        logger.debug('Imported=%d,consume=%f',iImported,endTime-startTime)
        connection.close()
    return iImported

def setLogger():
    global logger
    run_date = time.strftime('%Y%m%d', time.localtime(time.time()))
    logFile = logPath + "/"+ module_name+'_'+run_date+".log"

    logging.basicConfig(level=logging.DEBUG,
                    filename=logFile,
                    datefmt='%Y-%m-%d %H:%M:%S',
                    format='%(process)d - %(asctime)s - %(filename)s - %(levelname)s - %(lineno)d - %(message)s')
    logger = logging.getLogger('__main__')
    return 0

if __name__ == '__main__':
    print(sys.argv)
    print(len(sys.argv))
    if len(sys.argv) != 3:
        print('Usage:{0} <table_name> <thread_num>'.format(sys.argv[0]) )
        sys.exit(-1)

    sTable  = sys.argv[1]
    iThdCnt = int(sys.argv[2])
    
    setLogger()

    get_mysql_cfg(mysql_cfg_dict)
    logger.debug('mysql_cfg_dict[%s]',mysql_cfg_dict )
    mongo_url = get_mongo_cfg()
    logger.debug('mongo_url[%s]',mongo_url )
    
    
    #Get table meta
    table_meta_dict['table'] = sTable
    table_meta_dict['db'] = mysql_cfg_dict['db']
    get_table_metainfo(table_meta_dict,iThdCnt)

    ex = ProcessPoolExecutor(iThdCnt)
    
    startTime = time.perf_counter()
    logger.debug('[%s] imp begin...',sTable )
    
    iTotal = 0
    objs = []
    for i in range(iThdCnt):
        res = ex.submit(mysql_2_mongo,i,table_meta_dict,mysql_cfg_dict,mongo_url)
        objs.append(res)
    ex.shutdown(wait=True)
    endTime = time.perf_counter()
    logger.debug('ALL IS OK' )
    for obj in objs:
        iTotal += obj.result()

    logger.debug('[%s]:total_imp=%d,consume=%f',sTable,iTotal,endTime-startTime )
    
