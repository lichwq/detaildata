# coding=utf-8
#测试utf-8编码
from Queue import Queue
from threading import Thread
from single import *
import sys
from MysqlCommon import *
from Conf import *
import time
import datetime
from sqlalchemy.engine import create_engine
import pandas as pd
import json

reload(sys)
sys.setdefaultencoding('utf-8')

condition1 = {"query_id":"1",
        "query_condition":{"$start_date":'2019-04-01',
                "$end_date":'2019-10-01',"$num":[1,2,3,4,5,6,7,8,9]}
                }

class GetPrestoData():

    def __init__(self):

        self.returnresult_list = {"status": "",
                "error_message": "",
                "preview_data": ""
                }

        self.newmysql = MysqlCommon(DB_IP, DB_PORT, DB_SCHEAMA, DB_USER, DB_PASSWD)

    def get_query_text(self,condition):

        return self.newmysql.get_one(GETQUERYSQL % (condition["query_id"]))[3]

    def get_query_full_text(self,condition):

        querystring = self.get_query_text(condition)

        items = self.get_query_condition(condition).items()

        for key, value in items:

            if type(value).__name__ == 'str':

                value = '\'' + value + '\''

            if isinstance(value, list):

                if type(value[0]).__name__ != 'int':

                    value = '\''+"','".join(map(str, value))+'\''
                else:

                    value = ','.join(map(str, value))

            querystring = querystring.replace(key,str(value))

        return querystring

    def get_query_condition(self,condition):

        return condition['query_condition']

    def get_query_columns(self,condition):

        if self.get_query_type(condition) == "fixed":

            return eval(self.newmysql.get_one(GETQUERYSQL % (condition["query_id"]))[4])

        elif self.get_query_type(condition) == "customer":

            return condition['query_columns']

        else:

            raise str("请求type参数错误!")

    def get_query_type(self,condition):

        return self.newmysql.get_one(GETQUERYSQL % (condition["query_id"]))[1]

    def get_presto_data(self,condition):

        try:

            engine = create_engine(
                'presto://%s:%s/%s' % (PT_IP, PT_PORT, PT_CAT))

            # print self.get_query_full_text(condition)

            df = pd.read_sql(self.get_query_full_text(condition), engine)

            df.columns = self.get_query_columns(condition)

            self.returnresult_list['status'] = 'success'

            self.returnresult_list['preview_data'] = json.loads(df.to_json(orient='split', index=False))

            return json.dumps(self.returnresult_list)



        except Exception as e:

            self.returnresult_list['status'] = 'failed'

            self.returnresult_list['error_message'] = str(e)

            return json.dumps(self.returnresult_list)

# if __name__ == '__main__':
#
#     new_presto = GetPrestoData()