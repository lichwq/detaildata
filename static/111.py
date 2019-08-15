# coding=utf-8
#测试utf-8编码
from Queue import Queue
from threading import Thread
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

            if type(value).__name__ == 'unicode':

                value = '\'' + value + '\''

            if isinstance(value, list):

                if type(value[0]).__name__ != 'int':

                    value = '\''+"','".join(map(str, value))+'\''
                else:

                    value = ','.join(map(str, value))

            querystring = querystring.replace(key,str(value))

        # print querystring

        return querystring

    def get_query_condition(self,condition):

        return condition['query_condition']

    def get_query_columns(self,condition):

        if self.get_query_type(condition) == "fixed":

            return eval(self.newmysql.get_one(GETQUERYSQL % (condition["query_id"]))[4])

        elif self.get_query_type(condition) == "customer":

            for key, value in condition['query_columns'].items():

                if value[0] == '-99':

                    continue

            return condition['query_columns']

        else:

            raise str("请求type参数错误!")

    def get_query_type(self,condition):

        return self.newmysql.get_one(GETQUERYSQL % (condition["query_id"]))[1]

    def get_presto_data(self,condition):

        try:

            print datetime.datetime.now()

            engine = create_engine(
                'presto://%s:%s/%s' % (PT_IP, PT_PORT, PT_CAT))

            df = pd.read_sql(self.get_query_full_text(condition), engine)

            print datetime.datetime.now()

            df.columns = self.get_query_columns(condition)

            # print datetime.datetime.now()

            self.returnresult_list['status'] = 'success'

            self.returnresult_list['preview_data'] = json.loads(df.to_json(orient='split', index=False))

            # print datetime.datetime.now()

            return json.dumps(self.returnresult_list)

        except Exception as e:

            self.returnresult_list['status'] = 'failed'

            self.returnresult_list['error_message'] = str(e)

            return json.dumps(self.returnresult_list)

