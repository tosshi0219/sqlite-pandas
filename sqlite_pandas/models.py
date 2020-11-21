import sqlite3
import pandas as pd
import sys
import os


class sqlite_pandas(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.c = self.conn.cursor()

    #　tableの作成
    def create_table_sql(self, table_name):
        return '''create table if not exists {}'''.format(table_name)

    # tableの列名を作成
    def create_col_sql(self, cols):
        '''
        columns format is dictionary as follows.
        columns = {
            column_name1: 'unique'
            column_name2: None
            column_name3: None
            column_name4: 'unique'
        }
        '''
        col_sql = '(id integer primary key'
        uniques = []
        for k, v in cols.items():
            col_sql = col_sql + ',' + k + ' text'
            if v == 'unique':
                uniques.append(k)
        col_sql = self.unique_check(uniques, col_sql)
        col_sql = col_sql + ');'
        return col_sql

    # unique列の有無チェック
    def unique_check(self, uniques, col_sql):
        if len(uniques) != 0:
            col_sql = col_sql + ',unique('
            for u in uniques:
                col_sql = col_sql + u + ','
            col_sql = col_sql[:-1] + ')'
        return col_sql

    # tableを列名を含めて作成
    def create_table(self, table_name, cols):
        sql = self.create_table_sql(table_name) + self.create_col_sql(cols)
        try:
            self.c.execute(sql)
        except BaseException:
            print('table cannot be created')
        self.conn.commit()

    # tableのを削除
    def drop_table(self, table_name):
        self.c.execute('drop table if exists {}'.format(table_name))
        self.conn.commit()

    # tableデータを取得
    def get_data(self, table_name):
        try:
            return [
                row for row in self.c.execute(
                    'select * from {}'.format(table_name))]
        except BaseException:
            return None

    # tableカラム名を取得
    def get_tblcol(self, table_name):
        self.c.execute(
            "select * from sqlite_master where tbl_name='{}'".format(table_name))
        temp = self.c.fetchall()[0][4]
        if temp.find(',unique(') != -1:
            temp = temp[:temp.find(',unique(')]
        temp = temp[temp.find('(') + 1:]
        temp = temp.split(',')
        columns = [col.split(' ')[0] for col in temp]
        return columns

    # dataframe形式でtableを取得
    def get_dataframe(self, table_name):
        data = self.get_data(table_name)
        if data is not None:
            col = self.get_tblcol(table_name)
            df = pd.DataFrame(data, columns=col)
            return df.drop('id', axis=1)
        else:
            print('empty data')
            return None

    # 行毎にtableにデータを挿入する関数（insert_dataの補助）
    def create_insert_sql(self, row, table_name):
        ins_sql = 'insert into {} ('.format(table_name)
        tblcols = self.get_tblcol(table_name)
        for i in range(len(tblcols) - 1):
            ins_sql = ins_sql + tblcols[i + 1] + ','
        ins_sql = ins_sql[:-1] + ') values('
        for r in row:
            ins_sql = ins_sql + "'" + str(r) + "',"
        ins_sql = ins_sql[:-1] + ')'
        return ins_sql

    # dataframeのデータをtableに追加する関数
    def insert_data(self, table_name, df):
        for index, row in df.iterrows():
            try:
                sql = self.create_insert_sql(row, table_name)
                self.c.execute(sql)
            except BaseException:
                pass
        self.conn.commit()

    # dataframeのデータを削除する関数
    def delete_data(self, table_name, key, query):
        df_temp = self.get_dataframe(table_name)
        print(df_temp[df_temp[key] == query])
        choice = input(
            "above data will be deleted. 'yes' or 'no' [y/N]: ").lower()
        if choice in ['y', 'ye', 'yes']:
            sql = 'delete from {} where {} = "{}";'.format(
                table_name, key, query)
            self.c.execute(sql)
            print('deleted')
        elif choice in ['n', 'no']:
            print('stopped')
            pass

    def get_table_list(self):
        self.c.execute("select * from sqlite_master where type='table'")
        return sorted([table[1] for table in self.c.fetchall()])


if __name__ == '__main__':
    import numpy as np
    cols = {
        'a': 'unique',
        'b': None
    }
    df = pd.DataFrame(np.zeros((5, 2)), columns=cols)
    database_path = '/Users/toshio/project/sqlite_pandas/test.db'
    newdb = sqlite_pandas(database_path)
    newdb.create_table('test', cols)
    newdb.insert_data('test', df)
    newdb.get_dataframe('test')
    newdb.get_table_list()
