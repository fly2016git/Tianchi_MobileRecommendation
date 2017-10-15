import sys
import os
import pandas as pd
import matplotlib.pyplot as plt

class DeepAnalysis:
    '''
    基于深入的数据分析结果，进行简单的推荐
    '''

    #生成一个新的数据集，包含购买和加入购物车两种行为的数据
    def gen_act_34(self, userPath):
        batch = 0
        for df in pd.read_csv(open(userPath, 'r'), chunksize=100000):
            try:
                df_act_34 = df[df['behavior_type'].isin([3, 4])]
                df_act_34.to_csv('../data/act_34.csv', columns=['time', 'user_id', 'item_id', 'behavior_type'], index=False,header=False,mode='a')
                batch += 1
                print('chunk %d done!' % batch)
            except StopIteration:
                print('finish.')
                break

    #生成一个新的数据集，包含购买和加入购物车两种行为的数据
    def gen_act_14(self, userPath):
        batch = 0
        for df in pd.read_csv(open(userPath, 'r'), chunksize=100000):
            try:
                df_act_14 = df[df['behavior_type'].isin([1, 4])]
                df_act_14.to_csv('../data/act_14.csv', columns=['time', 'user_id', 'item_id', 'behavior_type'], index=False,header=False,mode='a')
                batch += 1
                print('chunk %d done!' % batch)
            except StopIteration:
                print('finish.')
                break

    #生成一个新的数据集，包含购买和加入购物车两种行为的数据
    def gen_act_24(self, userPath):
        batch = 0
        for df in pd.read_csv(open(userPath, 'r'), chunksize=100000):
            try:
                df_act_24 = df[df['behavior_type'].isin([2, 4])]
                df_act_24.to_csv('../data/act_24.csv', columns=['time', 'user_id', 'item_id', 'behavior_type'], index=False,header=False,mode='a')
                batch += 1
                print('chunk %d done!' % batch)
            except StopIteration:
                print('finish.')
                break

    def time_34(self):
        '''
        生成一个新的数据集，其格式为：df_time_34 = {<user_id, item_id, time_3, time_4>}
        其中，time_3, time_4分别是初次加入购物车和购买商品的时间
        :return:
        '''
        with open("../data/act_34.csv", "r") as data_file:
            dateparse = lambda dates: pd.datetime.strptime(dates, "%Y-%m-%d %H")
            df_act_34 = pd.read_csv(data_file, parse_dates=[0], date_parser=dateparse, index_col=False)

            df_act_34.columns = ['time', 'user_id', 'item_id', 'behavior_type']
            df_act_34 = df_act_34.drop_duplicates(['user_id', 'item_id', 'behavior_type'])

        #分离加入购物车行为的时间
        df_time_3 = df_act_34[df_act_34['behavior_type'].isin(['3'])][['user_id', 'item_id', 'time']]
        #分离购买行为的时间
        df_time_4 = df_act_34[df_act_34['behavior_type'].isin(['4'])][['user_id', 'item_id', 'time']]

        df_time_3.columns = ['user_id', 'item_id', 'time3']
        df_time_4.columns = ['user_id', 'item_id', 'time4']

        del df_act_34    #节约内存

        df_time = pd.merge(df_time_3, df_time_4, on=['user_id', 'item_id'], how='outer')
        df_time_34 = df_time.dropna()


        #保存df_time_3，用于预测
        df_time_3 = df_time[df_time['time4'].isnull()].drop(['time4'], axis = 1)
        df_time_3 = df_time_3.dropna()
        df_time_3.to_csv('../data/time_3.csv', columns=['user_id', 'item_id', 'time3'], index=False)

        df_time_34.to_csv('../data/time_34.csv', columns=['user_id', 'item_id', 'time3', 'time4'], index=False)

    def time_24(self):
        '''
        生成一个新的数据集，其格式为：df_time_34 = {<user_id, item_id, time_3, time_4>}
        其中，time_3, time_4分别是初次加入购物车和购买商品的时间
        :return:
        '''
        with open("../data/act_24.csv", "r") as data_file:
            dateparse = lambda dates: pd.datetime.strptime(dates, "%Y-%m-%d %H")
            df_act_24 = pd.read_csv(data_file, parse_dates=[0], date_parser=dateparse, index_col=False)

            df_act_24.columns = ['time', 'user_id', 'item_id', 'behavior_type']
            df_act_24 = df_act_24.drop_duplicates(['user_id', 'item_id', 'behavior_type'])

        #分离加入购物车行为的时间
        df_time_2 = df_act_24[df_act_24['behavior_type'].isin(['2'])][['user_id', 'item_id', 'time']]
        #分离购买行为的时间
        df_time_4 = df_act_24[df_act_24['behavior_type'].isin(['4'])][['user_id', 'item_id', 'time']]

        df_time_2.columns = ['user_id', 'item_id', 'time2']
        df_time_4.columns = ['user_id', 'item_id', 'time4']

        del df_act_24    #节约内存

        df_time = pd.merge(df_time_2, df_time_4, on=['user_id', 'item_id'], how='outer')
        df_time_24 = df_time.dropna()


        #保存df_time_3，用于预测
        df_time_2 = df_time[df_time['time4'].isnull()].drop(['time4'], axis = 1)
        df_time_2 = df_time_2.dropna()
        df_time_2.to_csv('../data/time_2.csv', columns=['user_id', 'item_id', 'time2'], index=False)

        df_time_24.to_csv('../data/time_24.csv', columns=['user_id', 'item_id', 'time2', 'time4'], index=False)

    def time_14(self):
        '''
        生成一个新的数据集，其格式为：df_time_34 = {<user_id, item_id, time_3, time_4>}
        其中，time_3, time_4分别是初次加入购物车和购买商品的时间
        :return:
        '''
        with open("../data/act_14.csv", "r") as data_file:
            dateparse = lambda dates: pd.datetime.strptime(dates, "%Y-%m-%d %H")
            df_act_14 = pd.read_csv(data_file, parse_dates=[0], date_parser=dateparse, index_col=False)

            df_act_14.columns = ['time', 'user_id', 'item_id', 'behavior_type']
            df_act_14 = df_act_14.drop_duplicates(['user_id', 'item_id', 'behavior_type'])

        #分离加入购物车行为的时间
        df_time_1 = df_act_14[df_act_14['behavior_type'].isin(['1'])][['user_id', 'item_id', 'time']]
        #分离购买行为的时间
        df_time_4 = df_act_14[df_act_14['behavior_type'].isin(['4'])][['user_id', 'item_id', 'time']]

        df_time_1.columns = ['user_id', 'item_id', 'time1']
        df_time_4.columns = ['user_id', 'item_id', 'time4']

        del df_act_14    #节约内存

        df_time = pd.merge(df_time_1, df_time_4, on=['user_id', 'item_id'], how='outer')
        df_time_14 = df_time.dropna()


        #保存df_time_3，用于预测
        df_time_1 = df_time[df_time['time4'].isnull()].drop(['time4'], axis = 1)
        df_time_1 = df_time_1.dropna()
        df_time_1.to_csv('../data/time_1.csv', columns=['user_id', 'item_id', 'time1'], index=False)

        df_time_14.to_csv('../data/time_14.csv', columns=['user_id', 'item_id', 'time1', 'time4'], index=False)




    def decay_time_34(self):
        with open('../data/time_34.csv', 'r') as data_file:
            #dateparse = lambda dates: pd.datetime.strptime(dates, "%Y-%m-%d %H")
            df_time_34 = pd.read_csv(data_file, parse_dates=['time3', 'time4'], index_col=False)

        delta_time = df_time_34['time4'] - df_time_34['time3']
        print(delta_time.head(10))
        delta_hour = []
        for i in range(len(delta_time)):
            d_hour = delta_time[i].days*24 + delta_time[i]._h
            if d_hour < 0:
                continue
            else:
                delta_hour.append(d_hour)

        f1 = plt.figure(1)
        plt.hist(delta_hour, 30)
        plt.xlabel('hours')
        plt.ylabel('count')
        plt.title(u'加入购物车行为的购买转化量')
        plt.grid(True)
        plt.show()

    def decay_time_14(self):
        with open('../data/time_14.csv', 'r') as data_file:
            #dateparse = lambda dates: pd.datetime.strptime(dates, "%Y-%m-%d %H")
            df_time_14 = pd.read_csv(data_file, parse_dates=['time1', 'time4'], index_col=False)

        delta_time = df_time_14['time4'] - df_time_14['time1']
        print(delta_time.head(10))
        delta_hour = []
        for i in range(len(delta_time)):
            d_hour = delta_time[i].days*24 + delta_time[i]._h
            if d_hour < 0:
                continue
            else:
                delta_hour.append(d_hour)

        f1 = plt.figure(1)
        plt.hist(delta_hour, 30)
        plt.xlabel('hours')
        plt.ylabel('count')
        plt.title(u'加入购物车行为的购买转化量')
        plt.grid(True)
        plt.show()

    def decay_time_24(self):
        with open('../data/time_24.csv', 'r') as data_file:
            df_time_24 = pd.read_csv(data_file, parse_dates=['time2', 'time4'], index_col=False)

        delta_time = df_time_24['time4'] - df_time_24['time2']
        print(delta_time.head(10))
        delta_hour = []
        for i in range(len(delta_time)):
            d_hour = delta_time[i].days*24 + delta_time[i]._h
            if d_hour < 0:
                continue
            else:
                delta_hour.append(d_hour)

        f1 = plt.figure(1)
        plt.hist(delta_hour, 30)
        plt.xlabel('hours')
        plt.ylabel('count')
        plt.title(u'加入购物车行为的购买转化量')
        plt.grid(True)
        plt.show()


    def predict(self):
        with open('../data/time_3.csv', 'r') as data_file:
            df_time_3 = pd.read_csv(data_file, parse_dates=['time3'], index_col=['time3'])
        ui_pred = df_time_3['2014-12-18']
        with open('/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_item.csv', 'r') as data_file:
            df_item = pd.read_csv(data_file, index_col=False)

        ui_pred_in_P = pd.merge(ui_pred, df_item, on=['item_id'])

        ui_pred_in_P.to_csv('../data/tianchi_mobile_recommendation_predict.csv', columns=['user_id', 'item_id'], index=False)


    # 生成一个新的数据集，包含['time', 'user_id', 'item_id', 'behavior_type']四个字段
    def gen_act_1234(self, userPath):
        batch = 0
        for df in pd.read_csv(open(userPath, 'r'), chunksize=100000):
            try:

                df.to_csv('../data/act_1234.csv', columns=['time', 'user_id', 'item_id', 'behavior_type'],
                                index=False, header=False, mode='a')
                batch += 1
                print('chunk %d done!' % batch)
            except StopIteration:
                print('finish.')
                break

if __name__ == '__main__':
    userPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_user.csv"
    da = DeepAnalysis()
    #da.gen_act_34(userPath)
    #da.gen_act_14(userPath)
    #da.gen_act_24(userPath)
    #da.predict()
    #da.time_14()
    #da.time_24()
    #da.decay_time_14()
    #da.decay_time_24()
    da.decay_time_34()
