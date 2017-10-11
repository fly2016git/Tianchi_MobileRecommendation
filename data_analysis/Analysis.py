import pandas as pd
import timeit
import os
import sys
from data_analysis.dict_convert_csv import *
import matplotlib.pyplot as plt


class Analysis:

    '''
    加载所有数据所需时间
    '''
    def time_cost_of_data_loading(self, path):
        # timeit提供了测量代码执行时间的方法，default_timer是默认计时器
        start_time = timeit.default_timer()

        with open(path, mode="r") as file:
            df = pd.read_csv(file)

        end_time = timeit.default_timer()

        print(df.head(10))
        #__file__：表示当前模块路径
        #os.path.split(__file__)[1]：取当前模块路径中的文件名部分
        print(('The code for file ' + os.path.split(__file__)[1] +
               ' ran for %.2fm' % ((end_time - start_time) / 60.)), file=sys.stderr)

    '''
    计算购买转化率
    '''
    def CTR(self, path):
        count_all = 0 #所有行为
        count_4 = 0 #购买行为
        #分块读取文件
        for df in pd.read_csv(open(path, 'r'), chunksize=100000):
            try:
                count_user = df['behavior_type'].value_counts()
                count_all = count_user[1] + count_user[2] + count_user[3] + count_user[4]
                count_4 = count_user[4]
            except StopIteration:
                print("迭代停止！")
                break

        ctr = count_4/count_all
        return ctr

    '''
    统计每天的操作次数
    '''
    def count_behavior_by_date(self, path):

        count_day = {} #使用字典存储统计结果
        for i in range(31):    #初始化字典
            if i <= 12:
                date = '2014-11-%d' % (i+18)
            else :
                date = '2014-12-%d' % (i-12)
            count_day[date] = 0

        batch = 0
        dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d %H')
        for df in pd.read_csv(open(path, 'r'), parse_dates=['time'], index_col=['time'], date_parser=dateparse, chunksize=100000):
            try:
                for i in range(31):  # 初始化字典
                    if i <= 12:
                        date = '2014-11-%d' % (i + 18)
                    else:
                        date = '2014-12-%d' % (i - 12)
                    #print(df[date].shape[0])
                    count_day[date] += df[date].shape[0]

                batch += 1
                print('chunk %d done.' % batch)
            except StopIteration:
                print('finish data process.')
                break
        #保存count_day到csv文件
        row_dict2csv(count_day, "../data/count_day.csv")
        df_count_day = pd.read_csv(open("../data/count_day.csv", 'r'), header=None, names=['time', 'count'])
        print(count_day)
        print(df_count_day.head(10))

        df_count_day = df_count_day.set_index('time')

        df_count_day.head(10)

        df_count_day['count'].plot(kind='bar')
        plt.legend(loc='best')
        plt.title("每天的操作次数")
        plt.grid(True)
        plt.show()

    '''
        统计属于商品子集P的每天的操作次数
        '''

    def count_behavior_inP_by_date(self, userpath, itempath):

        count_day = {}  # 使用字典存储统计结果
        for i in range(31):  # 初始化字典
            if i <= 12:
                date = '2014-11-%d' % (i + 18)
            else:
                date = '2014-12-%d' % (i - 12)
            count_day[date] = 0

        batch = 0
        dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d %H')

        df_p = pd.read_csv(open(itempath, 'r'), index_col=False)

        for df in pd.read_csv(open(userpath, 'r'), parse_dates=['time'], index_col=['time'], date_parser=dateparse,
                              chunksize=100000):
            try:
                df = pd.merge(df.reset_index(), df_p, on=['item_id']).set_index('time')

                for i in range(31):  # 初始化字典
                    if i <= 12:
                        date = '2014-11-%d' % (i + 18)
                    else:
                        date = '2014-12-%d' % (i - 12)
                    # print(df[date].shape[0])
                    count_day[date] += df[date].shape[0]

                batch += 1
                print('chunk %d done.' % batch)
            except StopIteration:
                print('finish data process.')
                break
        # 保存count_day到csv文件
        row_dict2csv(count_day, "../data/count_day_of_P.csv")
        df_count_day = pd.read_csv(open("../data/count_day_of_P.csv", 'r'), header=None, names=['time', 'count'])

        df_count_day = df_count_day.set_index('time')

        df_count_day.head(10)

        df_count_day['count'].plot(kind='bar')
        plt.legend(loc='best')
        plt.title("属于商品子集P的每天的操作次数")
        plt.grid(True)
        plt.show()

    '''
    统计12月17号和18号两天中每小时产生的行为量和购买量
    '''
    def count_behavior_by_hour(self, userPath):
        count_hour_1217 = {}
        count_hour_1218 = {}

        for i in range(24):
            time_str17 = '2014-12-17 %02.d' % i
            time_str18 = '2014-12-18 %02.d' % i

            count_hour_1217[time_str17] = [0, 0, 0, 0]
            count_hour_1218[time_str18] = [0, 0, 0, 0]

        batch = 0
        dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d %H')
        for df in pd.read_csv(open(userPath, "r"), parse_dates=['time'], index_col=['time'], date_parser=dateparse, chunksize=50000):
            try:
                for i in range(24):
                    time_str17 = '2014-12-17 %02.d' % i
                    time_str18 = '2014-12-18 %02.d' % i
                    tmp17 = df[time_str17]['behavior_type'].value_counts()
                    tmp18 = df[time_str18]['behavior_type'].value_counts()

                    for j in range(len(tmp17)):
                        count_hour_1217[time_str17][tmp17.index[j] - 1] += tmp17[tmp17.index[j]]
                    for j in range(len(tmp18)):
                        count_hour_1218[time_str18][tmp18.index[j] - 1] += tmp18[tmp18.index[j]]
                batch += 1
                print('chunk %d done !' % batch)
            except StopIteration:
                print("数据处理完成！")
                break

        df_1217 = pd.DataFrame.from_dict(count_hour_1217, orient='index')
        df_1218 = pd.DataFrame.from_dict(count_hour_1218, orient='index')

        df_1217.to_csv("../data/count_hour17.csv")
        df_1218.to_csv("../data/count_hour18.csv")

        df_1217 = pd.read_csv("../data/count_hour17.csv", index_col=0)
        df_1218 = pd.read_csv("../data/count_hour18.csv", index_col=0)
        print(df_1217)

        df_1718 = pd.concat([df_1217, df_1218])

        f1 = plt.figure(1)
        df_1718.plot(kind='bar')
        plt.legend(loc='best')
        plt.grid(True)
        plt.show()

        f2 = plt.figure(2)
        df_1718['3'].plot(kind='bar', color='r')
        plt.legend(loc='best')
        plt.grid(True)
        plt.show()

    '''
    用户行为分析
    '''
    def user_behavior_analysis(self, userPath):
        user_list = [10001082,
                     10496835,
                     107369933,
                     108266048,
                     10827687,
                     108461135,
                     110507614,
                     110939584,
                     111345634,
                     111699844]

        user_count = {}
        for i in range(len(user_list)):
            user_count[user_list[i]] = [0, 0, 0, 0, 0]

        batch = 0
        for df in pd.read_csv(open(userPath, 'r'), chunksize=100000, index_col=['user_id']):
            try:
                for i in range(10):
                    tmp = df[df.index == user_list[i]]['behavior_type'].value_counts()
                    sum_beh = 0
                    for j in range(len(tmp)):
                        user_count[user_list[i]][tmp.index[j]-1] += tmp[tmp.index[j]]

                batch += 1
                print('chunk %d done !' % batch)
            except StopIteration:
                print("Iteration is stopped.")
                break

        for i in range(10):
            user_count[user_list[i]][4] = user_count[user_list[i]][3] / \
                                          (user_count[user_list[i]][0] +
                                           user_count[user_list[i]][1] +
                                           user_count[user_list[i]][2] +
                                           user_count[user_list[i]][3])

        df_user_count = pd.DataFrame.from_dict(user_count, orient='index')  # convert dict to dataframe)
        df_user_count.to_csv("../data/user_count.csv")

if __name__ == '__main__':
    alys = Analysis()

    itemPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_item.csv"
    userPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_user.csv"

    # alys.time_cost_of_data_loading(userPath)
    # print(alys.CTR(userPath))
    alys.user_behavior_analysis(userPath)
