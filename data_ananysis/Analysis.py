import pandas as pd
import timeit
import os
import sys
from data_ananysis.dict_convert_csv import *
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


if __name__ == '__main__':
    alys = Analysis()

    itemPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_item.csv"
    userPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_user.csv"

    # alys.time_cost_of_data_loading(userPath)
    # print(alys.CTR(userPath))
    alys.count_behavior_inP_by_date(userPath, itemPath)

