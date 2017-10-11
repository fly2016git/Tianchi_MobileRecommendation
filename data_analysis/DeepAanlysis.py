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
    da.gen_act_1234(userPath)
