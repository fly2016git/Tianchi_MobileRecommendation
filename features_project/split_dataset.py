import pandas as pd
'''
按日期对文件进行分割，以7天为一个周期
'''
#输入
userPath = "/home/alfred/mywork/tianchi/action1/fresh_comp_offline/tianchi_fresh_comp_train_user.csv"
#输出
path_df_part_1 = '../data/features/path_df_part_1.csv'
path_df_part_2 = '../data/features/path_df_part_2.csv'
path_df_part_3 = '../data/features/path_df_part_3.csv'

path_df_part_1_y = '../data/features/path_df_part_1_y.csv'
path_df_part_2_y = '../data/features/path_df_part_2_y.csv'

path_df_part_1_uic_label = "../data/features/df_part_1_uic_label.csv"
path_df_part_2_uic_label = "../data/features/df_part_2_uic_label.csv"
path_df_part_3_uic = "../data/features/df_part_3_uic.csv"

#分割数据集
def dataset_split():
    batch = 0
    dateparse = lambda dates: pd.datetime.strptime(dates, '%Y-%m-%d %H')
    for df in pd.read_csv(open(userPath, 'r'), parse_dates=['time'], date_parser=dateparse, index_col=['time'],
                          chunksize=100000):
        try:
            df_part_1 = df['2014-11-22':'2014-11-27']
            df_part_2 = df['2014-11-29':'2014-12-04']
            df_part_3 = df['2014-12-13':'2014-12-18']

            df_part_1_tar = df['2014-11-28']
            df_part_2_tar = df['2014-12-05']

            df_part_1.to_csv(path_df_part_1, columns=['user_id', 'item_id', 'behavior_type', 'item_category'],
                             header=False,
                             mode='a')
            df_part_2.to_csv(path_df_part_2, columns=['user_id', 'item_id', 'behavior_type', 'item_category'],
                             header=False,
                             mode='a')
            df_part_3.to_csv(path_df_part_3, columns=['user_id', 'item_id', 'behavior_type', 'item_category'],
                             header=False,
                             mode='a')

            df_part_1_tar.to_csv(path_df_part_1_y, columns=['user_id', 'item_id', 'behavior_type', 'item_category'],
                                 header=False,
                                 mode='a')
            df_part_2_tar.to_csv(path_df_part_2_y, columns=['user_id', 'item_id', 'behavior_type', 'item_category'],
                                 header=False,
                                 mode='a')

            batch += 1
            print("chunk %d done!" % batch)

        except StopIteration:
            print(u"数据分割完成！")
            break


#构建“用户-商品-类别”数据集(UIC-label)
def build_uic_label(path_df_part, path_df_part_y, path_df_part_uic_label):
    with open(path_df_part, 'r') as datafile:
        df_part = pd.read_csv(datafile, index_col=False)
        df_part.columns = ['time', 'user_id', 'item_id', 'behavior_type', 'item_category']

    df_part_uic = df_part.drop_duplicates(['user_id', 'item_id', 'item_category'])[
        ['user_id', 'item_id', 'item_category']]

    with open(path_df_part_y, 'r') as datafile:
        df_part_tar = pd.read_csv(datafile, index_col=False, parse_dates=[0])
        df_part_tar.columns = ['time', 'user_id', 'item_id', 'behavior_type', 'item_category']

    df_part_1_uic_label_1 = df_part_tar[df_part_tar['behavior_type'] == 4][['user_id', 'item_id', 'item_category']]
    df_part_1_uic_label_1.drop_duplicates(['user_id', 'item_id'], 'last', inplace=True)

    df_part_1_uic_label_1['label'] = 1
    print(df_part_1_uic_label_1.head(10))

    df_part_1_uic_label = pd.merge(df_part_uic, df_part_1_uic_label_1, on=['user_id', 'item_id', 'item_category'],
                                   how='left')
    print(df_part_1_uic_label.head(10))
    df_part_1_uic_label.to_csv(path_df_part_uic_label, index=False)



#dataset_split()

#build_uic_label(path_df_part_1, path_df_part_1_y, path_df_part_1_uic_label)

build_uic_label(path_df_part_2, path_df_part_2_y, path_df_part_2_uic_label)






















