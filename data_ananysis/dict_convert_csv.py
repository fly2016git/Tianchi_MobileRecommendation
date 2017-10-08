import csv

'''
字典与csv之间的转换
'''
def csv2dict(csv_file, key, value):
    new_dict = {}
    with open(csv_file, 'r')as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            new_dict[row[key]] = row[value]
    return new_dict


# 把csv文件的每一行转换为key-value的形式
def row_csv2dict(csv_file=""):
    new_dict = {}
    with open(csv_file)as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            new_dict[row[0]] = row[1]
    return new_dict


####
# convert dict to csv file
####

# 把字典转换为csv文件，
def dict2csv(raw_dict={}, csv_file=""):
    with open(csv_file, 'w') as f:
        w = csv.writer(f)
        # write all keys on one row and all values on the next
        w.writerow(raw_dict.keys())
        w.writerow(raw_dict.values())


# 把字典转换为csv文件，(key-value 1-1 pairs each row)
def row_dict2csv(raw_dict={}, csv_file=""):
    with open(csv_file, 'w') as f:
        w = csv.writer(f)
        w.writerows(raw_dict.items())


#  把字典转换为csv文件，(key-[value] 1-M pairs each row)
def row2_dict2csv(raw_dict={}, csv_file=""):
    with open(csv_file, 'w') as f:
        w = csv.writer(f)
        for k, v in raw_dict.items():
            w.writerows([k, v])

raw_dict = {
    "1": "123",
    "2": "234",
    "3": "345"
}

dict2csv(raw_dict, "/home/alfred/test.csv")
