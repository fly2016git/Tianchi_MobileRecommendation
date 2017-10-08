import pandas as pd
import matplotlib.pyplot as plt

df_count_day = pd.read_csv(open("../data/count_day.csv", 'r'), header=None, names=['time', 'count'])
print(df_count_day.head(10))

df_count_day = df_count_day.set_index('time')

print(df_count_day.head(10))

df_count_day['count'].plot(kind='bar')
plt.legend(loc='best')

plt.grid(True)
plt.show()