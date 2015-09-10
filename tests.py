# -*- coding: utf-8 -*-

import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt

col_name = list(pd.read_csv('C:\Users\User\PycharmProjects\ibcda\data\column_names_train.csv', sep=None, engine='python'))
set100k = pd.read_csv('C:\Users\User\PycharmProjects\ibcda\data\\test2.csv',
                     names=col_name,
                     sep=None, engine='python')
tt_df = pd.read_csv('C:\Users\User\PycharmProjects\ibcda\data\\target_train.csv',
                    names=['Date', 'ID_2_1', 'ID_2_2', 'Target'],
                    sep=None, engine='python')
result = pd.merge(set100k, tt_df, on=['ID_2_1', 'ID_2_2'])
result.to_csv('C:\Users\User\PycharmProjects\ibcda\data\\result_2.csv')