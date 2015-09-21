# -*- coding: utf-8 -*-

import os
import pandas as pd
from sklearn import preprocessing

def col_to_list(dir_path, source_file, col_num, delimiter):
    """
    read from dir_path\source_file col_num as list_out
    col_num begins from zero for the 1-st column
    :return list_out
    """
    bufsize = 65536
    with open(dir_path+source_file) as file_in:
        list_out = []
        while True:
            lines = file_in.readlines(bufsize)
            if not lines:
                break
            for line in lines:
                split_line = line.split(delimiter)
                list_out.append(split_line[col_num])
    return list_out

def col_to_date(dir_path, source_file, col_num, delimiter):
    """
    read from dir_path\source_file col_num as list_out date without hours, minutes ets.
    col_num begins from zero for the 1-st column
    :return list_out
    """
    bufsize = 65536
    with open(dir_path+source_file) as file_in:
        list_out = []
        while True:
            lines = file_in.readlines(bufsize)
            if not lines:
                break
            for line in lines:
                split_line = line.split(delimiter)
                date_without_time = split_line[col_num].split()[0]
                list_out.append(date_without_time)
    return list_out

def data_to_index(dir_path, file_in, filename_out, list_col_index, list_col_date, delimiter):
    """
    Data from list_in convert data to int indices
    Write result in path_name\filename_out
    :param list_col_index - list columns names for indexing
    :param list_col_date - list date columns names
    """

    all_col_name = list(pd.read_csv(dir_path + 'column_names_train.csv', sep=None, engine='python'))
    data_indexed = pd.DataFrame()

    for col in list_col_index:
        col_num = all_col_name.index(col)
        list_in = col_to_list(dir_path, file_in, col_num, delimiter)
        col_proc = preprocessing.LabelEncoder()
        col_proc.fit(list_in)
        print 'Length of ' + col + '\t\t\t' + str(len(list_in))
        print 'Lenght of indeces ' + col + '\t' + str(len(col_proc.classes_))
        print '-' * 30
        data_indexed[col] = pd.Series(col_proc.transform(list_in))

    for col in list_col_date:
        col_num = all_col_name.index(col)
        data_indexed[col] = pd.Series(col_to_date(dir_path, file_in, col_num, delimiter))

    data_indexed.to_csv(dir_path + filename_out)

if __name__  == '__main__':
    dir_path = os.getcwd() + '\data\\'
    # filename_in = 'target_all_with_train.csv'
    filename_in = 'test2.csv'
    delimiter = '\t'
    filename_out = 'test_2_indexed.csv'
    column_names_file = 'column_names_train.csv'
    train_col_name = list(pd.read_csv(dir_path + column_names_file, sep=None, engine='python'))
    data_to_index(dir_path, filename_in, filename_out, train_col_name, ['Y2'], delimiter)