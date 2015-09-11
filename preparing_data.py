# -*- coding: utf-8 -*-

import gzip
import os
import pandas as pd

def unzip_gz(dir_path, file_name_gz, file_name_csv):
    """
    unpack gz archive and wright file.csv
    read byte mod
    wright byte mod
    """
    with gzip.open(dir_path + file_name_gz, 'rb') as file_gz:
        file_content = file_gz.read()
        with open(dir_path + file_name_csv, 'wb') as file_csv:
            file_csv.write(file_content)
    print 'File ' + file_name_gz + ' successfully unpacked'
    print 'File ' + file_name_csv + ' successfully written'

def cut_file(dir_path, source_file, target_file, num_lines, start_line=0):
    """
    read from source_file in dir_path num_lines from start_line
    save results in target_file
    """
    bufsize = 65536
    with open(dir_path+target_file, 'w') as f_target:
        with open(dir_path+source_file) as infile:
            line_position = 0
            while True:
                lines = infile.readlines(bufsize)
                if not lines or line_position > start_line + num_lines:
                    break
                for line in lines:
                    if line_position >= start_line and line_position < start_line + num_lines:
                        f_target.write(line)
                    line_position += 1

def select_data_with_train(dir_path, train, result, id_set=1):
    """
    select rows in dir_path/train with ID in target_train_index.csv
    add columns Date and Target
    wright result.csv
    use pair ID 1_1 and ID_1_2 if id_set = 1
    and pair ID 2_1 and ID_2_2 if id_set = 2
    """
    train_col_name = list(pd.read_csv(dir_path + '\data\column_names_train.csv', sep=None, engine='python'))
    train_df = pd.read_csv(dir_path + train, names=train_col_name, sep=None, engine='python')
    if id_set == 1:
        target_col_name = ['Date', 'ID_1_1', 'ID_1_2', 'Target']
        select_col_name = ['ID_1_1', 'ID_1_2']
    elif id_set == 2:
        target_col_name = ['Date', 'ID_2_1', 'ID_2_2', 'Target']
        select_col_name = ['ID_2_1', 'ID_2_2']
    else:
        print 'Wrong ID set'
        return
    target_train_df = pd.read_csv(dir_path + '\data\\target_train_index.csv', sep=None, engine='python')
    result_df = pd.merge(train_df, target_train_df, on=select_col_name)
    result_df.to_csv(dir_path + result)

def index_id_target(dir_path, file_index_name, id_set):
    """
    make set of ID (clear duplicate) from dir_path/target_train.csv
    columns Date and Target will be removed
    wright in file_index_name
    use pair ID 1_1 and ID_1_2 if id_set = 1
    and pair ID 2_1 and ID_2_2 if id_set = 2
    """
    if id_set == 1:
        target_col_name = ['Date', 'ID_1_1', 'ID_1_2', 'Target']
        select_col_name = ['ID_1_1', 'ID_1_2']
    elif id_set == 2:
        target_col_name = ['Date', 'ID_2_1', 'ID_2_2', 'Target']
        select_col_name = ['ID_2_1', 'ID_2_2']
    else:
        print 'Wrong ID set'
        return
    target_train_df = pd.read_csv(dir_path + '\data\\target_train.csv', names=target_col_name, usecols=select_col_name,
                                  sep=None, engine='python')
    target_train_df = target_train_df.drop_duplicates()
    target_train_df.to_csv(dir_path + file_index_name)

def devide_large_into_pieces(dir_path, large_file, ):
    """
    divide large file into list_files
    all files is num_rows except the last piece
    :return list_files - list of filename
    """
def processing_pieces():
    """
    select rows in list_files with ID in target_train_index.csv
    wright in pieces_N
    """

def concatenate_pieces():
    """
    concatenate files in list_files into result
    """

if __name__ == '__main__':
    dir_path = os.getcwdu()
    # source_file = '\data\\train.csv'
    # target_file = '\data\\test3.csv'
    # cut_file(dir_path, source_file, target_file, 2500000, 0)

    # index_id_target(dir_path, '\data\\target_train_index.csv', 1)
    select_data_with_train(dir_path, '\data\\test3.csv', '\data\\result3.csv', id_set=1)