# -*- coding: utf-8 -*-

import os
import pandas as pd

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

def select_data(dir_path, train, result, filename_index, id_set):
    """
    select rows in dir_path\\train with ID in filename_index
    add columns Date and Target
    save in result.csv
    use pair ID 1_1 and ID_1_2 if id_set = 1
    and pair ID 2_1 and ID_2_2 if id_set = 2
    """
    train_col_name = list(pd.read_csv(dir_path + 'column_names_train.csv', sep=None, engine='python'))
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
    target_train_df = pd.read_csv(dir_path + filename_index, sep=None, engine='python')
    result_df = pd.merge(train_df, target_train_df, on=select_col_name)
    result_df.to_csv(dir_path + result, index=False)

def index_id_target_train(dir_path, filename_index, id_set):
    """
    make set of ID (clear duplicate) from dir_path\\target_train.csv
    columns Date and Target will be removed
    save in file_index_name
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
    target_train_df = pd.read_csv(dir_path + 'target_train.csv', names=target_col_name, usecols=select_col_name,
                                  sep=None, engine='python')
    target_train_df = target_train_df.drop_duplicates()
    target_train_df.to_csv(dir_path + filename_index, index=False)
    print 'Write file of index: ' + filename_index

def count_lines(dir_path, file_name):
    """
    :return: length (in lines) dir_path\\file_name
    """
    with open(dir_path + file_name, mode='r') as f:
        lines = 0
        for line in f:
            lines += 1
    return lines

def devide_large_into_pieces(dir_path, large_file, base_name_piece, num_rows):
    """
    divide large file into list_files
    all files is num_rows except the last piece
    :return list_files - list of filename
    """
    current_row = 0
    current_file = 0
    list_file = []
    length_f = count_lines(dir_path, large_file)
    print 'In large file ' + str(length_f) + ' lines'
    while current_row < length_f:
        name_of_piece = base_name_piece + '_' + str(current_file) + '.csv'
        cut_file(dir_path, large_file, name_of_piece, num_rows, current_row)
        print 'Write file: ' + name_of_piece
        current_row += num_rows
        list_file.append(name_of_piece)
        current_file += 1
    return list_file

def processing_pieces(dir_path, list_file_in, base_name_result, filename_index, id_set):
    """
    select_data_with_train (ID in file index_set) from file in list_files
    save in base_name_result_N
    :return list_file_out
    """
    current_file = 0
    list_file_out = []
    for name_piece in list_file_in:
        name_result = base_name_result + '_' + str(current_file) + '.csv'
        select_data(dir_path, name_piece, name_result, filename_index, id_set)
        print 'Write file: ' + name_result
        current_file += 1
        list_file_out.append(name_result)
    return list_file_out

def concatenate_pieces(dir_path, list_file_in, file_out):
    """
    concatenate files from list_files_in into file_out
    """
    with open(dir_path + file_out, 'w') as outfile:
        with open(dir_path + list_file_in[0]) as first_infile:
            for line in first_infile:
                outfile.write(line)
        for fname in list_file_in[1:]: # read and wright files skip header
            with open(dir_path + fname) as infile:
                infile.next()
                for line in infile:
                    outfile.write(line)
    print 'Write: ' + file_out

def select_target_data_with_train(dir_path, filename_index, file_in, file_out, num_lines, id_set):
    """
    select_target_data from file_in with_train
    :param dir_path: dir with data
    :param filename_index: file set of indices with train
    :param file_in: parsing file
    :param file_out: result file
    :param num_lines: number of lines in intermediate files
    :param id_set: ID_1_1 + ID_1_2 or ID_2_1 + ID_2_2
    save in file_out
    """
    list_pieces = devide_large_into_pieces(dir_path, file_in, 'piece', num_lines)  # devide large file, num_lines in piece
    list_result = processing_pieces(dir_path, list_pieces, 'result', filename_index, id_set)
    concatenate_pieces(dir_path, list_result, file_out)

if __name__ == '__main__':
    dir_path = os.getcwdu() + '\data\\'
    filename_index = 'target_train_index.csv'
    file_in = 'test2.csv'
    file_out = 'target_100k_with_train.csv'
    num_lines = 15000
    index_id_target_train(dir_path, filename_index, 1)
    select_target_data_with_train(dir_path,filename_index, file_in, file_out, num_lines, 1)
