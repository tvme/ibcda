# -*- coding: utf-8 -*-

import gzip

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

if __name__ == '__main__':
    import os

    dir_path = os.getcwdu()
    source_file = '\data\source_test.csv'
    target_file = '\data\\test.csv'
    cut_file(dir_path, source_file, target_file, 100, 0)