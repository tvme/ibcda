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