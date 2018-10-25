#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 CloudBrain <byzhang@>
#
# Distributed under terms of the CloudBrain license.

import random
import tensorflow as tf
from datetime import datetime, timedelta
import sys
import os

rate = 0.9

def main(argv):
    split_data_dir = '/home/wllu/data/cpc_v1_online_8/split_online_data/'
    split_data_train_cfg = '/home/wllu/data/cfg/split_data_train/'
    split_data_valid_cfg = '/home/wllu/data/cfg/split_data_valid/'
    base_cfg = day + '_'+str(int(hour)-1)
    split_data_train_cfg_f = open(split_data_train_cfg + base_cfg, 'a')
    split_data_valid_cfg_f = open(split_data_valid_cfg + base_cfg, 'a')

    #split and shuffle tfrecords
    for dirpath,dirnames,filenames in os.walk(base_dir+day+'/'+hour):
        for file in filenames:
            fullpath = os.path.join(dirpath,file)
            full_path_split = fullpath.split('/')
            f = open(fullpath, 'r')
            train_data = split_data_dir + 'train/' + full_path_split[-3] + '_' \
                + full_path_split[-2] + '_' + full_path_split[-1]
            valid_data = split_data_dir + 'valid/' + full_path_split[-3] + '_' \
                + full_path_split[-2] + '_' + full_path_split[-1]
            train_data = train_data.strip('\n')
            valid_data = valid_data.strip('\n')
            print(train_data)
            f0 = tf.python_io.TFRecordWriter(train_data)
            f1 = tf.python_io.TFRecordWriter(valid_data)
            for r in tf.python_io.tf_record_iterator(fullpath):
                rand = random.random()
                if rand < rate:
                    f0.write(r)
                else:
                    f1.write(r)
            split_data_train_cfg_f.write(train_data + "\n")
            split_data_valid_cfg_f.write(valid_data + "\n")

    #define data to be deleted in cfg file
    date_to_be_del = datetime.now() - timedelta(days=9)
    date_to_be_del_str = date_to_be_del.strftime('%Y-%m-%d')
    del_train_data_cfg = split_data_dir + 'train/' + date_to_be_del_str \
        + '_'+ hour
    del_valid_data_cfg = split_data_dir + 'valid/' + date_to_be_del_str \
        + '_' + hour

    #instert new generated train/valid data file full path
    #and remove old fashion data in cfg
    new_split_data_train_cfg = [l for l in open(split_data_train_cfg \
                            + base_cfg, "r") if l.find(del_train_data_cfg) != 0]
    new_split_data_valid_cfg = [l for l in open(split_data_valid_cfg \
                            + base_cfg, "r") if l.find(del_valid_data_cfg) != 0]
    fd = open(split_data_train_cfg + day + '_' + hour, 'a')
    fd.writelines(new_split_data_train_cfg)
    fv = open(split_data_valid_cfg + day + '_' + hour, 'a')
    fv.writelines(new_split_data_valid_cfg)
    split_data_train_cfg_f.close()
    split_data_valid_cfg_f.close()
    fd.close()
    fv.close()

if __name__ == "__main__":
    day = sys.argv[1]
    hour = sys.argv[2]
    base_dir = sys.argv[3]
    tf.app.run(main)
