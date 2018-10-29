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
import shutil

FLAGS = tf.app.flags.FLAGS

tf.app.flags.DEFINE_string("split_data_train_dir",\
                           '/home/wllu/data/cpc_v1_online_8/split_online_data/train/', \
                           "dir for splited and shuffled train data")
tf.app.flags.DEFINE_string("split_data_valid_dir", \
                           '/home/wllu/data/cpc_v1_online_8/split_online_data/valid/',\
                           "dir for splited and shuffled valid data")
tf.app.flags.DEFINE_string("split_data_train_cfg", \
                           '/home/wllu/data/cfg/split_data_train/', \
                           "splited and shuffled train data cfg file")
tf.app.flags.DEFINE_string("split_data_valid_cfg", \
                           '/home/wllu/data/cfg/split_data_valid/', \
                           "splited and shuffled valid data cfg file")
tf.app.flags.DEFINE_string("base_dir", '/home/wllu/data/cpc_v1_online_8/tfrecords/', \
                           "raw tfrecords file dir")
tf.app.flags.DEFINE_float("train_split_rate", 0.9, "split rate for train data")


date_time_now = datetime.now()
base_cfg = (date_time_now - timedelta(hours=1)).strftime('%Y-%m-%d') + '_' \
               + (date_time_now - timedelta(hours=1)).strftime('%H')
pre_base_cfg = (date_time_now - timedelta(hours=2)).strftime('%Y-%m-%d') + '_' \
               + (date_time_now - timedelta(hours=2)).strftime('%H')
data_dir = date_time_now.strftime('%Y-%m-%d') + '/' + \
               (date_time_now - timedelta(hours=1)).strftime('%H')
date_to_be_del = (date_time_now - timedelta(days = 9)).strftime('%Y-%m-%d')
hour_to_be_del = (date_time_now - timedelta(hours = 1)).strftime('%H')

def update_cfg(self, train_data, valid_data, is_normal_data):
    del_train_data_cfg = FLAGS.split_data_train_dir + date_to_be_del \
              + '_'+ hour_to_be_del
    del_valid_data_cfg = FLAGS.split_data_valid_dir + date_to_be_del \
              + '_' + hour_to_be_del
    new_split_data_train_cfg = [l for l in open(FLAGS.split_data_train_cfg \
                  + pre_base_cfg, "r") if l.find(del_train_data_cfg) != 0]
    new_split_data_valid_cfg = [l for l in open(FLAGS.split_data_valid_cfg \
                  + pre_base_cfg, "r") if l.find(del_valid_data_cfg) != 0]
    if is_normal_data == True:
        new_split_data_train_cfg.append(train_data + '\n')
        new_split_data_valid_cfg.append(valid_data + '\n')
    with open(FLAGS.split_data_train_cfg + base_cfg, 'a') as ft:
        ft.writelines(new_split_data_train_cfg)
    with open(FLAGS.split_data_valid_cfg + base_cfg, 'a') as fv:
        fv.writelines(new_split_data_valid_cfg)
    tf.logging.warn('Generated new cfg file:{}'\
                    .format(FLAGS.split_data_train_cfg + base_cfg))
    tf.logging.warn('Generated new cfg file:{}'\
                    .format(FLAGS.split_data_valid_cfg + base_cfg))

def split_data(self, fullpath):
    if (os.path.isfile(fullpath) and fullpath.find("_SUCCESS") == -1 \
          and fullpath.find("_temporary") == -1):
        full_path_split = fullpath.split('/')
        with  open(fullpath, 'r') as f:
            train_data = FLAGS.split_data_train_dir + full_path_split[-3] + '_' \
                       + full_path_split[-2] + '_' + full_path_split[-1]
            valid_data = FLAGS.split_data_valid_dir + full_path_split[-3] + '_' \
                       + full_path_split[-2] + '_' + full_path_split[-1]
            train_data = train_data.strip('\n')
            valid_data = valid_data.strip('\n')
            f0 = tf.python_io.TFRecordWriter(train_data)
            f1 = tf.python_io.TFRecordWriter(valid_data)
            for r in tf.python_io.tf_record_iterator(fullpath):
                rand = random.random()
                if rand < FLAGS.train_split_rate:
                    f0.write(r)
                else:
                    f1.write(r)
        tf.logging.warn('Compeleted processing tfrecords file: {}'.format(fullpath))
        update_cfg(self, train_data, valid_data, True)
    else:
        tf.logging.warn('Not valid tfrecords file: {}'.format(fullpath))
        update_cfg(self, 'train', 'valid', False)

def main(self):
    date_now = date_time_now.strftime('%Y-%m-%d')
    hour_now = date_time_now.strftime('%H')
    if os.path.exists(FLAGS.base_dir + data_dir):
        for dirpath,dirnames,filenames in os.walk(FLAGS.base_dir + data_dir):
            for file in filenames:
                fullpath = os.path.join(dirpath,file)
                tf.logging.warn('Start to process data {}'.format(fullpath))
                split_data(self, fullpath)
    else:
        tf.logging.warn('Directory does not exit:{}'.format(FLAGS.base_dir + data_dir))
        update_cfg(self, 'train', 'valid', False)
if __name__ == "__main__":
    tf.app.run(main)
