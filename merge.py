#coding: utf-8
import codecs
import os
import json

START = 1
END = 101

def merge(target):
    save_to = target + '.json'
    with open(save_to, 'w') as f_out:
        for directory, _, files in os.walk(target):
            for file_name in files:
                with codecs.open(os.path.join(directory, file_name), 'r', encoding='utf-8') as f:
                    for row in f.readlines():
                        s = row[:-2] + ',"video_id":"{}"}}\n'.format(file_name[:-6])
                        f_out.write(s)

for i in range(START, END):
    merge('{0:04d}'.format(i))
