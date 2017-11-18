#coding: utf-8
import codecs
import os
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

START = 1001
END = 1500

def merge(target):
    save_to = target + '.json'
    with open(save_to, 'w') as f_out:
        for directory, _, files in os.walk(target):
            for file_name in files:
                with codecs.open(os.path.join(directory, file_name), 'r', encoding='utf-8') as f:
                    for row in f.readlines():
                        s = row[:-2] + ',"video_id":"{0}"}}\n'.format(file_name[:-6])
                        f_out.write(s)

for i in range(START, END):
    merge('{0:04d}'.format(i))
