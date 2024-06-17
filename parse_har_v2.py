import json
import csv
from datetime import datetime 
import re
import os
import itertools

def get_chunk_info(folder_path, result_path):
    files = os.listdir(folder_path)
    video_id = folder_path.split('/')[-1]
    
    with open(result_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        for filename in files:
            # writer.writerow(['#'*30 + filename + '#'*30])
            quality = filename.split('--')[1]
            har_file_path = os.path.join(folder_path, filename).replace('\\', '/')
            with open(har_file_path, "r", encoding='utf-8') as f:
                har_data = json.load(f)
            # 响应体变量
            headers_size = []
            body_size = []
            tansfer_size = []
            body_sum = []
            # 以前的range
            range_size = []
            url_set = set()
            url_list = []
            
            for entry in har_data['log']['entries']:
                url = entry['request']['url']
                if 'videoplayback?' in url and entry['response']['bodySize']>0:
                    headers_size.append(entry['response']['headersSize'])
                    body_size.append(entry['response']['bodySize'])
                    tansfer_size.append(entry['response']['_transferSize'])
                    match = re.search(r'sn-(.*?)\.google', url)
                    if match:
                        url_set.add(match.group(1))
                        url_list.append(match.group(1))

                if '&range=' in url:
                    pattern = r'&range=(\d+)-(\d+)'
                    # 搜索匹配
                    match = re.search(pattern, url)
                    if match:
                        start = int(match.group(1))
                        end = int(match.group(2))
                    range_size.append(end-start+1)
            
            # 干净的body_list
            clean_body_size = []
            for item in body_size:
                if item<500:
                    continue
                clean_body_size.append(item)
            #写入
            writer = csv.writer(file)
            writer.writerow([video_id, quality, '/'.join(map(str, clean_body_size))])
            print('...')
            
    
if __name__ == '__main__':
    
    har_path = 'data/har_result'
    result_path = f'data/results/body_list_test.csv'
    with open(result_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['url', 'quality', 'body_list'])
    for filename in os.listdir(har_path):
        folder_path = os.path.join(har_path, filename).replace("\\", "/")
        get_chunk_info(folder_path, result_path)

