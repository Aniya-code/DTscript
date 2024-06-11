import csv
from collections import defaultdict
from tqdm import trange

def load_fingerprint_library(file_path):
    fingerprint_library = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['video_fp'] = list(map(int, row['video_fp'].split('/')[1:]))
            row['audio_fp'] = list(map(int, row['audio_fp'].split('/')[1:]))
            row['video_timeline'] = list(map(int, row['video_timeline'].split('/')[1:]))
            row['audio_timeline'] = list(map(int, row['audio_timeline'].split('/')[1:]))
            fingerprint_library.append(row)
    return fingerprint_library


# def check_time_constraint(fingerprint, v_start, v_end, a_start, a_end):
#     # 这里假设存在一个实际的检查时间约束的逻辑
#     # 返回值应当是布尔值
#     return True
def check_time_constraint(fingerprint, video_seg_start_idx, video_seg_end_idx, audio_seg_start_idx, audio_seg_end_idx):
    video_timeline = fingerprint['video_timeline']
    audio_timeline = fingerprint['audio_timeline']
    video_seg_start_time = video_timeline[video_seg_start_idx]
    video_seg_end_time = video_timeline[video_seg_end_idx]
    audio_seg_start_time = audio_timeline[audio_seg_start_idx]
    audio_seg_end_time = audio_timeline[audio_seg_end_idx]
    if audio_seg_start_time - video_seg_start_time >= 0 and audio_seg_end_time - video_seg_end_time >= 0:
        return True
    return False

def match_chunk_with_fingerprint(chunk, fingerprint, v_start_idx, a_start_idx, first_chunk=False, max_video_blocks=5, max_audio_blocks=4):
    video_fp = fingerprint['video_fp']
    audio_fp = fingerprint['audio_fp']
    
    # max_video_blocks = 5  #1~5  78%
    # max_audio_blocks = 4  #0~4 OK

    v_start_len = max_video_blocks if first_chunk else 1 #第一块默认考虑2倍的max, 默认考虑一个偏移（因为已经丢掉一个块了，相当于考虑了1~2个chunk偏移）
    a_start_len = max_audio_blocks if first_chunk else 1

    for a_start in range(a_start_idx, a_start_idx+a_start_len):
        for v_start in range(v_start_idx, v_start_idx+v_start_len):
            for a_end in range(a_start, min(len(audio_fp) + 1, a_start + max_audio_blocks + 1)): #保证长度max_audio_blocks
                for v_end in range(v_start + 1, min(len(video_fp) + 1, v_start + max_video_blocks + 1)): #保证长度max_video_blocks
                    if (v_end - v_start) >= (a_end - a_start) and check_time_constraint(fingerprint, v_start, v_end, a_start, a_end):
                        video_sum = sum(video_fp[v_start:v_end])
                        audio_sum = sum(audio_fp[a_start:a_end])
                        total_sum = video_sum + audio_sum
                        if -2000 <= chunk-total_sum < 4000:
                            # if "uyhm" in fingerprint['url']:
                            # print(f"--{chunk}\nMatch {fingerprint['ID']}: {fingerprint['url'][-11:]}: \n {video_sum}={video_fp[v_start:v_end]}s_idx:{v_start} \n {audio_sum}={audio_fp[a_start:a_end]}s_idx:{a_start} \n {chunk-total_sum}")
                            return True, v_end, a_end, v_start-v_start_idx, a_start-a_start_idx # return:（boolean, 下一个chunk去匹配每一行时的v_start_idx, 同理a_start_idx）
    
    return False, -1, -1, -1, -1

def word_counts(dict_list):
    summed_dict = defaultdict(int)
    for d in dict_list:
        k = d['url']
        v = d['match_count']
        summed_dict[k] += v
    return dict(summed_dict)

def decide_best_match(matches):
    # 根据具体需求定义决策逻辑，比如选择匹配次数最多的
    if not matches:
        return "Unknown"
    alive_matches = [m for m in matches if m['alive']]
    match len(alive_matches):
        case x if x == 0:
            global current_matches
            for m in current_matches:
                m['alive'] = True
            return None
            # matches = word_counts(matches)
        case x if x == 1:
            global final_result
            final_result = alive_matches[0]['url']
            return final_result
        case _:
            matches = word_counts(alive_matches)
    matched = max(matches.items(), key=lambda x: x[-1])
    return matched[0]
        
def update_matches(new_chunk, fingerprint_library, first_chunk=False, is_restart=False):
    global chunk_stream, current_matches
    chunk_stream.append(new_chunk)
    
    if first_chunk and not is_restart:
        current_matches.clear()
        for fingerprint in fingerprint_library:
            success, v_idx, a_idx, v_h_jump, a_h_jump = match_chunk_with_fingerprint(new_chunk, fingerprint, 0, 0, first_chunk=True)
            if success:
                current_matches.append({'ID': fingerprint['ID'],'url': fingerprint['url'], 'match_count': 1, 'v_start_idx': v_idx, 'a_start_idx': a_idx, 'v_h_jump': v_h_jump, 'a_h_jump':a_h_jump, 'alive': True, 'reborn': 1})
    else:
        for match in current_matches:
            if not match['alive']:
                continue
            id = match['ID']
            for fingerprint in fingerprint_library:
                if fingerprint['ID'] == id:
                    first_chunk = True if is_restart else False
                    success, v_idx, a_idx, _, _ = match_chunk_with_fingerprint(new_chunk, fingerprint, match['v_start_idx'], match['a_start_idx'], first_chunk=first_chunk)
                    if success:
                        match['match_count'] += 1
                        match['v_start_idx'] = v_idx
                        match['a_start_idx'] = a_idx
                    else:
                        if match['reborn'] <= 0:
                            match['alive'] = False
                            # print(f"淘汰：{match}")
                        else:
                            match['reborn'] -= 1
                        
    
    best_match = decide_best_match(current_matches)
    if best_match:
        return best_match
    else:
        # 全部都挂了，抛弃所有的第一个匹配到的组合，重新启动（第一次匹配会更具有弹性）
        global cur_context
        cur_context['cur_chunk'] = new_chunk
        chunk_stream.clear()
        print('***********restarted***********')
        return main(cur_context['test_case'], is_restart=True)

def main(test_case, is_restart=False):
    global fingerprint_library, final_result, cur_context
    global chunk_stream, current_matches, final_result
    if not is_restart:
        fingerprint_library = load_fingerprint_library('final_yt_online1.csv')
        cur_context = {'test_case':test_case}
        chunk_stream = []
        current_matches = []
        final_result = None
        start_case_idx = 0
    else:
        start_case_idx = list(test_case).index(cur_context['cur_chunk'])

    ret = None
    for i in range(start_case_idx, len(test_case)):
        new_chunk = test_case[i]
        if not final_result:
            best_match_url = update_matches(new_chunk, fingerprint_library, first_chunk=(len(chunk_stream) == 0), is_restart=is_restart)
            print(f'Best match URL: {best_match_url}')
            ret = best_match_url
        else:
            print(f"=-=-=-=Final result: {final_result}=-=-=-=")
            ret = final_result
    return ret

def load_test_csv(file_path):
    test_csv = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['body_list'] = list(map(int, row['body_list'].split('/')[1:]))
            test_csv.append(row)
    return test_csv

if __name__ == '__main__':
    good = 0
    test_csv = load_test_csv('body_list_test.csv')
    wrong_list = []
    for row in test_csv:

        test_case = row['body_list']
        test_case[0] -= 2000
        take_idx = 0
        for i in range(len(test_case)):
            case = test_case[i]
            if case / 1e6 > 1 and str(case)[:2] in ['31','16']:
                take_idx += 1
            else:
                break
        test_case = test_case[take_idx: take_idx+10]
        print(f"video:{row['url']}, test_case:{test_case}")
        ret = main(test_case)

        if row['url'] in ret:
            good += 1
        else:
            wrong_list.append({row['url']: test_case})
            print("#### Wrong ######")
            print(test_case)
            print("############# Wrong ##############")
        print(good)
        print(len(test_csv))
        print(good/len(test_csv))
        print(wrong_list)

    for d in wrong_list:
        for url, case in d.items():
            case = case
            ret = main(case)
            print(url)
            print(case)

    # test_case = "3130249, 1743511, 1374369, 1557207, 1634824, 2905805, 413265, 1358585, 2555002, 404415"
    # test_case = list(map(int, test_case.split(', ')))[1:]
    # main(test_case)