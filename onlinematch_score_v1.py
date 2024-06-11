import csv
from collections import defaultdict

def _check_time_constraint(fingerprint, video_seg_start_idx, video_seg_end_idx, audio_seg_start_idx, audio_seg_end_idx):
    video_timeline = fingerprint['video_timeline']
    audio_timeline = fingerprint['audio_timeline']
    video_seg_start_time = video_timeline[video_seg_start_idx]
    video_seg_end_time = video_timeline[video_seg_end_idx]
    audio_seg_start_time = audio_timeline[audio_seg_start_idx]
    audio_seg_end_time = audio_timeline[audio_seg_end_idx]
    if audio_seg_start_time - video_seg_start_time >= 0 and audio_seg_end_time - video_seg_end_time >= 0:
        return True
    return False

def _word_counts(dict_list):
    summed_dict = defaultdict(int)
    for d in dict_list:
        k = d['url']
        v = d['match_score']
        summed_dict[k] += v
    return dict(summed_dict)

def _calculate_weighted_max_url(data, max_coef=0.35, total_coef=0.21, avg_coef=0.14, vadiff_coef=0.30):
    """
    计算给定字典列表中每个url的match_score的总和、平均值、最大值和平均va字段差值
    然后根据最大值的35%、总和的21%、平均值的14%和差值的30%计算一个综合得分，
    返回综合得分最高的url及其得分。

    参数:
    data (list): 包含字典的列表，每个字典有'url'、'match_score'、'v_start_idx'和'a_start_idx'键。

    返回:
    tuple: 包含综合得分最高的url及其得分的元组。
    """
    # 初始化一个字典，用于存储每个url的总match_score、出现次数、最大值和字段差值
    url_counts = defaultdict(lambda: {'total_score': 0, 'num_entries': 0, 'max_score': 0, 'total_diff': 0})

    # 遍历数据，累加每个url的match_score、记录出现次数、最大值和字段差值
    for entry in data:
        url = entry['url']
        match_score = entry['match_score']
        diff = abs(entry['v_start_idx'] - entry['a_start_idx'])
        url_counts[url]['total_score'] += match_score
        url_counts[url]['num_entries'] += 1
        url_counts[url]['total_diff'] += diff
        if match_score > url_counts[url]['max_score']:
            url_counts[url]['max_score'] = match_score

    # 计算每个url的平均match_score和综合得分
    weighted_scores = {}
    for url, counts in url_counts.items():
        total_score = counts['total_score']
        average_count = total_score / counts['num_entries']
        max_score = counts['max_score']
        avg_diff = counts['total_diff'] / counts['num_entries']
        weighted_score = max_score * max_coef + total_score * total_coef + average_count * avg_coef - avg_diff * vadiff_coef
        weighted_scores[url] = weighted_score

    # 找到综合得分最高的url
    max_weighted_url = max(weighted_scores, key=weighted_scores.get)
    max_weighted_value = weighted_scores[max_weighted_url]

    return max_weighted_url, max_weighted_value

def decide_best_match(matches):
    global fingerprint_db
    if not matches:
        return "Unknown"
    
    alive_matches = [m for m in matches if m['alive']]
    match len(alive_matches):
        case s if s == 0: # 目前，该分支不可能进入
            ...
            # global current_matches
            # for m in current_matches:
            #     m['alive'] = True
            # return None
            # matches = _word_counts(matches)
        case s if s == 1:
            global final_result
            final_result = alive_matches[0]['url']
            return final_result
        case _:
            ret_match = max(alive_matches, key=lambda x: x['match_score'])
            max_m_score = ret_match['match_score']
            if max_m_score == 1: # 只有第一块命中
                max_weighted_url, max_weighted_value = _calculate_weighted_max_url(alive_matches, max_coef=0, total_coef=0.3, avg_coef=0, vadiff_coef=0.7)
            else:
                max_m_list = [m for m in alive_matches if max_m_score-m['match_score'] <= 0.3]
                max_weighted_url, max_weighted_value = _calculate_weighted_max_url(max_m_list)
            return max_weighted_url

def match_chunk_with_fingerprint(chunk, fingerprint, v_start_idx, a_start_idx, flexible=False, max_video_blocks=5, max_audio_blocks=4):
    video_fp = fingerprint['video_fp']
    audio_fp = fingerprint['audio_fp']
    
    # max_video_blocks = 5  #1~5  78%
    # max_audio_blocks = 4  #0~4 OK

    v_start_len = max_video_blocks if flexible else 1 #默认考虑一个chunk偏移
    a_start_len = max_audio_blocks if flexible else 1

    for v_start in range(v_start_idx, v_start_idx+v_start_len):
        for a_start in range(a_start_idx, a_start_idx+a_start_len):
            for v_end in range(v_start + 1, min(len(video_fp) + 1, v_start + max_video_blocks + 1)): #保证长度max_video_blocks
                for a_end in range(a_start, min(len(audio_fp) + 1, a_start + max_audio_blocks + 1)): #保证长度max_audio_blocks
                    if (v_end - v_start) >= (a_end - a_start) and _check_time_constraint(fingerprint, v_start, v_end, a_start, a_end):
                        video_sum = sum(video_fp[v_start:v_end])
                        audio_sum = sum(audio_fp[a_start:a_end])
                        total_sum = video_sum + audio_sum
                        if -2000 <= chunk-total_sum < 4000:
                            # if  "_ia" in fingerprint['url'] and fingerprint['ID']=='30': 
                                # print(f"--{chunk}\nMatch {fingerprint['ID']}: {fingerprint['url'][-11:]}: \n {video_sum}={video_fp[v_start:v_end]}s_idx:{v_start} \n {audio_sum}={audio_fp[a_start:a_end]}s_idx:{a_start} \n {chunk-total_sum}")
                            return True, v_end, a_end, v_start==v_start_idx, a_start==a_start_idx # return:（boolean, 下一个chunk去匹配每一行时的v_start_idx, 同理a_start_idx）
    
    return False, -1, -1, False, False

def update_matches(new_chunk, fingerprint_db, i, flexible=False, is_restart=False):
    """
    期待的是：出现两两连续
    逻辑：先宽松匹配一次，再尝试连续匹配一次（严格），根据情况计分和加分。
    循环该逻辑，为了增加弹性，每次循环都独立。
    """
    global chunk_stream, current_matches
    chunk_stream.append(new_chunk)
    
    if flexible and not is_restart:
        if i == 0: # first chunk
            for cache_idx, fingerprint in enumerate(fingerprint_db):
                success, v_idx, a_idx, is_v_lx, is_a_lx = match_chunk_with_fingerprint(new_chunk, fingerprint, 0, 0, flexible=True)
                if success:
                    m_info = {
                        'ID': fingerprint['ID'], # 用于关联
                        'cache_idx': cache_idx, # 在列表fingerprint_db中的索引（内存中）
                        'url': fingerprint['url'], 
                        'match_score': 1,   # 累计匹配次数（得分），第一个chunk成功match的，初始化为1分，表示match了1次。
                        'v_start_idx': v_idx, # video下次从video指纹的该索引处开始尝试组合
                        'a_start_idx': a_idx, # audio下次从audio指纹的该索引处开始尝试组合
                        'is_v_lx': is_v_lx, # video是否连续匹配
                        'is_a_lx': is_a_lx, # audio是否连续匹配
                        'alive': True,
                        'reborn': 1,    # 复活卡数量
                        'last_matched': True,  # 上次是否match
                        'once_lx_times': 1  # 连续match成功的chunk数
                    }
                    current_matches.append(m_info)
        else:   # trailing chunks but flexible match
            for match in current_matches:
                fingerprint = fingerprint_db[match['cache_idx']]
                success, v_idx, a_idx, is_v_lx, is_a_lx = match_chunk_with_fingerprint(new_chunk, fingerprint, match['v_start_idx'], match['a_start_idx'], flexible=True) #int(i/2*3), int(i/2*2)
                if success:
                    # match时是否：没有跳过video/audio块，在上次match的位置接着又成功match
                    if is_a_lx:
                        match['once_lx_times'] += 0.5  # audio连续（理论上，（v or a）更容易连续的要放在第一个判断里）
                        match['match_score'] += 0.75 * match['once_lx_times']
                    if is_v_lx:
                        match['once_lx_times'] += 0.5  # video连续（目前，和audio连续加分相同）
                        match['match_score'] += 0.75 * match['once_lx_times']
                    if not (is_v_lx or is_a_lx):
                        match['once_lx_times'] = 1
                        match['match_score'] += 0.5  # match但在库里不连续
                    match['last_matched'] = True
                    match['v_start_idx'] = v_idx
                    match['a_start_idx'] = a_idx
                    match['is_v_lx'] = is_v_lx
                    match['is_a_lx'] = is_a_lx
                else:
                    match['last_matched'] = False
                    match['once_lx_times'] = 1
    else:
        for match in current_matches:
            if not match['alive']:
                continue
            if is_restart:
                flexible = True
            fingerprint = fingerprint_db[match['cache_idx']]
            success, v_idx, a_idx, is_v_lx, is_a_lx = match_chunk_with_fingerprint(new_chunk, fingerprint, match['v_start_idx'], match['a_start_idx'], flexible=flexible)
            if success:
                if match['last_matched']: #
                    match['once_lx_times'] += 1
                    match['match_score'] += 1 * match['once_lx_times']
                else:
                    match['once_lx_times'] = 1
                    match['match_score'] += 0.5
                match['last_matched'] = True
                match['v_start_idx'] = v_idx
                match['a_start_idx'] = a_idx
            else:
                match['last_matched'] = False
                match['once_lx_times'] = 1
    
    best_match = decide_best_match(current_matches)
    return best_match
    # if best_match:
    #     return best_match
    # else:
        # 全部都挂了，调整参数，重新启动
        # global cur_context
        # cur_context['cur_chunk'] = new_chunk
        # chunk_stream.clear()
        # print('***********restarted***********')
        # return main(cur_context['test_case'], is_restart=True)

def load_fingerprint_db(file_path):
    fingerprint_db = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['video_fp'] = list(map(int, row['video_fp'].split('/')[1:]))
            row['audio_fp'] = list(map(int, row['audio_fp'].split('/')[1:]))
            row['video_timeline'] = list(map(int, row['video_timeline'].split('/')[1:]))
            row['audio_timeline'] = list(map(int, row['audio_timeline'].split('/')[1:]))
            fingerprint_db.append(row)
    return fingerprint_db

def load_test_csv(file_path):
    test_csv = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            row['body_list'] = list(map(int, row['body_list'].split('/')[1:]))
            test_csv.append(row)
    return test_csv

def main(test_case, is_restart=False):
    # init
    global fingerprint_db, final_result
    global chunk_stream, current_matches
    global cur_context # for restart
    if not is_restart:
        fingerprint_db = load_fingerprint_db('final_yt_online1.csv')
        chunk_stream = []
        current_matches = []
        final_result = None
        cur_context = {'test_case':test_case}  # for restart
        start_case_idx = 0   # for restart
    else:
        # 重启时可以调整一些参数
        start_case_idx = list(test_case).index(cur_context['cur_chunk']) # for restart 这里默认从重启的chunk继续

    ret = None
    best_match_url_history = defaultdict(int)
    # update
    for i in range(start_case_idx, len(test_case)):
        new_chunk = test_case[i]
        if not final_result:
            best_match_url = update_matches(new_chunk, fingerprint_db, i, flexible=((i%2==0) or is_restart), is_restart=is_restart)
            print(f'Best match URL: {best_match_url}')
            ret = best_match_url
            best_match_url_history[ret] += 1
            if best_match_url_history[ret] >= 3: #连续3次返回同样的结果时，后续就不再进行匹配，直接返回该结果。
                final_result = ret
        else:
            print(f"=-=-=-=Final result: {final_result}=-=-=-=")
            ret = final_result
    return ret

if __name__ == '__main__':
    right_count = 0
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
            right_count += 1
        else:
            wrong_list.append({row['url']: test_case})
            print("#### Wrong ######")
            print(test_case)
            print("############# Wrong ##############")
        print(right_count) # right count
        print(len(test_csv)) # total count
        print(right_count/len(test_csv)) # rate
        print(wrong_list) # wrong case list

    # 测试额外偏移一个chunk，是否可以纠错
    for d in wrong_list:
        for url, case in d.items():
            case = case[1:]
            ret = main(case)
            print(url)
            print(case)
            if url not in ret:
                print('@'*20)
                print('---wrong---')
                print('@'*20)
    
    # 单独测试特殊用例
    # test_case = "1174869, 368771, 929679, 1221531, 788146, 776362, 726561, 795520, 777196, 1111483"
    # test_case = list(map(int, test_case.split(', ')[1:]))
    # test_case[0] -= 2000
    # main(test_case)