import subprocess
import pyautogui
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime
import time,os,gc
from tqdm import tqdm

CUR_ABS_PATH = os.path.dirname(__file__)
DEFAULT_DOWN_PATH = os.path.join(CUR_ABS_PATH, 'traffic_result')

# 创建Chrome选项
chrome_options =  webdriver.ChromeOptions()
chrome_options.add_argument("--auto-open-devtools-for-tabs")  # 打开开发者模式
chrome_options.add_argument(f"load-extension={CUR_ABS_PATH}/fjdmkanbdloodhegphphhklnjfngoffa/1.5_0")  # 加载插件，修改默认清晰度
# chrome_options.add_argument(f"--user-data-dir={os.path.join(CUR_ABS_PATH, 'chrome_user_data')}")  # 必要時使用，由於緩存，容易觸發廣告
chrome_options.add_experimental_option("prefs", {
	'download.default_directory': DEFAULT_DOWN_PATH,  # 设置默认下载路径
    # 'devtools.preferences.panel-selectedTab': '"network"', # 设置默认devtools-panel 理论正确但是doesn't work
    "devtools.preferences.currentDockState": '"left"',
})
chrome_options.add_argument("--lang=en-US")

def start_chrome():
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_position(0, 0) # left top
    driver.set_window_size(width=987, height=800) # 
    # driver.maximize_window()
    print(driver.get_window_size())
    return driver

def execution_time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time
    return wrapper

@execution_time_decorator
def switchto_network_and_filter():
    try:
        pyautogui.click(pyautogui.locateCenterOnScreen('play.png', confidence=0.85))
    except Exception as e:
        print("#####video-auto-played#####")
    try:
        time.sleep(3) # sometimes 播放器會變形一下 需要等一等再定位
        pyautogui.click(pyautogui.locateCenterOnScreen('1.png', confidence=0.8))  # 更多
        time.sleep(0.2)
        pyautogui.click(pyautogui.locateCenterOnScreen('2.png', confidence=0.8))  # 点击Network标签
    except Exception as e:
        pyautogui.click(pyautogui.locateCenterOnScreen('2_.png', confidence=0.8))  # 点击Network标签
    finally:
        try:
            pyautogui.click(pyautogui.locateCenterOnScreen('clear_filter.png', confidence=0.8))
        except: pass
        finally:
            time.sleep(0.5)
            pyautogui.write('videoplayback', interval=0.1)  # 输入筛选条件
            pyautogui.hotkey('enter')

def _get_duration_text(driver):
    return driver.find_element(By.CSS_SELECTOR, '#movie_player > div.ytp-chrome-bottom > div.ytp-chrome-controls > div.ytp-left-controls > div.ytp-time-display.notranslate > span:nth-child(2) > span.ytp-time-duration') or driver.find_element(By.XPATH, '//*[@id="movie_player"]/div[28]/div[2]/div[1]/div[1]/span[2]/span[3]') or driver.find_element(By.XPATH, '//span[starts-with(@class,"ytp-time-duration")]/text()')

@execution_time_decorator
def get_wait_second():
    while not (duration_text:=_get_duration_text(driver).text):
        ...
    h_m_s = duration_text.split(":")
    if len(h_m_s) == 2:
        h_m_s.insert(0, 0)
    h_m_s = list(map(int, h_m_s))
    duration = h_m_s[-1] + h_m_s[-2]*60 + h_m_s[-3]*3600
    return int(duration)-10

def export_har_file(dir_name, file_name):
    pyautogui.click(pyautogui.locateCenterOnScreen('save_har.png', confidence=0.8))  
    time.sleep(2)
    file_fullname = os.path.join(CUR_ABS_PATH, "har_result", dir_name, file_name)
    if not os.path.exists(file_fullname):
        pyautogui.write(os.path.join(dir_name, file_name)) 
        time.sleep(0.5)
        pyautogui.press('enter')  # 保存文件
        time.sleep(3)

def subprocess_stdio_has_kw(process_pointer, keywords, try_lines=-1, decoding='utf8'):
    '''
        实时监测子进程是否在stdio中打印了指定关键字，包括stdout、stderr。
        @try_lines 尝试监测的输出行数，-1表示无限轮询
    '''
    import sys
    while try_lines == -1 or try_lines > 0:
        sys.stderr.flush()
        sys.stdout.flush()
        out = process_pointer.stderr or process_pointer.stdout
        if out and keywords in out.readline().decode(decoding):
            return True
        
        if try_lines > 0:
            try_lines -= 1
    
    return False

def capture_traffic(tshark_path, interface, pcap_path, pcap_name, loopcount=1):
    if not os.path.exists(pcap_path):
        os.makedirs(pcap_path)

    pcap_fullname = os.path.join(pcap_path, pcap_name)

    for _ in range(loopcount):
        tshark_cmd = [tshark_path, "-F", "pcap", "-i", interface, "-w", pcap_fullname]
        tshark_process = subprocess.Popen(tshark_cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, executable=tshark_path)
        if subprocess_stdio_has_kw(tshark_process, "Capturing"):
            yield
        tshark_process.kill()
        time.sleep(1)
        print(f'-Catpured: {pcap_fullname}')

if __name__ == '__main__':
    ###########修改默认分辨率 1.here  2.content.js; ENUS###########
    RESOLUTION = '720'
    URL_LIST = [
        # "https://www.youtube.com/watch?v=pzKerr0JIPA",  #just for test
        # "https://www.youtube.com/watch?v=pzKerr0JIPA", #just for test
        # "https://www.youtube.com/watch?v=pzKerr0JIPA", #just for test
        # "https://www.youtube.com/watch?v=pzKerr0JIPA", #just for test
        # "https://www.youtube.com/watch?v=pzKerr0JIPA", #just for test
        # "https://www.youtube.com/watch?v=KD_ijUQ-YF4",
        # "https://www.youtube.com/watch?v=_iaEOlewwNQ",
        # "https://www.youtube.com/watch?v=rSR_9CJqJ60",
        # "https://www.youtube.com/watch?v=2xyo2sqmb1k",
        # "https://www.youtube.com/watch?v=Y6o_TZnQr5E",
        # "https://www.youtube.com/watch?v=QEXj7idTMV4",
        # "https://www.youtube.com/watch?v=WOD1HkxL9tw",
        # "https://www.youtube.com/watch?v=YRkBxHDjEG8",
        # "https://www.youtube.com/watch?v=uyhmxgG-tA8",
        # "https://www.youtube.com/watch?v=JKRKiSoBLEU",

        "https://www.youtube.com/watch?v=_iaEOlewwNQ",
        # "https://www.youtube.com/watch?v=2xyo2sqmb1k",
        # "https://www.youtube.com/watch?v=KD_ijUQ-YF4",
        # "https://www.youtube.com/watch?v=rSR_9CJqJ60",
        # "https://www.youtube.com/watch?v=YRkBxHDjEG8"
        # "https://www.youtube.com/watch?v=smOFZ1Jc9LY", "https://www.youtube.com/watch?v=byfhtEyW4gk", "https://www.youtube.com/watch?v=hGrkov9RSqo", "https://www.youtube.com/watch?v=vigCVdnZ38A", "https://www.youtube.com/watch?v=OHhkOzBKaNo", "https://www.youtube.com/watch?v=yHER8nrjTv4", "https://www.youtube.com/watch?v=hoU7x4o6ZhU", "https://www.youtube.com/watch?v=TnJr2jxhzxk", "https://www.youtube.com/watch?v=LcyzC_ONU1Q", "https://www.youtube.com/watch?v=EnCNSv-HCPY", "https://www.youtube.com/watch?v=Ul_UFVnFj1k", "https://www.youtube.com/watch?v=ZW7L99Fw-PE", "https://www.youtube.com/watch?v=1dr8S65l_T8", "https://www.youtube.com/watch?v=uLfxMgN5azs", "https://www.youtube.com/watch?v=jtIWoTPZMe8", "https://www.youtube.com/watch?v=6KRl0lg7XmQ", "https://www.youtube.com/watch?v=JuReXlgM9_c", "https://www.youtube.com/watch?v=HpFj_XhcXPI", "https://www.youtube.com/watch?v=qzje1C9pP30", "https://www.youtube.com/watch?v=tXwd1HCt9Dw", "https://www.youtube.com/watch?v=IUDOn1FfRTc", "https://www.youtube.com/watch?v=TB3JeOjuBws", "https://www.youtube.com/watch?v=uSToFto360g", "https://www.youtube.com/watch?v=A_aAyHNzpTw", "https://www.youtube.com/watch?v=viiD4t0O1B8", "https://www.youtube.com/watch?v=47ntBElzaWk", "https://www.youtube.com/watch?v=mvvp2rYgbNo", "https://www.youtube.com/watch?v=rpPjRoAGt5k", "https://www.youtube.com/watch?v=X8jISrQmgvA", "https://www.youtube.com/watch?v=YofFQsYHqBw"
    ]
    TSHARK_PATH = r'C:\Program Files\Wireshark\tshark.exe'
    INTERFACE = 'localnet'
    ########################################################

    # main
    for i, url in enumerate(tqdm(URL_LIST, desc="总进度")):
        # for _ in range(8):
        driver = start_chrome()

        _dir = os.path.join(CUR_ABS_PATH, "traffic_result", url[-11:])
        if not os.path.exists(_dir):
            os.mkdir(_dir)

        traffic_basefilename = f"{url[-11:]}--{RESOLUTION}--{datetime.now():%Y%m%d%H%M%S}"

        ensure_tshark_gen = capture_traffic(TSHARK_PATH, INTERFACE, os.path.join(DEFAULT_DOWN_PATH, f'{url[-11:]}--PCAP'), f'{traffic_basefilename}.pcap') # yield
        next(ensure_tshark_gen)  # ensure tshark started
        print('Tshark Started')

        driver.get(url) # 阻塞，直到网页加载完成deo

        _, waste_second1 = switchto_network_and_filter() # sleep around 1s

        # wait_second, waste_second2 = get_wait_second() # 全采
        wait_second, waste_second2 = 360, 0  # 固定采集前6分钟
        if (sleep_time:=wait_second - waste_second1 - waste_second2) > 0:
            time.sleep(sleep_time)

        next(ensure_tshark_gen, 'capture done') # kill tshark
        export_har_file(dir_name=url[-11:], file_name=f"{traffic_basefilename}.har") # cost much time cuz of sleep

        # ensure the save is successful (the save process took much time)
        time.sleep(wait_second / 60 / 2 + 3) # 10min -> 5s  60min -> 30s

        driver.quit()
        del driver # chrome实例内存占用较大，反复创建最好还是手动释放内存，避免内存溢出引起故障
        gc.collect()
        