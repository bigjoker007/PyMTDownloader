import requests
from threading import Lock
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import os
import json


class DownloadManager:
    def __init__(self, tasks=5, header=None, folder=None, threads=5,
                 chunk='100KB', retry=3, trace=True, resume=False):
        """
        :param tasks: 同时下载的资源数量
        :param header: 资源请求头
        :param folder: 保存资源的文件夹，默认为项目文件夹
        :param threads: 每个下载任务使用的线程数量
        :param chunk: 每个线程下载的文件块大小
        :param retry: 请求重试次数
        :param trace: 打印下载过程日志
        :param resume: 启用断点续传功能
        """
        self.pool = ThreadPoolExecutor(max_workers=tasks)
        self.queue = []
        self.header = header
        self.folder = folder
        self.threads = threads
        self.chunk = chunk
        self.retry = retry
        self.trace = trace
        self.resume = resume
        self.schedule = list()
        self.result = list()

    # 直接开始下载任务
    def add(self, url, file=None, header=None, folder=None, threads=None,
            chunk=None, retry=None, trace=None, resume=False):
        params = {"url": url,
                  "file": file,
                  "header": header if header else self.header,
                  "folder": folder if folder else self.folder,
                  "threads": threads if threads else self.threads,
                  "chunk": chunk if chunk else self.chunk,
                  "retry": retry if retry is not None else self.retry,
                  "trace": trace if trace else self.trace,
                  "resume": resume if resume else self.resume}
        future = self.pool.submit(self.run, params)
        future.add_done_callback(self.callback)
        self.queue.append(future)

    # 预约下载任务
    def reverse(self, url, file=None, header=None, folder=None, threads=None,
                chunk=None, retry=None, trace=None, resume=False):
        params = {"url": url,
                  "file": file,
                  "header": header if header else self.header,
                  "folder": folder if folder else self.folder,
                  "threads": threads if threads else self.threads,
                  "chunk": chunk if chunk else self.chunk,
                  "retry": retry if retry is not None else self.retry,
                  "trace": trace if trace in [False, True] else self.trace,
                  "resume": resume if resume else self.resume}
        self.schedule.append(params)

    # 启动预约任务/等待所有任务结束
    def done(self):
        for s in self.schedule:
            future = self.pool.submit(self.run, s)
            future.add_done_callback(self.callback)
            self.queue.append(future)
        wait(self.queue, return_when=ALL_COMPLETED)
        return [x.result() for x in self.queue]

    def callback(self, future):
        pass

    @staticmethod
    def run(params):
        download = Downloader(**params)
        return download.download()


class Downloader:
    def __init__(self, url, file=None, header=None, folder=None, threads=5,
                 chunk='100KB', retry=3, trace=True, resume=False):
        """
        :param url: 资源下载路径
        :param header: 资源请求头
        :param folder: 保存资源的文件夹，默认为项目文件夹
        :param threads: 下载任务使用的线程池大小
        :param chunk: 每个线程下载的文件块大小
        :param retry: 请求重试次数
        :param file: 保存的文件名，默认为资源文件名
        :param trace: 打印下载过程日志
        :param resume: 启用断点续传功能
        """
        self.url = url
        self.header = header
        self.threads = threads
        self.chunk = chunk
        self.retry = retry if retry > 0 else 1
        self.trace = trace
        self.resume = resume
        self.file = file
        self.folder = folder
        self.task = dict()
        self.lock = Lock()
        self.file_size = 0
        self.error = ''
        self.pool = None
        if not file:
            file = url.split('/')[-1]
        if folder:
            os.makedirs(folder, exist_ok=True)
            self.file_path = f'{folder}\\{file}'
        else:
            self.file_path = file
        self.tg_path = self.file_path + '.tg'
        self.params = {"url": self.url,
                       "file": self.file,
                       "header": self.header,
                       "folder": self.folder,
                       "threads": self.threads,
                       "chunk": self.chunk,
                       "retry": self.retry,
                       "trace": self.trace,
                       "resume": self.resume}

    def download(self):
        if not self.url:
            raise Exception('Downloader launched without resource url')
        # 初始化任务
        self._init_task()
        # self._print(self.task)
        # 初始化失败
        if self.error:
            self._print(self.url, self.error)
            return {"url": self.url, "path": self.file_path,
                    "result": 2 if self.error in ['文件已存在'] else 0,
                    "params": self.params}
        # 运行任务
        self._launch_task()
        # 校验文件完整性
        return self._integrity_check()

    # 初始化任务
    def _init_task(self):
        # 检查任务相关文件是否存在
        file_exist = os.path.exists(self.file_path)
        tg_exist = os.path.exists(self.tg_path)
        # 有下载文件无任务文件，判断为不相干文件或已下载完成
        if file_exist and not tg_exist:
            self.error = '文件已存在'
            return
        # 获取文件大小
        if not self._get_file_size():
            self.error = '头文件获取失败'
            return
        # 如果文件不存在，创建个空文件并设置任务计划
        if not file_exist:
            temp = open(self.file_path, 'w')
            temp.close()
            self._create_tg()
            return
        # 如果文件存在，下载进度文件也存在，加载文件进度
        if file_exist and tg_exist and self.resume:
            with open(self.tg_path, 'r') as r:
                self.task = json.loads(r.read())
            return

    # 获取文件长度
    def _get_file_size(self):
        for retry in range(self.retry):
            # self._print(requests.head(self.resource_url).headers["Content-Length"])
            session = self._create_session()
            try:
                head = session.head(self.url, timeout=(10, 30))
                if head.status_code != 200:
                    continue
            except Exception as e:
                continue
            # self._print(head.headers)
            try:
                self.file_size = int(head.headers["Content-Length"])
                # self._print(f'file_size: {self.file_size}')
                return 1
            except Exception as e:
                self._print(e)
        return 0

    # 生成任务组
    def _create_tg(self):
        # 生成块大小
        chunk_length = self._resolve_chunk_length()
        # 必要信息
        self.task = {"file_size": self.file_size, "chunk_size": chunk_length, "chunks": list()}
        # 如果不存在，根据文件长度和块大小创建队列
        start = 0
        end = - 1
        while True:
            if end + chunk_length > self.file_size - 1:
                end = self.file_size - 1
                self.task["chunks"].append([start, end])
                break
            else:
                end += chunk_length
                self.task["chunks"].append([start, end])
            start += chunk_length
        # 生成tg文件
        if self.resume:
            with open(self.tg_path, 'w') as w:
                w.write(json.dumps(self.task))

    # 将块大小转换为字节数
    def _resolve_chunk_length(self):
        chunk_length = 0
        # 设置块大小时，将设置的值转换为块大小
        if 'KB' in self.chunk:
            chunk_length = int(self.chunk.replace('KB', '')) * 1024
        elif 'MB' in self.chunk:
            chunk_length = int(self.chunk.replace('MB', '')) * 1024 * 1024
        # 未设置块大小时，根据启动的线程数确定块大小
        elif not self.chunk:
            if not self.file_size % self.threads:
                chunk_length = self.file_size // self.threads
            else:
                chunk_length = self.file_size // self.threads + 1
        return chunk_length

    # 执行任务
    def _launch_task(self):
        self.pool = ThreadPoolExecutor(max_workers=self.threads)
        tasks = []
        self._print(f'{self.url} 开始下载')
        for chunk in self.task["chunks"]:
            tasks.append(self.pool.submit(self._download, chunk))
        wait(tasks, return_when=ALL_COMPLETED)

    # 下载模块
    def _download(self, chunk):
        # 设置块起点与终点
        start = chunk[0]
        end = chunk[1]
        # self._print(f'开始下载 {start}-{end}')
        # 组装header
        header = self._set_header(start, end)
        # 下载文件块
        session = self._create_session()
        for i in range(self.retry):
            try:
                res = session.get(self.url, headers=header, timeout=(30, 120))
                # 比较下载的文件块大小与chunk大小，不相同则重试
                if len(res.content) != end - start + 1:
                    continue
                # 更新任务状态
                self.task["chunks"].remove(chunk)
                # 写文件前加锁
                self.lock.acquire()
                # 写下载文件
                with open(self.file_path, 'rb+') as f:
                    f.seek(start)
                    f.write(res.content)
                # 写tg文件
                if self.resume:
                    with open(self.tg_path, 'w') as g:
                        g.write(json.dumps(self.task))
                # 写完解锁
                self.lock.release()
                break
            except requests.exceptions.ConnectionError:
                pass

    # 拼装header
    def _set_header(self, start, end):
        header = self.header
        if header:
            header["Range"] = f"bytes={start}-{end}"
        else:
            header = {"Range": f"bytes={start}-{end}"}
        return header

    # https支持
    def _create_session(self):
        session = requests.Session()
        session.mount('http://', HTTPAdapter(max_retries=self.retry))
        session.mount('https://', HTTPAdapter(max_retries=self.retry))
        return session

    # 下载完成后文件校验
    def _integrity_check(self):
        # 对比资源文件大小与已下载的文件大小
        if os.path.getsize(self.file_path) == self.file_size:
            if self.resume:
                os.remove(self.tg_path)
            self._print(self.url, '下载完成')
            return {"url": self.url, "path": self.file_path, "result": 1, "params": self.params}
        if self.trace:
            self._print(self.url, '下载失败，文件缺损')
        return {"url": self.url,
                "path": self.file_path,
                "result": 0,
                "params": self.params}

    # 打印判断
    def _print(self, *message):
        if self.trace:
            print(' '.join(message))


if __name__ == '__main__':
    name = '[彦馬ヒロユキ] BBQDQN (COMIC LO 2022年2月号)'
    save_to = r'F:\Hacg\comic\unidentified\2020' + f'\\{name}'
    link = 'https://i.nhentai.net/galleries/2139574'
    dm = DownloadManager(folder=save_to, threads=3)
    for i in range(30):
        dm.add(f'{link}/{i + 1}.jpg')
    dm.done()
