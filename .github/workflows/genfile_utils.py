import os
import multiprocessing as mp
import hashlib
import re
import time
import numpy as np
import threading


def is_absolute_path(path):
    return path and path[0] == '/'

def get_absolute_path(path):
    assert(len(path) > 0)
    if path[0] == '/':
        return path
    return f"{os.getcwd().rstrip('/')}/{path}"

def get_upload_path_lstrip(path_lstrip, src):
    if path_lstrip == "":
        if not is_absolute_path(src):
            path_lstrip = os.getcwd()
    else:
        if not is_absolute_path(path_lstrip):
            path_lstrip = get_absolute_path(path_lstrip)
    return path_lstrip.rstrip('/')

def chunk_list_by_num_chunks(the_list, num_chunks):
    """
    Evenly shards @the_list into @num_chunks chunks
    >>> the_list, num_chunks
    ([1, 2, 3, 4, 5], 2)
    >>> chunk_list_by_num_chunks(the_list, num_chunks)
    [[1, 3, 5], [2, 4]]
    """
    chunks = [[] for _ in range(num_chunks)]
    for i, o in enumerate(the_list):
        chunks[i % num_chunks].append(o)
    return chunks

def create_chunks(the_list, num_chunks=None):
    if len(the_list) == 0:
        return []
    if num_chunks is None:
        num_chunks = min(len(the_list), mp.cpu_count())
    num_chunks = min(len(the_list), num_chunks)
    res = np.array_split(the_list, num_chunks)
    res = [list(x) for x in res]
    return res

def chunk_list_by_chunk_size(the_list, chunk_size):
    """
    Shards @the_list into equal size chunks of size @chunk_size
    >>> len(the_list), chunk_size
    (18, 5)
    >>> [len(x) for x in chunk_list_by_chunk_size(the_list, chunk_size)]
    [5, 5, 5, 3]
    """
    sliced_list = []
    for i in range(0, len(the_list), chunk_size):
        sliced_list.append(the_list[i:i+chunk_size])
    return sliced_list

def get_num_cores(num_cores, multi_core):
    if num_cores is not None:
        return num_cores
    if not multi_core:
        return 1
    return mp.cpu_count()

def standardize_path(path):
    if path == '.' or path == './':
        path = f'{os.getcwd()}/'
    
    if path[0] == '~':
        path = os.path.expanduser(path)

    if path[-1] == '.':
        path = path[:-1]

    path = get_absolute_path(path)
    return path


def get_md5_hash(filename):
    with open(filename, 'rb') as f:
        data = f.read()    
        md5 = hashlib.md5(data).hexdigest()
    return md5

def get_md5_hashes(filenames):
    md5s = []
    for filename in filenames:
        md5s.append(get_md5_hash(filename))
    return md5s

def get_download_path_lstrip(path_lstrip, src, is_regex, obj_list):
    if not is_regex:
        if len(obj_list) == 1 and obj_list[0]['name'] == src: # src is a file
            return os.path.dirname(src)
        return src
    return path_lstrip

def get_filenames_under_folder(path, recursive=False):
    res = []
    for root, _, files in os.walk(path):
        for file_ in files:
            res.append(os.path.join(root, file_))
        if not recursive:
            break
    return res

def get_folders_under_folder(path, recursive=False):
    res = []
    for root, folders, _ in os.walk(path):
        for folder in folders:
            res.append(os.path.join(root, folder))
        if not recursive:
            break
    return res

def delete_empty_folders_under_folder(folder, recursive):
    folders = get_folders_under_folder(folder, recursive)
    for folder in folders[::-1]:
        if not os.listdir(folder):
            os.rmdir(folder)

def delete_files_under_folder(folder, recursive, exclude_set):
    filenames = get_filenames_under_folder(folder, recursive)
    for filename in filenames:
        if filename in exclude_set: continue
        os.remove(filename)


def get_filenames_matching_regex(regex, path_lstrip):
    folder_to_list = '/' if not path_lstrip else path_lstrip
    filenames = get_filenames_under_folder(folder_to_list, True)
    filenames = [f for f in filenames if bool(re.match(regex, f))]
    return filenames

def mp_wrapper(args):
    func, kwargs = args
    return func(**kwargs)

def do_mp(func, arg_list):
    pool = mp.Pool(len(arg_list))
    result = pool.map(mp_wrapper, [(func, arg) for arg in arg_list])
    pool.close()
    pool.join()
    return result

def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        return {"data": result, "time": round(time.time() - ts, 3)}
    return timed

def join(s1, s2):
    s1 = s1.rstrip('/')
    s2 = s2.lstrip('/')
    return f"{s1}/{s2}"


def read_file_lines_into_list(filename):
    with open(filename) as f:
        content = f.readlines()
    return [x.strip() for x in content]

def exec_async(funcs, wait=True):
    if funcs and not isinstance(funcs[0], tuple):
        funcs = list(zip(funcs, [{}] * len(funcs)))

    ts = []
    for f, kwargs in funcs:
        t = threading.Thread(target=f, kwargs=kwargs)
        ts.append(t)
        t.start()

    if not wait:
        return
        
    for t in ts:
        t.join()

def do_exponential_backoff(num_tries=1):
    def decorator(func):
        def method(*args, **kw):
            for i in range(num_tries):
                time.sleep(pow(2, i) - 1)
                try:
                    return func(*args, **kw)
                except Exception as e:
                    if i == num_tries - 1:
                        raise Exception(e)
        return method
    return 
