import os
import time


def scan_directory(scan_dir):
    if os.path.exists(scan_dir):
        return [entry for entry in os.scandir(scan_dir) if entry.is_file()]


def rotate_directory(rotate_dir):
    if os.path.exists(rotate_dir):
        if os.path.isdir(rotate_dir):
            os.rename(rotate_dir,
                      os.path.join("f:\\temp", "temp_" + time.strftime("%Y%m%d-%H%M%S")))
    os.mkdir(rotate_dir)


def check_directory(check_dir):
    if not os.path.exists(check_dir):
        os.mkdir(check_dir)


