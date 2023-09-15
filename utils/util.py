import os


def create_directory(dir_name) -> None:
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
