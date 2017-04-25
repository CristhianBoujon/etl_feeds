import shutil
import os


def _unpack_rarfile(filename, extract_dir = "."):
    """
        Unpack rar `filename` to `extract_dir`
    """
    import rarfile  # late import for breaking circular dependency
    rarobj = rarfile.RarFile(filename)

    try:
        rarobj.extractall(extract_dir)
    finally:
        rarobj.close()


def _unpack_gzfile(full_file_name, extract_dir = "."):
    """
        Unpack .gz `filename` to `extract_dir`
    """
    import gzip  # late import for breaking circular dependency

    if not os.path.exists(extract_dir):
        os.mkdir(extract_dir)

    unpacked_file_name = os.path.join(extract_dir, os.path.split(full_file_name)[1][:-3])

    with gzip.open(full_file_name, 'rb') as f_in, open(unpacked_file_name, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


shutil.register_unpack_format("rar", [".rar"], _unpack_rarfile) # Registering RAR
shutil.register_unpack_format("gz", [".gz"], _unpack_gzfile) # Registering GZ


def is_compressed(file):
    """ Check if file is compresses in zip, tar or rar format """
    filename, file_extension = os.path.splitext(file)
    return file_extension in [format for unpack_format in shutil.get_unpack_formats() for format in unpack_format[1]]


def get_compressed_files(dirname, filename_only = True):
    if(filename_only):
        return [entry.name for entry in os.scandir(dirname) if entry.is_file() and is_compressed(entry.name)]

    return [os.path.join(dirname, entry.name) for entry in os.scandir(dirname) if entry.is_file() and is_compressed(entry.name)]

        
def extract(path_file_name, extract_dir = None):
    shutil.unpack_archive(path_file_name, extract_dir)
