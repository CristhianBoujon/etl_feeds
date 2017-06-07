import os
from feed_process.tools import compression

def preprocess(feed_source, callback, args = None):
    if(compression.is_compressed(feed_source)):
        extract_dir = extract_feed(feed_source)
        return preprocess(extract_dir, callback, args)
    
    elif(os.path.isfile(feed_source)):
        return [callback(feed_source, *args)]

    elif(os.path.isdir(feed_source)):
        feeds = (entry.path for entry in os.scandir(feed_source) if entry.is_file())
        
        return [callback(feed, *args) for feed in feeds]

    raise Exception("Invalid feed_source")


def extract_feed(compressed_file):
    
    path_file_name, extension = os.path.splitext(compressed_file)
    compression.extract(compressed_file, path_file_name)

    return path_file_name

if __name__ == '__main__':

    print(extract_feeds("feeds"))
