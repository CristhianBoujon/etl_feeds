import os
from tools import compression

def massive_preprocess(feeds_source, callback):
    
    if(type(feeds_source) == str and os.path.isdir(feeds_source)):
        feeds = (os.path.join(feeds_source, entry.name) for entry in os.scandir(feeds_source) if entry.is_file())

    elif(type(feeds_source) == str and not os.path.isdir(feeds_source)):
        raise Exception("directory is not exists")
    else:
        feeds = feeds_source

    for feed in feeds:
        callback(feed)

def preprocess(feed_source, callback, args = None):
    if(compression.is_compressed(feed_source)):
        result = extract_feed(feed_source)

        # A compressed feed could be more than one file, so we re-call to preprocess passing the folder
        # where was unpaked the feed
        # @TODO: We need to log somewhere if extract feed fails 
        if result['ok']:
            return preprocess(result['data'][0], callback, args)
        else:
            return result    
    
    elif(os.path.isdir(feed_source)):
        feeds = (os.path.join(feed_source, entry.name) for entry in os.scandir(feed_source) if entry.is_file())
        
        for feed in feeds:
            callback(feed, *args)
    
    else:
        return callback(feed_source, *args)

def extract_feeds(dirname):
    result = {'ok': [], 'errors': []}
    for compressed_file in compression.get_compressed_files(dirname, filename_only = False):
        
        res = extract_feed(compressed_file)

        result[res[status]].append(res[data])

    return result

def extract_feed(compressed_file):
    
    path_file_name, extension = os.path.splitext(compressed_file)
    try:
        compression.extract(compressed_file, path_file_name)
        ok = True
        data = (path_file_name, "Extracted OK")

    except Exception as e:
        ok = False
        data = (compressed_file, str(e))

    return {'ok': ok, 'data': data}


if __name__ == '__main__':

    print(extract_feeds("feeds"))
