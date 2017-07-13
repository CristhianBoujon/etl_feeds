import string
import random

def chunk_list(l, n):
    """ 
    return n-sized chunks list from l. 
        e.g: 
        >>> chunk_list([1,2,3,4], 1) ->  [[1], [2], [3], [4]]        
        >>> chunk_list([1,2,3,4], 2) ->  [[1, 2], [3, 4]]
        >>> chunk_list([1,2,3,4], 3) -> [[1, 2, 3], [4]]
        >>> chunk_list([1,2,3,4], 4) -> [[1, 2, 3, 4]]
        >>> chunk_list([1,2,3,4], 5) -> [[1, 2, 3, 4]]


    """
    return [l[i: i + n] for i in range(0, len(l), n)]

def id_generator(size = 10, chars = string.hexdigits):
    return ''.join(random.choice(chars) for _ in range(size))