import math
import numpy as np
from typing import List

def distance(a:List[float],b:List[float])->float:
    """
    Computes the distance between two vectors.
    Args:
        a (List[float]): The first vector.
        b (List[float]): The second vector.
    Returns:
        float: The distance between the two vectors.
    """
    return math.sqrt(sum([(x-y)**2 for x,y in zip(a,b)])) 

print(distance([1,2,3],[4,5,6]))