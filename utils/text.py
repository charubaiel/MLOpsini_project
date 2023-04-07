
from navec import Navec
from scipy.spatial.distance import cosine
import numpy as np 
import requests
import pathlib
from utils.utils import ROOT



VECTOR_FILEPATH = f'{ROOT}/utils/navec_500k.tar'

if ~pathlib.Path(VECTOR_FILEPATH).exists():

    _vec_pack = requests.get('https://storage.yandexcloud.net/natasha-navec/packs/navec_hudlit_v1_12B_500K_300d_100q.tar').content

    with open(VECTOR_FILEPATH,'wb') as buff:
        buff.write(_vec_pack)
    del _vec_pack


navec = Navec.load(VECTOR_FILEPATH)

def check_similarity(text_a,text_b):

    vec_a = np.mean((navec.get(word) for word in text_a if navec.get(word) is not None),axis=0)
    vec_b = np.mean((navec.get(word) for word in text_b if navec.get(word) is not None),axis=0)

    return vec_a,vec_b
