import pathlib

import numpy as np
import requests
from navec import Navec
from scipy.spatial.distance import cosine

ROOT = pathlib.Path(__file__).parent
VECTOR_FILEPATH = f'{ROOT}/navec_500k.tar'

if ~pathlib.Path(VECTOR_FILEPATH).exists():

    _vec_pack = requests.get(
        'https://storage.yandexcloud.net/natasha-navec/packs/navec_hudlit_v1_12B_500K_300d_100q.tar'
    ).content

    with open(VECTOR_FILEPATH, 'wb') as buff:
        buff.write(_vec_pack)
    del _vec_pack

navec = Navec.load(VECTOR_FILEPATH)


def check_similarity(text_a: str, text_b: str) -> float:

    vec_a = get_sentence_vector(text_a)
    vec_b = get_sentence_vector(text_b)
    try:
        return cosine(vec_a, vec_b)
    except Exception as e:
        print(e)
        return None


def get_sentence_vector(text: str) -> np.ndarray:

    sentence_vec = np.mean([
        navec.get(word)
        for word in text.lower().split() if navec.get(word) is not None
    ],
                           axis=0)

    return sentence_vec
