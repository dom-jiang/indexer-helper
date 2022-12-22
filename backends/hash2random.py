#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'Marco'

"""

"""

import sys
import math
import base58
import binascii

# the blockhash that used as random source
hashes = [
    'GczTaWKjEVbuCRMJGrympqTJtW2TjD9AFxtkbUQBSU44',
    '0x8cc2b24c321aec063c8cea612205b1d6f29c05b1e57c3e0ee89a6e098bd24c5c',
]


random_seq = []
rslt_seq = []


def fillin_randomsource(hash, pace):
    if hash.startswith('0x'):
        hash = hash[2:]
    else:  # assume it is a base-58 format blockhash
        hash = binascii.hexlify(base58.b58decode(hash))

    print('Fill in random source:', hash)

    for i in range(0, len(hash), pace):
        hex_str = hash[i:i+pace]
        # print(hex_str)
        random_seq.append(int(hex_str, 16))


def random(scope):

    print('Blockhash random Tool, candidates count is ', scope)

    if scope < 256:
        pace = 2
    elif scope < 65536:
        pace = 4
    else:
        pace = 8

    for hash in hashes:
        fillin_randomsource(hash, pace)

    
    print('We got %s random seeds' % len(random_seq))

    for random in random_seq:
        rank = math.floor(scope * random / 16**pace) + 1
        if rank not in rslt_seq:
            rslt_seq.append(rank)
        else:
            # print('%s already in rank sequence' % rank)
            pass

    print('result len:', len(rslt_seq))
    print(rslt_seq)
    print('End.')
    return rslt_seq

