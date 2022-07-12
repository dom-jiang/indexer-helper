import sys
sys.path.append('../')
from redis_provider import RedisProvider
from indexer_provider import get_proposal_id_hash


def proposal_id_hash_mapping(network_id):

    try:
        redis_conn = RedisProvider()
        redis_conn.begin_pipe()
        res = get_proposal_id_hash(network_id)
        for proposal in res:
            redis_conn.add_proposal_id_hash(network_id, proposal["proposal_id"], proposal["proposal_hash"])
        redis_conn.end_pipe()
        redis_conn.close()
    except Exception as e:
        print("Error occurred when get proposal id hash, Error is: ", e)


if __name__ == '__main__':
    proposal_id_hash_mapping("TESTNET")
