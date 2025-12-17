import json
import sys
sys.path.append('../')
from db_provider import query_multichain_lending_zcash_pending, update_multichain_lending_zcash_data
from zcash_utils import verify_mca_creation, verify_business, get_pubkey, get_mca_by_wallet, ZcashRPC


def sync_zcash_pending_data(network_id):
    rpc = ZcashRPC()
    zcash_pending_data_list = query_multichain_lending_zcash_pending(network_id, 60)
    for zcash_pending_data in zcash_pending_data_list:
        data_id = zcash_pending_data["id"]
        ma_id = zcash_pending_data["ma_id"]
        tx_id_list = rpc.getaddresstxids([zcash_pending_data["deposit_address"]])
        print("tx_id_list:", tx_id_list)
        for tx_id in tx_id_list:
            transaction_data = rpc.getrawtransaction(tx_id)
            if transaction_data is None:
                print("transaction_data is None, continue")
                continue
            hex_data = ""
            prevs = []
            error_msg = ""
            if "hex" in transaction_data:
                hex_data = transaction_data["hex"]
            if "vin" in transaction_data:
                vin_data = transaction_data["vin"]
                if vin_data is not None and len(vin_data) > 0:
                    vin = vin_data[0]
                    # vin_address = vin["address"]
                    # if not vin_address.startswith('t'):
                    #     error_msg = "Non transparent address transfer"
                    txid = vin["txid"]
                    vout_number = vin["vout"]
                    transaction_data_ret = rpc.getrawtransaction(txid)
                    if transaction_data_ret is None:
                        print("transaction_data_ret is None, continue")
                        continue
                    if "vout" in transaction_data_ret:
                        vout_list = transaction_data_ret["vout"]
                        if len(vout_list) >= vout_number:
                            vout_data = vout_list[vout_number]
                            prev = (str(vout_data["valueZat"]), vout_data["scriptPubKey"]["hex"])
                            prevs.append(prev)
                # else:
                #     error_msg = "Non transparent address transfer"
            if error_msg != "":
                update_multichain_lending_zcash_data(network_id, hex_data, json.dumps(prevs), data_id, "", "", "", tx_id, error_msg, status=3)
                continue
            if zcash_pending_data["type"] == 1:
                try:
                    verify_mca_creation_ret = verify_mca_creation(network_id, ma_id, hex_data, prevs,
                                                                  zcash_pending_data["deposit_uuid"])
                    if verify_mca_creation_ret is None:
                        print("verify_mca_creation_ret is None, continue")
                        continue
                    print("verify_mca_creation_ret:", verify_mca_creation_ret)
                except Exception as e:
                    error_msg = f"verify_mca_creation error: {str(e)}"
                    print(f"verify_mca_creation error for tx_id={tx_id}, data_id={data_id}: {error_msg}")
            elif zcash_pending_data["type"] == 2:
                try:
                    verify_business_ret = verify_business(network_id, zcash_pending_data["request_data"], hex_data, prevs,
                                                          zcash_pending_data["near_number"])
                    if verify_business_ret is None:
                        print(f"verify_business_ret is None for tx_id={tx_id}, data_id={data_id}, skip processing")
                        continue
                    print("verify_business_ret:", verify_business_ret)
                except Exception as e:
                    error_msg = f"verify_business error: {str(e)}"
                    print(f"verify_business error for tx_id={tx_id}, data_id={data_id}: {error_msg}")
            
            t_address = ""
            encryption_pubkey = ""
            mca_id = ""
            try:
                t_address, encryption_pubkey = get_pubkey(hex_data, prevs)
                mca_id = get_mca_by_wallet(network_id, encryption_pubkey)
            except Exception as e:
                if error_msg:
                    error_msg = f"{error_msg}; get_pubkey/get_mca_by_wallet error: {str(e)}"
                else:
                    error_msg = f"get_pubkey/get_mca_by_wallet error: {str(e)}"
                print(f"get_pubkey/get_mca_by_wallet error for tx_id={tx_id}, data_id={data_id}: {str(e)}")
            
            try:
                status = 3 if error_msg else 1
                update_multichain_lending_zcash_data(network_id, hex_data, json.dumps(prevs), data_id, t_address,
                                                     encryption_pubkey, mca_id, tx_id, error_msg, status=status)
                if error_msg:
                    print(f"Updated data_id={data_id} with error status=3")
                else:
                    print(f"Successfully updated data_id={data_id} with status=1, tx_id={tx_id}")
            except Exception as e:
                print(f"Failed to update data_id={data_id}: {str(e)}")


if __name__ == '__main__':
    sync_zcash_pending_data("MAINNET")
