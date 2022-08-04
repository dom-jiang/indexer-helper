import asyncio
import os

from near_lake_framework import LakeConfig, streamer, near_primitives


async def main():
    config = LakeConfig.testnet()
    config.start_block_height = 71288185
    config.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "AKIAQVRJFS5CWOBAV45U")
    config.aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "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")

    stream_handle, streamer_messages_queue = streamer(config)
    print("========111")
    while True:
        streamer_message = await streamer_messages_queue.get()
        print("========222")
        print(f"Block #{streamer_message.block.header.height} Shards: {len(streamer_message.shards)}")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
