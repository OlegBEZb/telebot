from credentials import api_id as API_ID
from credentials import api_hash as API_HASH
from interactive_telegram_client import InteractiveTelegramClient

import asyncio
# Create a global variable to hold the loop we will be using
loop = asyncio.get_event_loop()


if __name__ == '__main__':
    client = InteractiveTelegramClient('interactive', API_ID, API_HASH)
    loop.run_until_complete(client.run())
