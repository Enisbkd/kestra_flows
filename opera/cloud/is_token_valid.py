import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

paris_tz = ZoneInfo("Europe/Paris")

cached = os.environ.get('CACHED_VALUE', '').strip()

if not cached:
    now = datetime.now(paris_tz)
    print(f'No cached token found at {now.isoformat()} (Paris time), a new one will be fetched.')
    print('::{"outputs":{"needs_refresh":true}}::')
else:
    data = json.loads(cached)

    expiry = datetime.fromisoformat(data['expiry_timestamp'])
    now = datetime.now(paris_tz)

    print(f'Current time (Paris): {now.isoformat()}')
    print(f'Token expiry time (Paris): {expiry.isoformat()}')

    needs_refresh = now > expiry

    if needs_refresh:
        print('Cached token is expired or expiring soon.')
    else:
        print('Cached token is still valid, reusing it.')

    print('::{"outputs":{"needs_refresh":' + str(needs_refresh).lower() + '}}::')