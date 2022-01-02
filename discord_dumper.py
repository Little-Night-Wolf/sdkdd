import config
import psycopg2
import sys
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup

conn = psycopg2.connect(
    host = config.database_host,
    dbname = config.database_dbname,
    user = config.database_user,
    password = config.database_password,
    port = 5432,
    cursor_factory=RealDictCursor
)
cursor = conn.cursor()
cursor.execute("SELECT * FROM discord_posts")

sys.stdout.write('\t'.join(['discord_server_id', 'discord_channel_id', 'discord_message_id', 'file_path']) + '\n')

for post in cursor:
    for attachment in post['attachments']:
        if attachment.get('path'):
            attachment_path = attachment['path'].replace('https://kemono.party', '')
            sys.stdout.write('\t'.join([post['server'], post['channel'], post['id'], attachment_path]) + '\n')