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
cursor.execute("SELECT * FROM posts")

sys.stdout.write('\t'.join(['service', 'user_id', 'post_id', 'file_path']) + '\n')

for post in cursor:
    if post['file'].get('path'):
        file_path = post['file']['path'].replace('https://kemono.party', '')
        sys.stdout.write('\t'.join([post['service'], post['user'], post['id'], file_path]) + '\n')
    
    for attachment in post['attachments']:
        if attachment.get('path'):
            attachment_path = attachment['path'].replace('https://kemono.party', '')
            sys.stdout.write('\t'.join([post['service'], post['user'], post['id'], attachment_path]) + '\n')
    
    for inline in BeautifulSoup(post['content'], 'html.parser').select('img[src^="https://kemono.party/"]'):
        inline_path = image['src'].replace('https://kemono.party', '')
        sys.stdout.write('\t'.join([post['service'], post['user'], post['id'], inline_path]) + '\n')

    for inline in BeautifulSoup(post['content'], 'html.parser').select('img[src^="/"]'):
        inline_path = inline['src'].replace('https://kemono.party', '')
        sys.stdout.write('\t'.join([post['service'], post['user'], post['id'], inline_path]) + '\n')