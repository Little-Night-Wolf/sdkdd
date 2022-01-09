# everyone but internal kemono team can ignore
import config
import psycopg2
import sqlite3
import json
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup

psql_conn = psycopg2.connect(
    host = config.database_host,
    dbname = config.database_dbname,
    user = config.database_user,
    password = config.database_password,
    port = 5432,
    cursor_factory=RealDictCursor
)

sqlite_conn = sqlite3.connect('/dev/shm/baseline/processing.db')

posts_to_fix = sqlite_conn.execute('''
    SELECT
        posts_dump.service,
        posts_dump.user_id,
        posts_dump.post_id,
        posts_dump.file_path,
        migration_log.migration_hashed_path
    FROM posts_dump, migration_log 
    WHERE migration_log.migration_original_path = posts_dump.file_path;
''')

for (post_service, post_user_id, post_id, old_file_location, new_file_location) in posts_to_fix:
    with psql_conn.cursor() as cursor:
        cursor.execute('SELECT * FROM posts WHERE service = %s AND "user" = %s AND id = %s', (post_service, post_user_id, post_id,))
        post_data = cursor.fetchone()

        # replace
        post_data['content'] = post_data['content'].replace('https://kemono.party' + old_file_location, new_file_location)
        post_data['content'] = post_data['content'].replace(old_file_location, new_file_location)
        if post_data['file']['path']:
            post_data['file']['path'] = post_data['file']['path'].replace('https://kemono.party' + old_file_location, new_file_location)
            post_data['file']['path'] = post_data['file']['path'].replace(old_file_location, new_file_location)
        for (i, _) in enumerate(post_data['attachments']):
            if post_data['attachments'][i]['path']:
                post_data['attachments'][i]['path'] = post_data['attachments'][i]['path'].replace('https://kemono.party' + old_file_location, new_file_location)
                post_data['attachments'][i]['path'] = post_data['attachments'][i]['path'].replace(old_file_location, new_file_location)

        # format
        post_data['embed'] = json.dumps(post_data['embed'])
        post_data['file'] = json.dumps(post_data['file'])
        for i in range(len(post_data['attachments'])):
            post_data['attachments'][i] = json.dumps(post_data['attachments'][i])

        # update
        columns = post_data.keys()
        data = ['%s'] * len(post_data.values())
        data[list(columns).index('attachments')] = '%s::jsonb[]'  # attachments
        query = 'UPDATE posts SET {updates} WHERE {conditions}'.format(
            updates=','.join([f'"{column}" = %s' for column in columns]),
            conditions='service = %s AND "user" = %s AND id = %s'
        )
        cursor.execute(query, list(post_data.values()) + list((post_service, post_user_id, post_id,)))

        psql_conn.commit()