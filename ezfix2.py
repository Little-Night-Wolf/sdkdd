# creates or fixes file relationships
# everyone but internal kemono team can ignore, again
import os
import config
import psycopg2
import sqlite3
import json
import requests
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup

sqlite_conn = sqlite3.connect('/root/migration_prep/baseline/processing.db')

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
    psql_conn = psycopg2.connect(
        host = config.database_host,
        dbname = config.database_dbname,
        user = config.database_user,
        password = config.database_password,
        port = 5432,
        cursor_factory=RealDictCursor
    )

    new_file_hash = os.path.splitext(os.path.basename(new_file_location))
    old_filename = os.path.basename(old_file_location)

    cursor = psql_conn.cursor()
    cursor.execute("""
        INSERT INTO file_post_relationships (file_id, filename, service, \"user\", post, inline)
        VALUES ((SELECT id FROM files WHERE hash = %(hash)s), %(filename)s, %(service)s, %(user)s, %(post)s, %(inline)s) ON CONFLICT DO NOTHING
    """, {
        'hash': new_file_hash,
        'filename': old_filename,
        'service': post_service,
        'user': post_user_id,
        'post': post_id,
        'inline': 'inline' in old_filename
    })
    cursor.close()
    psql_conn.commit()
    psql_conn.close()