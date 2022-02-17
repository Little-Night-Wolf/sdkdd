# everyone but internal kemono team can ignore
import requests
import psycopg2
import sqlite3
import config
import json
import os
from src.utils import replace_file_from_discord_message
from psycopg2.extras import RealDictCursor
from bs4 import BeautifulSoup

sqlite_conn = sqlite3.connect('/root/migration_prep/baseline/processing.db')

messages_to_fix = sqlite_conn.execute('''
    SELECT
        discord_posts_dump.discord_server_id,
        discord_posts_dump.discord_channel_id,
        discord_posts_dump.discord_message_id,
        discord_posts_dump.file_path,
        migration_log.migration_hashed_path
    FROM
        discord_posts_dump,
        migration_log
    WHERE
        migration_log.migration_original_path = discord_posts_dump.file_path
        AND discord_posts_dump.file_path NOT NULL
        AND migration_log.migration_original_path NOT NULL;
''')

for (message_service, message_channel_id, message_id, old_file_location, new_file_location) in messages_to_fix:
    psql_conn = psycopg2.connect(
        host=config.database_host,
        dbname=config.database_dbname,
        user=config.database_user,
        password=config.database_password,
        port=5432,
        cursor_factory=RealDictCursor
    )

    (updated_rows, message) = replace_file_from_discord_message(
        psql_conn,
        old_file_location,
        new_file_location,
        server_id=message_service,
        channel_id=message_channel_id,
        message_id=message_id
    )

    new_file_hash = os.path.splitext(os.path.basename(new_file_location))[0]
    old_filename = os.path.basename(old_file_location)
    with psql_conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO file_discord_message_relationships (file_id, filename, server, channel, id)
            VALUES ((SELECT id FROM files WHERE hash = %(hash)s), %(filename)s, %(server)s, %(channel)s, %(id)s) ON CONFLICT DO NOTHING
        """, {
            'hash': new_file_hash,
            'filename': old_filename,
            'server': message['server'],
            'channel': message['channel'],
            'id': message['id']
        })

    psql_conn.commit()
    psql_conn.close()
