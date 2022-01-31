import os
import psycopg2
import requests
import config
import json

from psycopg2.extras import RealDictCursor, Json
from src.utils import remove_prefix

with open('./shinofix.txt', 'r') as f:
    for line in f:
        if line.strip():
            conn = psycopg2.connect(
                host = config.database_host,
                dbname = config.database_dbname,
                user = config.database_user,
                password = config.database_password,
                port = 5432,
                cursor_factory=RealDictCursor
            )
            with conn.cursor() as cursor:
                (_, correct_hash, old_path) = line.strip().split(',', maxsplit=2)
                (old_hash, old_ext) = os.path.splitext(os.path.basename(old_path))
                old_path = '/' + old_path
                correct_path = os.path.join('/', correct_hash[0:2], correct_hash[2:4], correct_hash + old_ext)

                # Check if the correct hash already exists in the file table.
                cursor.execute('SELECT * FROM files WHERE hash = %s', (correct_hash,))
                existing_hash_record = cursor.fetchone()
                if not existing_hash_record:
                    # If the record for the correct hash doesn't exist, find and update the hash of the old one.
                    cursor.execute('UPDATE files SET hash = %s WHERE hash = %s', (correct_hash, old_hash))
                else:
                    relationship_tables = ['file_post_relationships', 'file_discord_message_relationships', 'file_server_relationships']
                    with conn.cursor() as cursor:
                        for table in relationship_tables:
                            # Delete a post's references that use the old hash when it also references the new one.
                            cursor.execute(f'''
                                DELETE FROM {table} old_r
                                USING {table} new_r
                                WHERE
                                    new_r.file_id = (SELECT id FROM files WHERE hash = %(old_hash)s)
                                    AND old_r.file_id = (SELECT id FROM files WHERE hash = %(correct_hash)s)
                            ''', {'old_hash': old_hash, 'correct_hash': correct_hash})
                            # If the record for the correct hash does exist, update post relations that reference the old one to use the correct hash, then delete old hash.
                            cursor.execute(f'''
                                UPDATE {table}
                                SET file_id = (SELECT id FROM files WHERE hash = %(correct_hash)s)
                                WHERE file_id = (SELECT id FROM files WHERE hash = %(old_hash)s)
                                ''',
                                {
                                    'old_hash': old_hash,
                                    'correct_hash': correct_hash
                                }
                            )
                        cursor.execute('DELETE FROM files WHERE hash = %s', (old_hash,))

                print(f"File entry fixed ({old_path} > {correct_path})")

                # Find posts that contain this file and replace in its data, just to be sure
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM file_post_relationships WHERE file_id = (SELECT id FROM files WHERE hash = %s)', (correct_hash,))
                    relationships_to_scrub = cursor.fetchall()
                    for relationship in relationships_to_scrub:
                        with conn.cursor() as cursor:
                            cursor.execute('''
                                SELECT *
                                FROM posts
                                WHERE
                                    posts.service = %s
                                    AND posts."user" = %s
                                    AND posts.id = %s
                            ''', (relationship['service'], relationship['user'], relationship['post']))
                            posts_to_scrub = cursor.fetchall()

                            for post in posts_to_scrub:
                                post['content'] = post['content'].replace('https://kemono.party' + old_path, correct_path)
                                post['content'] = post['content'].replace(old_path, correct_path)
                                if post['file'].get('path'):
                                    post['file']['path'] = post['file']['path'].replace('https://kemono.party' + old_path, correct_path)
                                    post['file']['path'] = post['file']['path'].replace(old_path, correct_path)
                                for (i, _) in enumerate(post['attachments']):
                                    if post['attachments'][i].get('path'): # not truely needed, but...
                                        post['attachments'][i]['path'] = post['attachments'][i]['path'].replace('https://kemono.party' + old_path, correct_path)
                                        post['attachments'][i]['path'] = post['attachments'][i]['path'].replace(old_path, correct_path)

                                # format
                                post['embed'] = Json(post['embed'])
                                post['file'] = Json(post['file'])
                                for i in range(len(post['attachments'])):
                                    post['attachments'][i] = Json(post['attachments'][i])

                                # update
                                columns = post.keys()
                                data = ['%s'] * len(post.values())
                                data[list(columns).index('attachments')] = '%s::jsonb[]'  # attachments
                                query = 'UPDATE posts SET {updates} WHERE {conditions}'.format(
                                    updates=','.join([f'"{column}" = {data[i]}' for (i, column) in enumerate(columns)]),
                                    conditions='service = %s AND "user" = %s AND id = %s'
                                )
                                cursor.execute(query, list(post.values()) + list((post['service'], post['user'], post['id'],)))

                                print(f"{post['service']}/{post['user']}/{post['id']} fixed ({old_path} -> {correct_path})")
                                if (not config.dry_run):
                                    requests.request('BAN', f"{config.ban_url}/{post['service']}/user/{post['user']}")

                # DICKSWORD
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM file_discord_message_relationships WHERE file_id = (SELECT id FROM files WHERE hash = %s)', (correct_hash,))
                    relationships_to_scrub = cursor.fetchall()
                    for relationship in relationships_to_scrub:
                        with conn.cursor() as cursor:
                            cursor.execute('''
                                SELECT *
                                FROM discord_posts
                                WHERE
                                    discord_posts.server = %s
                                    AND discord_posts.channel = %s
                                    AND discord_posts.id = %s
                            ''', (relationship['server'], relationship['channel'], relationship['id']))
                            messages_to_scrub = cursor.fetchall()

                            for message in messages_to_scrub:
                                for (i, _) in enumerate(message['attachments']):
                                    if message['attachments'][i].get('path'): # not truely needed, but...
                                        message['attachments'][i]['path'] = message['attachments'][i]['path'].replace('https://kemono.party' + old_path, correct_path)
                                        message['attachments'][i]['path'] = message['attachments'][i]['path'].replace(old_path, correct_path)

                                # format
                                message['author'] = Json(message['author'])
                                for i in range(len(message['mentions'])):
                                    message['mentions'][i] = Json(message['mentions'][i])
                                for i in range(len(message['attachments'])):
                                    message['attachments'][i] = Json(message['attachments'][i])
                                for i in range(len(message['embeds'])):
                                    message['embeds'][i] = Json(message['embeds'][i])

                                # update
                                columns = message.keys()
                                data = ['%s'] * len(message.values())
                                data[list(columns).index('mentions')] = '%s::jsonb[]'  # mentions
                                data[list(columns).index('attachments')] = '%s::jsonb[]'  # attachments
                                data[list(columns).index('embeds')] = '%s::jsonb[]'  # embeds
                                query = 'UPDATE discord_posts SET {updates} WHERE {conditions}'.format(
                                    updates=','.join([f'"{column}" = {data[i]}' for (i, column) in enumerate(columns)]),
                                    conditions='server = %s AND channel = %s AND id = %s'
                                )
                                cursor.execute(query, list(message.values()) + list((message['server'], message['channel'], message['id'],)))

                                print(f"discord: {message['server']}/{message['channel']}/{message['id']} fixed ({old_path} -> {correct_path})")
                    
                if (not config.dry_run):
                    conn.commit()
                else:
                    conn.rollback()

                if (not config.dry_run):
                    old_path_without_prefix = remove_prefix(old_path, '/')
                    correct_path_without_prefix = remove_prefix(correct_path, '/')

                    thumb_dir = config.thumb_dir or os.path.join(config.data_dir, 'thumbnail')
                    if os.path.isfile(os.path.join(thumb_dir, old_path_without_prefix)) and not os.path.isfile(os.path.join(thumb_dir, correct_path_without_prefix)):
                        os.makedirs(os.path.join(thumb_dir, correct_hash[0:2], correct_hash[2:4]), exist_ok=True)
                        os.rename(os.path.join(thumb_dir, old_path_without_prefix), os.path.join(thumb_dir, correct_path_without_prefix))
                    
                    if os.path.isfile(os.path.join(config.data_dir, old_path_without_prefix)) and not os.path.isfile(os.path.join(config.data_dir, correct_path_without_prefix)):
                        os.makedirs(os.path.join(config.data_dir, correct_hash[0:2], correct_hash[2:4]), exist_ok=True)
                        os.rename(os.path.join(config.data_dir, old_path_without_prefix), os.path.join(config.data_dir, correct_path_without_prefix))
