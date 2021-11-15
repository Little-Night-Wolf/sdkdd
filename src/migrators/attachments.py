import hashlib
import os
from ..utils import trace_unhandled_exceptions, remove_suffix, remove_prefix
import config
import psycopg2
import pathlib
import datetime
import magic
import re
import mimetypes
import requests
from psycopg2.extras import RealDictCursor
from retry import retry

@trace_unhandled_exceptions
@retry(tries=5)
def migrate_attachment(path, migration_id):
    # check if the file is special (symlink, hardlink, empty) and return if so
    if os.path.islink(path) or os.path.getsize(path) == 0 or os.path.ismount(path) or path.endswith('.temp'):
        return
    
    if config.ignore_temp_files and path.endswith('.temp'):
        return
    
    file_ext = os.path.splitext(path)[1]
    web_path = path.replace(remove_suffix(config.data_dir, '/'), '')
    service = None
    post_id = None
    user_id = None
    with open(path, 'rb') as f:
        # get hash and filename
        file_hash_raw = hashlib.sha256()
        for chunk in iter(lambda: f.read(8192), b''):
            file_hash_raw.update(chunk)
        file_hash = file_hash_raw.hexdigest()
        new_filename = os.path.join('/', file_hash[0:2], file_hash[2:4], file_hash)
        
        mime = magic.from_file(path, mime=True)
        if (config.fix_extensions):
            file_ext = mimetypes.guess_extension(mime or 'application/octet-stream', strict=False)
            new_filename = new_filename + (re.sub('^.jpe$', '.jpg', file_ext or '.bin') if config.fix_jpe else file_ext or '.bin')
        else:
            new_filename = new_filename + (re.sub('^.jpe$', '.jpg', file_ext or '.bin') if config.fix_jpe else file_ext or '.bin')
        
        fname = pathlib.Path(path)
        mtime = datetime.datetime.fromtimestamp(fname.stat().st_mtime)
        ctime = datetime.datetime.fromtimestamp(fname.stat().st_ctime)

        conn = psycopg2.connect(
            host = config.database_host,
            dbname = config.database_dbname,
            user = config.database_user,
            password = config.database_password,
            port = 5432,
            cursor_factory=RealDictCursor
        )

        # log to file tracking table
        if (not config.dry_run):
            cursor = conn.cursor()
            cursor.execute("INSERT INTO files (hash, mtime, ctime, mime, ext) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hash) DO UPDATE SET hash = EXCLUDED.hash RETURNING id", (file_hash, mtime, ctime, mime, file_ext))
            file_id = cursor.fetchone()['id']
        
        updated_rows = 0
        step = 1
        # update "attachment" path references in db, using different strategies to speed the operation up
        # strat 1: attempt to derive the user and post id from the original path
        if (len(web_path.split('/')) >= 4):
            guessed_post_id = web_path.split('/')[-2]
            guessed_user_id = web_path.split('/')[-3]
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT index as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE id = %s AND \"user\" = %s
                      AND (attachment ->> 'path' = %s OR attachment ->> 'path' = %s OR attachment ->> 'path' = %s)
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.id = selected_attachment.id AND posts."user" = selected_attachment."user" AND posts.service = selected_attachment.service 
                    RETURNING posts.id, posts.service, posts."user"
                """,
                (guessed_post_id, guessed_user_id, web_path, 'https://kemono.party' + web_path, new_filename, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            post = cursor.fetchone()
            if (post):
                service = post['service']
                user_id = post['user']
                post_id = post['id']
            cursor.close()
        
        # strat 2: attempt to scope out posts archived up to 1 hour after the file was modified (kemono data should almost never change)
        if updated_rows == 0:
            step = 2
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT index as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE added >= %s AND added < %s
                      AND (attachment ->> 'path' = %s OR attachment ->> 'path' = %s OR attachment ->> 'path' = %s)
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.id = selected_attachment.id AND posts."user" = selected_attachment."user" AND posts.service = selected_attachment.service
                    RETURNING posts.id, posts.service, posts."user"
                """,
                (mtime, mtime + datetime.timedelta(hours=1), web_path, 'https://kemono.party' + web_path, new_filename, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            post = cursor.fetchone()
            if (post):
                service = post['service']
                user_id = post['user']
                post_id = post['id']
            cursor.close()

        # optimizations didn't work, scan the entire table
        if updated_rows == 0:
            step = 3
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT index as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE (attachment ->> 'path' = %s OR attachment ->> 'path' = %s OR attachment ->> 'path' = %s)
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.id = selected_attachment.id AND posts."user" = selected_attachment."user" AND posts.service = selected_attachment.service
                    RETURNING posts.id, posts.service, posts."user"
                """,
                (web_path, 'https://kemono.party' + web_path, new_filename, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            post = cursor.fetchone()
            if (post):
                service = post['service']
                user_id = post['user']
                post_id = post['id']
            cursor.close()
        
        # log file post relationship (not discord)
        if (not config.dry_run and updated_rows > 0 and service and user_id and post_id):
            cursor = conn.cursor()
            cursor.execute("INSERT INTO file_post_relationships (file_id, filename, service, \"user\", post, inline) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (file_id, os.path.basename(path), service, user_id, post_id, False))

        # Discord
        server_id = None
        channel_id = None
        message_id = None
        if (updated_rows == 0 and len(web_path.split('/')) >= 4):
            step = 4
            guessed_message_id = web_path.split('/')[-2]
            guessed_server_id = web_path.split('/')[-3]
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT index as json_index, server, channel, id FROM discord_posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE id = %s AND server = %s
                      AND (attachment ->> 'path' = %s OR attachment ->> 'path' = %s OR attachment ->> 'path' = %s)
                  )
                  UPDATE discord_posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE discord_posts.id = selected_attachment.id AND discord_posts.channel = selected_attachment.channel AND discord_posts.server = selected_attachment.server
                    RETURNING discord_posts.server, discord_posts.channel, discord_posts.id
                """,
                (guessed_message_id, guessed_server_id, web_path, 'https://kemono.party' + web_path, new_filename, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            message = cursor.fetchone()
            if (message):
                server_id = message['server']
                channel_id = message['channel']
                message_id = message['id']
            cursor.close()

        if (updated_rows == 0):
            step = 5
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT index as json_index, server, channel, id FROM discord_posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE (attachment ->> 'path' = %s OR attachment ->> 'path' = %s OR attachment ->> 'path' = %s)
                  )
                  UPDATE discord_posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE discord_posts.id = selected_attachment.id AND discord_posts.channel = selected_attachment.channel AND discord_posts.server = selected_attachment.server
                    RETURNING discord_posts.server, discord_posts.channel, discord_posts.id
                """,
                (web_path, 'https://kemono.party' + web_path, new_filename, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            message = cursor.fetchone()
            if (message):
                server_id = message['server']
                channel_id = message['channel']
                message_id = message['id']
            cursor.close()
        
        # log file post relationship (discord)
        if (not config.dry_run and updated_rows > 0 and server_id and channel_id and message_id):
            cursor = conn.cursor()
            cursor.execute("INSERT INTO file_discord_message_relationships (file_id, filename, server, channel, id) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (file_id, os.path.basename(path), server_id, channel_id, message_id))
        
        # log to sdkdd_migration_{migration_id}
        if (not config.dry_run):
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO sdkdd_migration_{migration_id} (old_location, new_location, ctime, mtime) VALUES (%s, %s, %s, %s)", (web_path, new_filename, mtime, ctime))
            cursor.close()

        # commit db
        if (config.dry_run):
            conn.rollback()
        else:
            conn.commit()
        
        if (not config.dry_run):
            new_filename_without_prefix = remove_prefix(new_filename, '/')
            web_path_without_prefix = remove_prefix(web_path, '/')
            # move to hashy location, do nothing if something is already there
            # move thumbnail to hashy location
            thumb_dir = config.thumb_dir or os.path.join(config.data_dir, 'thumbnail')
            if os.path.isfile(os.path.join(thumb_dir, web_path_without_prefix)) and not os.path.isfile(os.path.join(thumb_dir, new_filename_without_prefix)):
                os.makedirs(os.path.join(thumb_dir, file_hash[0:2], file_hash[2:4]), exist_ok=True)
                os.rename(os.path.join(thumb_dir, web_path_without_prefix), os.path.join(thumb_dir, new_filename_without_prefix))
            
            if os.path.isfile(path) and not os.path.isfile(os.path.join(config.data_dir, new_filename_without_prefix)):
                os.makedirs(os.path.join(config.data_dir, file_hash[0:2], file_hash[2:4]), exist_ok=True)
                os.rename(path, os.path.join(config.data_dir, new_filename_without_prefix))

        if (not config.dry_run and config.ban_url and service and user_id):
            requests.request('BAN', f"{config.ban_url}/{service}/user/" + user_id)
        
        conn.close()

        # done!
        if (service and user_id and post_id):
            print(f'{web_path} -> {new_filename} ({updated_rows} database entries updated; {service}/{user_id}/{post_id}, found at step {step})')
        elif (server_id and channel_id and message_id):
            print(f'{web_path} -> {new_filename} ({updated_rows} database entries updated; discord/{server_id}/{channel_id}/{message_id}, found at step {step})')
        else:
            print(f'{web_path} -> {new_filename} ({updated_rows} database entries updated; no post/messages found)')