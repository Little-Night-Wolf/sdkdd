import hashlib
import os
from ..utils import trace_unhandled_exceptions, remove_suffix
import config
import psycopg2
import pathlib
import datetime

@trace_unhandled_exceptions
def migrate_attachment(path, migration_id):
    # check if the file is special (symlink, hardlink, empty) and return if so
    if os.path.islink(path) or os.path.getsize(path) == 0 or os.path.ismount(path) or path.endswith('.temp'):
        return
    
    if config.ignore_temp_files and path.endswith('.temp'):
        return
    
    file_ext = os.path.splitext(path)[1]
    web_path = path.replace(remove_suffix(config.data_dir, '/'), '')
    with open(path, 'rb') as f:
        # get hash and filename
        file_hash_raw = hashlib.sha256()
        for chunk in iter(lambda: f.read(8192), b''):
            file_hash_raw.update(chunk)
        file_hash = file_hash_raw.hexdigest()
        new_filename = os.path.join('/', file_hash[0:2], file_hash[2:4], file_hash + file_ext)
        
        if (config.fix_jpe):
            new_filename = new_filename.replace('.jpe', '.jpg')
        
        fname = pathlib.Path(path)
        mtime = datetime.datetime.fromtimestamp(fname.stat().st_mtime)
        ctime = datetime.datetime.fromtimestamp(fname.stat().st_ctime)

        conn = psycopg2.connect(
            host = config.database_host,
            dbname = config.database_dbname,
            user = config.database_user,
            password = config.database_password,
            port = 5432
        )

        updated_rows = 0
        # update "attachment" path references in db, using different strategies to speed the operation up
        # strat 1: attempt to derive the user and post id from the original path
        if (len(web_path.split('/')) >= 4):
            guessed_post_id = web_path.split('/')[-2]
            guessed_user_id = web_path.split('/')[-3]
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT (index-1) as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE id = %s AND \"user\" = %s
                      AND attachment ->> 'path' = %s
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.service = selected_attachment.service AND posts."user" = selected_attachment."user" AND posts.id = selected_attachment.id
                """,
                (guessed_post_id, guessed_user_id, web_path, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            cursor.close()
        
        # strat 2: attempt to scope out posts archived up to 1 hour after the file was modified (kemono data should almost never change)
        if updated_rows == 0:
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT (index-1) as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE added >= %s AND added < %s
                      AND attachment ->> 'path' = %s
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.service = selected_attachment.service AND posts."user" = selected_attachment."user" AND posts.id = selected_attachment.id
                """,
                (mtime, mtime + datetime.timedelta(hours=1), web_path, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            cursor.close()

        # optimizations didn't work, scan the entire table
        if updated_rows == 0:
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT (index-1) as json_index, service, "user", id FROM posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE attachment ->> 'path' = %s
                  )
                  UPDATE posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE posts.service = selected_attachment.service AND posts."user" = selected_attachment."user" AND posts.id = selected_attachment.id
                """,
                (web_path, f'"{new_filename}"',)
            )
            updated_rows = cursor.rowcount
            cursor.close()

        # Discord
        if (updated_rows == 0 and len(web_path.split('/')) >= 4):
            guessed_message_id = web_path.split('/')[-2]
            guessed_server_id = web_path.split('/')[-3]
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT (index-1) as json_index, server, channel, id FROM discord_posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE id = %s AND server = %s
                      AND attachment ->> 'path' = %s
                  )
                  UPDATE discord_posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE discord_posts.server = selected_attachment.server AND discord_posts.channel = selected_attachment.channel AND discord_posts.id = selected_attachment.id
                """,
                (guessed_message_id, guessed_server_id, web_path, f'"{new_filename}"')
            )
            updated_rows = cursor.rowcount
            cursor.close()

        if (updated_rows == 0):
            cursor = conn.cursor()
            cursor.execute(
                """
                  WITH selected_attachment as (
                    SELECT (index-1) as json_index, server, channel, id FROM discord_posts, jsonb_array_elements(to_jsonb(attachments)) WITH ORDINALITY arr(attachment, index)
                    WHERE attachment ->> 'path' = %s
                  )
                  UPDATE discord_posts
                    SET attachments[selected_attachment.json_index] = jsonb_set(attachments[selected_attachment.json_index], '{path}', %s, false)
                    FROM selected_attachment
                    WHERE discord_posts.server = selected_attachment.server AND discord_posts.channel = selected_attachment.channel AND discord_posts.id = selected_attachment.id
                """,
                (web_path, f'"{new_filename}"',)
            )
            updated_rows = cursor.rowcount
            cursor.close()
        
        # log to sdkdd_migration_{migration_id} (see sdkdd.py for schema)
        # log to general file tracking table (schema: serial id, hash, filename, locally stored path, remotely stored path?, last known mtime, last known ctime, extension, mimetype, service, user, post, contributor_user?)
        if (not config.dry_run):
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO sdkdd_migration_{migration_id} (old_location, new_location, ctime, mtime) VALUES (%s, %s, %s, %s)", (path, new_filename, mtime, ctime))
            cursor.close()

        # commit db
        if (config.dry_run):
            conn.rollback()
        else:
            conn.commit()
        
        if (not config.dry_run):
            # move to hashy location, do nothing if something is already there
            if os.path.isfile(path) and not os.path.isfile(os.path.join(config.data_dir, new_filename)):
                os.rename(path, os.path.join(config.data_dir, new_filename))

            # move thumbnail to hashy location
            thumb_dir = config.thumb_dir or os.path.join(config.data_dir, 'thumbnail')
            if os.path.isfile(os.path.join(thumb_dir, web_path)) and not os.path.isfile(os.path.join(thumb_dir, new_filename)):
                os.rename(os.path.join(thumb_dir, web_path), os.path.join(thumb_dir, new_filename))

        conn.close()

        # done!
        print(f'{web_path} -> {new_filename} ({updated_rows} database entries updated)')