import hashlib
import os
from ..utils import trace_unhandled_exceptions
import config
import psycopg2
import pathlib
import datetime

@trace_unhandled_exceptions
def migrate_file(path: str, migration_id):
    # check if the file is special (symlink, hardlink, empty) and return if so
    if os.path.islink(path) or os.path.getsize(path) == 0 or os.path.ismount(path):
        return
    
    file_ext = os.path.splitext(path)[1]
    web_path = path.replace(config.data_dir.removesuffix('/'), '')
    with open(path, 'rb') as f:
        # get hash and filename
        file_hash_raw = hashlib.sha256()
        for chunk in iter(lambda: f.read(8192), b''):
            file_hash_raw.update(chunk)
        file_hash = file_hash_raw.hexdigest()
        new_filename = os.path.join('/', file_hash[0], file_hash[1:3], file_hash + file_ext)
        
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
        # update "file" path references in db, using different strategies to speed the operation up
        # strat 1: attempt to derive the user and post id from the original path
        if (len(web_path.split('/')) >= 4):
            guessed_post_id = web_path.split('/')[-2]
            guessed_user_id = web_path.split('/')[-3]
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE posts SET file = jsonb_set(file, '{path}', %s, false) WHERE id = %s AND \"user\" = %s AND file ->> 'path' = %s;",
                (f'"{new_filename}"', guessed_post_id, guessed_user_id, web_path)
            )
            updated_rows = cursor.rowcount
            cursor.close()
        
        # strat 2: attempt to scope out posts archived up to 1 hour after the file was modified (kemono data should almost never change)
        if updated_rows == 0:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE posts SET file = jsonb_set(file, '{path}', %s, false) WHERE added >= %s AND added < %s AND file ->> 'path' = %s;",
                (f'"{new_filename}"', mtime, mtime + datetime.timedelta(hours=1), web_path)
            )
            updated_rows = cursor.rowcount
            cursor.close()

        # optimizations didn't work, scan the entire table
        if updated_rows == 0:
            cursor = conn.cursor()
            cursor.execute("UPDATE posts SET file = jsonb_set(file, '{path}', %s, false) WHERE file ->> 'path' = %s;", (f'"{new_filename}"', web_path))
            updated_rows = cursor.rowcount
            cursor.close()

        # log to sdkdd_migration_{migration_id} (see sdkdd.py for schema)
        # log to general file tracking table (schema: serial id, hash, filename, locally stored path, remotely stored path?, last known mtime, last known ctime, extension, mimetype, service, user, post, contributor_user?)
        if (not config.dry_run):
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO sdkdd_migration_{migration_id} (old_location, new_location, ctime, mtime) VALUES (%s, %s, %s, %s)", (path, new_filename, mtime.timestamp(), ctime.timestamp()))
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