import multiprocessing
import config
import os
import time
import click
import psycopg2
import sqlite3
from click_default_group import DefaultGroup

from src.utils import remove_prefix
from src.migrators.files import migrate_file
from src.migrators.attachments import migrate_attachment
from src.migrators.inline import migrate_inline

def handle_process_error(e):
    print(e)

def scan_files_for_apply(pool, migration_id, dir = os.path.join(config.data_dir, 'files')):
    if not os.path.exists(os.path.join(config.data_dir, 'files')):
        print('"files" directory is missing, and will be skipped.')
        return
    
    with os.scandir(dir) as it:
        for entry in it:
            if entry.is_file():
                pool.apply_async(migrate_file, args=(entry.path, migration_id))
            else:
                scan_files_for_apply(pool, migration_id, dir = entry.path)

def scan_attachments_for_apply(pool, migration_id, dir = os.path.join(config.data_dir, 'attachments')):
    if not os.path.exists(os.path.join(config.data_dir, 'attachments')):
        print('"attachments" directory is missing, and will be skipped.')
        return
    
    with os.scandir(dir) as it:
        for entry in it:
            if entry.is_file():
                pool.apply_async(migrate_attachment, args=(entry.path, migration_id))
            else:
                scan_attachments_for_apply(pool, migration_id, dir = entry.path)

def scan_inline_for_apply(pool, migration_id, dir = os.path.join(config.data_dir, 'inline')):
    if not os.path.exists(os.path.join(config.data_dir, 'inline')):
        print('"inline" directory is missing, and will be skipped.')
        return
    
    with os.scandir(dir) as it:
        for entry in it:
            if entry.is_file():
                pool.apply_async(migrate_inline, args=(entry.path, migration_id))
            else:
                scan_inline_for_apply(pool, migration_id, dir = entry.path)

@click.group(cls=DefaultGroup, default='apply', default_if_no_args=True)
def cli():
    pass

@cli.command()
def apply():
    timestamp = int(time.time())
    if (not config.dry_run):
        conn = psycopg2.connect(
            host = config.database_host,
            dbname = config.database_dbname,
            user = config.database_user,
            password = config.database_password,
            port = 5432
        )
        cursor = conn.cursor()
        cursor.execute(
            f"""
                CREATE TABLE sdkdd_migration_{timestamp} (
                    "old_location" text NOT NULL,
                    "new_location" text NOT NULL,
                    "ctime" timestamp NOT NULL,
                    "mtime" timestamp NOT NULL
                );
            """
        )
        conn.commit()
        conn.close()
    else:
        print('(You are running `sdkdd` dry. Nothing will actually be updated/moved. Feel free to exit anytime.)\n')
    
    with multiprocessing.Pool(config.processes or multiprocessing.cpu_count()) as pool:
        if not config.sql_file:
            if (config.scan_files):
                scan_files_for_apply(pool, timestamp)
            if (config.scan_attachments):
                scan_attachments_for_apply(pool, timestamp)
            if (config.scan_inline):
                scan_inline_for_apply(pool, timestamp)
        else:
            sqlite_conn = sqlite3.connect(config.sql_file)
            posts_to_fix = sqlite_conn.execute('''
                SELECT
                    posts_dump.service,
                    posts_dump.user_id,
                    posts_dump.post_id,
                    posts_dump.file_path
                FROM posts_dump, hashdeep_to_migrate
                WHERE
                    posts_dump.file_path = hashdeep_to_migrate.path
                    AND posts_dump.file_path not in (
                      SELECT a.file_path
                      FROM posts_dump a, migration_log b
                      WHERE b.migration_original_path = a.file_path
                    )
            ''')
            for (post_service, post_user_id, post_id, file_location) in posts_to_fix:
                absolute_file_location = os.path.join(config.data_dir, remove_prefix(file_location, '/'))
                
                if file_location.startswith('/files/') and config.scan_files:
                    print('files:' + file_location)
                    pool.apply_async(migrate_file, args=(absolute_file_location, timestamp), kwds={
                        '_service': post_service,
                        '_user_id': post_user_id,
                        '_post_id': post_id
                    }, error_callback=handle_process_error)
                elif file_location.startswith('/attachments/') and config.scan_attachments:
                    print('attachments:' + file_location)
                    pool.apply_async(migrate_attachment, args=(absolute_file_location, timestamp), kwds={
                        '_service': post_service,
                        '_user_id': post_user_id,
                        '_post_id': post_id
                    }, error_callback=handle_process_error)
                elif file_location.startswith('/inline/') and config.scan_inline:
                    print('inline:' + file_location)
                    pool.apply_async(migrate_inline, args=(absolute_file_location, timestamp), kwds={
                        '_service': post_service,
                        '_user_id': post_user_id,
                        '_post_id': post_id
                    }, error_callback=handle_process_error)
                else:
                    print('nothing:' + file_location)

        pool.close()
        pool.join()

@cli.command()
def revert():
    click.echo('revert (unimplemented...)')

if __name__ == '__main__':
    cli()