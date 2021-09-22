import multiprocessing
import config
import os
import time
import click
import psycopg2
from click_default_group import DefaultGroup

from src.migrators.files import migrate_file
from src.migrators.attachments import migrate_attachment
from src.migrators.inline import migrate_inline

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
                    "ctime" numeric NOT NULL,
                    "mtime" numeric NOT NULL
                );
            """
        )
        # indexing locks up db writes, disabled for now
        # cursor.execute("CREATE INDEX IF NOT EXISTS filepathidx ON posts((file->>'path'));")
        conn.commit()
        conn.close()
    else:
        print('(You are running `sdkdd` dry. Nothing will actually be updated/moved. Feel free to exit anytime.)\n')
    
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        scan_files_for_apply(pool, timestamp)
        scan_attachments_for_apply(pool, timestamp)
        scan_inline_for_apply(pool, timestamp)
        pool.close()
        pool.join()

    # conn = psycopg2.connect(
    #     host = config.database_host,
    #     dbname = config.database_dbname,
    #     user = config.database_user,
    #     password = config.database_password,
    #     port = 5432
    # )
    # cursor = conn.cursor()
    # cursor.execute("DROP INDEX IF EXISTS filepathidx;")
    # conn.commit()
    # conn.close()

@cli.command()
def revert():
    click.echo('revert')

if __name__ == '__main__':
    cli()