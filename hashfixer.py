import os
import psycopg2
import requests

with open('./shinofix.txt', 'r') as f:
    for line in f:
        if line.strip():
            conn = psycopg2.connect(
                host = config.database_host,
                dbname = config.database_dbname,
                user = config.database_user,
                password = config.database_password,
                port = 5432,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            with conn.cursor() as cursor:
                (_, correct_hash, old_path) = line.split(',', maxsplit=2)
                (old_hash, old_ext) = os.path.splitext(os.path.basename(path))
                old_path = '/' + path
                correct_path = join(correct_hash[0:2], correct_hash[2:4], correct_hash + old_ext)

                # Update the hash for the bad file entry.
                cursor.execute('UPDATE files SET hash = %(correct_hash)s WHERE hash = %(old_hash)s', {
                    'correct_hash': correct_hash,
                    'old_hash': old_hash
                })
                print(f"File entry fixed ({old_path} > {correct_path})")

                # Find posts that contain this file and replace in its data, just to be sure
                cursor.execute('''
                    WITH relationships as (
                        SELECT *
                        FROM file_post_relationships
                        WHERE file_id = (SELECT id FROM files WHERE hash = %(correct_hash)s)
                    )
                    SELECT *
                    FROM posts
                    WHERE
                        posts.service = relationships.service
                        AND posts."user" = relationships."user"
                        AND posts.id = relationships.post
                ''', {
                    'correct_hash': correct_hash
                })
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
                    post['embed'] = json.dumps(post['embed'])
                    post['file'] = json.dumps(post['file'])
                    for i in range(len(post['attachments'])):
                        post['attachments'][i] = json.dumps(post['attachments'][i])

                    # update
                    columns = post.keys()
                    data = ['%s'] * len(post.values())
                    data[list(columns).index('attachments')] = '%s::jsonb[]'  # attachments
                    query = 'UPDATE posts SET {updates} WHERE {conditions}'.format(
                        updates=','.join([f'"{column}" = {data[i]}' for (i, column) in enumerate(columns)]),
                        conditions='service = %s AND "user" = %s AND id = %s'
                    )
                    cursor.execute(query, list(post.values()) + list((post['service'], post['user'], post['id'],)))

                    print(f"{post['service']}/{post['user']}/{post['id']} fixed ({old_path} > {correct_path})")
                    requests.request('BAN', f"{config.ban_url}/{post['service']}/user/{post['user']}")
                    
                    # conn.commit()