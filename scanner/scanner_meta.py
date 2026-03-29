# import base64
# import hashlib

# def get_md5_b64(path: str) -> str:
#     md5 = hashlib.md5()
#     with open(path, 'rb') as f:
#         for chunk in iter(lambda: f.read(10_485_760), b''):
#             md5.update(chunk)
#     return base64.b64encode(md5.digest()).decode()


# def gather_metadata(db: ScannerDb, batch_size: int):
#     counter = Counter('gather_metadata', batch_size)

#     missing_meta_file_count = int(db.run_query_tuple('select count(*) from hashtab where md5_computed is null;')[0][0])
#     if missing_meta_file_count == 0:
#         print('Nothing to do - all files have had their metadata gathered already.')
#         return

#     print(f'Starting to gather metadata for ({missing_meta_file_count}) files...')

#     sql_select = f'''
#     select
#         h.rowid,
#         d.dirpath,
#         h.filename_no_ext,
#         e.ext
#     from hashtab h
#         join directory d using (dir_id)
#         join extension e using (ext_id)
#     where
#         h.md5_computed is null
#     limit {int(batch_size)};
#     '''

#     sql_update = '''update hashtab set md5_computed = ?, fsize_computed = ? where rowid = ?'''

#     while True:
#         rows = db.run_query_tuple(sql_select)
#         if not rows:
#             break

#         params = []
#         for rowid, dirpath, filename_no_ext, ext in rows:
#             fullpath = os.path.join(dirpath, f'{filename_no_ext}.{ext}')

#             # file could have been deleted since the gather_filesystem()'s last run
#             if not os.path.isfile(fullpath):
#                 md5_computed = None
#                 fsize_computed = None
#             else:
#                 md5_computed = get_md5_b64(fullpath)
#                 fsize_computed = os.path.getsize(fullpath)

#             params.append((
#                 md5_computed,
#                 fsize_computed,
#                 rowid,
#             ))

#         db.run_query_many(sql_update, params=params, commit=True)
#         counter(increment_by=len(rows))
#     print('\ngather_metadata, completed')

