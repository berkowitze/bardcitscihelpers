import os
import mimetypes
import magic

from google.cloud import storage

# the two functions to look at are download_image_from_filename and
# download_images. the two below variables are configuration.

# usage:
# 1. activate virtual environment: source venv/bin/activate
#
# 2. enter python shell: ipython
#
# 3. download images
#
# 3a) sample usage of download_image_from_filename:
#    from images import download_image_from_filename
#    download_image_from_filename('Citizen Science 2020/area/BCSW20-5', '.')

# 3b) sample usage of download_images:
#    from images import download_iamges
#    download_images('Citizen Science 2020/area/', 'CS20/area/')


default_bucket = 'citizensciencewater.appspot.com'
service_account = 'storage-reader-service-account.json'

if not os.path.exists(service_account):
    e = "To use this script, copy service account credentials into ./storage-reader-service-account.json"
    raise ValueError(e)

mime = magic.Magic(mime=True)

def _download_image(blob, dest_folder, ext=None, dest_filename=None):
    """
    (ideally for internal use)
    downloads the input file `blob` to `dest_folder`.

    if `dest_filename` is None, uses the Storage file's name as the filename
        Note: do not include extension in dest_filename (please! :) )
    if `ext` is None, uses the Cloud Storage mimetype to determine the extension,
        if there is a mimetype.
        if there is not, uses the guessed mimetype using magic package after
        file download
    
    returns the downloaded filepath

    """
    os.makedirs(dest_folder, exist_ok=True)

    if dest_filename is None:
        filename = os.path.basename(blob.name)
    else:
        filename = dest_filename

    rename_after_download = False
    if ext is not None:
        extension = ext
    else:
        if blob.content_type is None:
            rename_after_download = True
        else:
            extension = mimetypes.guess_extension(blob.content_type)
            if extension is None:
                rename_after_download = True

    if rename_after_download:
        orig_path = os.path.join(dest_folder, filename)
        blob.download_to_filename(orig_path)
        content_type = mime.from_file(orig_path)
        extension = mimetypes.guess_extension(content_type)
        if extension is None:
            basename = os.path.basename(blob.name)
            print(f'Could not determine extension for {basename!r} - naming it {filename!r}')
            return orig_path

        extension = extension.strip('.')
        destination = os.path.join(dest_folder, f'{filename}.{extension}')
        os.rename(orig_path, destination)
    else:
        extension = extension.strip('.')
        destination = os.path.join(dest_folder, f'{filename}.{extension}')
        blob.download_to_filename(destination)

    return destination

def download_image_from_filename(source, dest_folder, ext=None,
                                 dest_filename=None,
                                 bucket_name=default_bucket):
    """ 
    downloads the input file `source` from Cloud Bucket `bucket_name`
    to `dest_folder`.

    if `dest_filename` is None, uses the Storage file's name as the filename
        Note: do not include extension in dest_filename (please! :) )
    if `ext` is None, uses the Cloud Storage mimetype to determine the extension,
        if there is a mimetype.
        if there is not, uses the guessed mimetype using magic package after
        file download
    
    """
    client = storage.Client.from_service_account_json(service_account)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source)
    if not blob.exists():
        raise ValueError(f"File {source} does not exist")

    return _download_image(blob,
                           dest_folder,
                           ext=ext,
                           dest_filename=dest_filename,
                           bucket_name=bucket_name)


def download_images(prefix, destination, bucket_name=default_bucket):
    """ downloads all images in the input `bucket` starting with
    `prefix` to `dest_folder`. `prefix` must end with a '/'
    For example if prefix="a/", and the folder a/ contains the files:
        b/c
        b/d
        c/c
        c/d
        c/e
    Then will download the files into destination/b/c, destination/b/d, etc.
    (with extensions guessed as explained in download_image documentation)

    returns list of downloaded image paths
    """
    if not prefix.endswith('/'):
        raise ValueError('Can only download with prefix ending with "/"')

    client = storage.Client.from_service_account_json(service_account)
    blobs = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if not blob.name.endswith('/'):  # blobs ending with / are folders
            blobs.append(blob)

    paths = []
    for i, blob in enumerate(blobs):
        print(f'Downloaded {i + 1} / {len(blobs)}...')
        path, filename = os.path.split(blob.name)
        out_dir = os.path.join(destination, path[len(prefix):])
        path = _download_image(blob, out_dir)
        paths.append(path)

    return paths

