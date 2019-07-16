import mimetypes
import os.path
import time
from datetime import datetime
from time import mktime

from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import ContentFile
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible

from storages.utils import setting

try:
    import azure  # noqa
except ImportError:
    raise ImproperlyConfigured(
        "Could not load Azure bindings. "
        "See https://github.com/WindowsAzure/azure-sdk-for-python")

try:
    # azure-storage 0.20.0
    from azure.storage.blob.blobservice import BlobService
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    from azure.storage import BlobService
    from azure import WindowsAzureMissingResourceError as AzureMissingResourceHttpError


def clean_name(name):
    return os.path.normpath(name).replace("\\", "/")


@deconstructible
class AzureStorage(Storage):
    account_name = setting("AZURE_ACCOUNT_NAME")
    account_key = setting("AZURE_ACCOUNT_KEY")
    azure_container = setting("AZURE_CONTAINER")
    azure_ssl = setting("AZURE_SSL")

    # adding support for custom storage endpoint
    azure_storage_endpoint_suffix = setting("AZURE_STORAGE_ENDPOINT")

    def __init__(self, container=None, url_expiry_secs=None, *args, **kwargs):

        super(AzureStorage, self).__init__(*args, **kwargs)
        self._connection = None
        self.url_expiry_secs = url_expiry_secs

        if container:
            self.azure_container = container


    @property
    def connection(self):
        if self._connection is None:
            if self.azure_storage_endpoint_suffix:
                self._connection = BlobService(self.account_name, self.account_key,  host_base=self.azure_storage_endpoint_suffix)
            else:
                self._connection = BlobService(self.account_name, self.account_key)
        return self._connection

    @property
    def azure_protocol(self):
        if self.azure_ssl:
            return 'https'
        return 'http' if self.azure_ssl is not None else None

    def __get_blob_properties(self, name):
        try:
            return self.connection.get_blob_properties(
                self.azure_container,
                name
            )
        except AzureMissingResourceHttpError:
            return None

    def _open(self, name, mode="rb"):
        contents = self.connection.get_blob(self.azure_container, name)
        return ContentFile(contents)

    def exists(self, name):
        return self.__get_blob_properties(name) is not None

    def delete(self, name):
        try:
            self.connection.delete_blob(self.azure_container, name)
        except AzureMissingResourceHttpError:
            pass

    def size(self, name):
        properties = self.connection.get_blob_properties(
            self.azure_container, name)
        return properties["content-length"]

    def _save(self, name, content):
        if hasattr(content.file, 'content_type'):
            content_type = content.file.content_type
        else:
            content_type = mimetypes.guess_type(name)[0]

        if hasattr(content, 'chunks'):
            content_data = b''.join(chunk for chunk in content.chunks())
        else:
            content_data = content.read()

        self.connection.put_blob(self.azure_container, name,
                                 content_data, "BlockBlob",
                                 x_ms_blob_content_type=content_type)
        return name

    def url(self, name):

        sas_token = None
        if self.url_expiry_secs:
            now = datetime.utcnow().replace(tzinfo=pytz.utc)
            expire_at = now + timedelta(seconds=self.url_expiry_secs)

            policy = AccessPolicy()
            # generate an ISO8601 time string and use split() to remove the sub-second
            # components as Azure will reject them. Plus add the timezone at the end.
            policy.expiry = expire_at.isoformat().split('.')[0] + 'Z'
            policy.permission = 'r'

            sas_token = self.connection.generate_shared_access_signature(
                self.azure_container,
                blob_name=name,
                shared_access_policy=SharedAccessPolicy(access_policy=policy),
            )

        return self.connection.make_blob_url(
            container_name=self.azure_container,
            blob_name=name,
            protocol=self.azure_protocol,
            sas_token=sas_token
        )

    def modified_time(self, name):
        try:
            modified = self.__get_blob_properties(name)['last-modified']
        except (TypeError, KeyError):
            return super(AzureStorage, self).modified_time(name)

        modified = time.strptime(modified, '%a, %d %b %Y %H:%M:%S %Z')
        modified = datetime.fromtimestamp(mktime(modified))

        return modified

    def listdir(self, path):
        """
        The base implementation does not have a definition for this method
        which Open edX requires
        """
        if not path:
            path = None

        blobs = self.connection.list_blobs(
            container_name=self.azure_container,
            prefix=path,
        )
        results = []
        for f in blobs:
            name = f.name
            if path:
                name = name.replace(path, '')
            results.append(name)

        return ((), results)

