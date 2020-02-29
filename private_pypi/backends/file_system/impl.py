from dataclasses import dataclass
import hashlib
from os.path import isdir
from typing import Dict, List, Optional, Tuple

from private_pypi.backends.backend import (
        PkgRef,
        PkgRepo,
        PkgRepoConfig,
        PkgRepoSecret,
        UploadPackageStatus,
        UploadPackageResult,
        UploadIndexStatus,
        UploadIndexResult,
        DownloadIndexStatus,
        DownloadIndexResult,
        record_error_if_raises,
        basic_model_get_default,
)
from private_pypi.utils import (
        normalize_distribution_name,
        update_hash_algo_with_file,
        git_hash_sha,
)

FILE_SYSTEM_TYPE = 'file_system'


class FileSystemConfig(PkgRepoConfig):
    # Override.
    type: str = FILE_SYSTEM_TYPE
    max_file_bytes: int = 5 * 1024**3 - 1
    # File system specific.
    read_secret: str = ''
    write_secret: str = ''


class FileSystemSecret(PkgRepoSecret):
    # Override.
    type: str = FILE_SYSTEM_TYPE

    @property
    def token(self) -> str:
        # pylint: disable=no-member
        return self.raw

    def secret_hash(self) -> str:
        sha256_algo = hashlib.sha256()
        sha256_algo.update(self.raw.encode())
        return f'fs-{sha256_algo.hexdigest()}'


class FileSystemPkgRef(PkgRef):
    # Override.
    type: str = FILE_SYSTEM_TYPE

    def auth_url(self, config: FileSystemConfig, secret: FileSystemSecret) -> str:
        pass


@dataclass
class FileSystemPkgRepoPrivateFields:
    ready: bool
    err_msg: str


class FileSystemPkgRepo(PkgRepo):
    # Override.
    type: str = FILE_SYSTEM_TYPE
    # GitHub specific.
    config: FileSystemConfig
    secret: FileSystemSecret

    __slots__ = ('_private_fields',)

    @property
    def _pvt(self) -> FileSystemPkgRepoPrivateFields:
        return object.__getattribute__(self, '_private_fields')

    def __init__(self, **data):
        super().__init__(**data)
        object.__setattr__(
                self,
                '_private_fields',
                FileSystemPkgRepoPrivateFields(ready=True, err_msg=''),
        )

        if not isdir(self.local_paths.cache):
            self._pvt.ready = False
            self._pvt.err_msg = 'Cache path not exists'

    def record_error(self, error_message: str) -> None:
        self._pvt.ready = False
        self._pvt.err_msg = error_message

    def ready(self) -> Tuple[bool, str]:
        return self._pvt.ready, self._pvt.err_msg

    def auth_read(self) -> bool:
        return self.secret.token in (self.config.read_secret, self.config.write_secret)

    def auth_write(self) -> bool:
        return self.secret.token == self.config.write_secret

    def upload_package(self, filename: str, meta: Dict[str, str], path: str) -> UploadPackageResult:
        pass

    def collect_all_published_packages(self) -> List[PkgRef]:
        pass

    def local_index_is_up_to_date(self, path: str) -> bool:
        pass

    def upload_index(self, path: str) -> UploadIndexResult:
        pass

    def download_index(self, path: str) -> DownloadIndexResult:
        pass
