from abc import abstractmethod
from enum import Enum, auto
import functools
import traceback
from typing import Dict, List, Tuple, TypeVar

from pydantic import BaseModel


class PkgRepoConfig(BaseModel):
    type: str
    name: str
    max_file_bytes: int = 1024**3
    sync_index_interval: int = 60


class PkgRepoSecret(BaseModel):
    type: str
    name: str
    raw: str

    @abstractmethod
    def secret_hash(self) -> str:
        pass


class PkgRef(BaseModel):
    type: str
    distrib: str
    package: str
    ext: str
    sha256: str
    meta: Dict[str, str]

    @abstractmethod
    def auth_url(self, config: PkgRepoConfig, secret: PkgRepoSecret) -> str:
        pass


class LocalPaths(BaseModel):
    index: str
    log: str
    lock: str
    job: str
    cache: str


class UploadPackageStatus(Enum):
    SUCCEEDED = auto()
    FAILED = auto()


class UploadPackageResult(BaseModel):
    status: UploadPackageStatus
    message: str = ''


class DownloadPackageStatus(Enum):
    pass  # TODO


class DownloadPackageResult(BaseModel):
    pass  # TODO


class UploadIndexStatus(Enum):
    SUCCEEDED = auto()
    FAILED = auto()


class UploadIndexResult(BaseModel):
    status: UploadIndexStatus
    message: str = ''


class DownloadIndexStatus(Enum):
    SUCCEEDED = auto()
    FAILED = auto()


class DownloadIndexResult(BaseModel):
    status: DownloadIndexStatus
    message: str = ''


class PkgRepo(BaseModel):
    config: PkgRepoConfig
    secret: PkgRepoSecret
    local_paths: LocalPaths

    @abstractmethod
    def record_error(self, error_message: str) -> None:
        pass

    @abstractmethod
    def ready(self) -> Tuple[bool, str]:
        pass

    @abstractmethod
    def auth_read(self) -> bool:
        pass

    @abstractmethod
    def auth_write(self) -> bool:
        pass

    @abstractmethod
    def upload_package(self, filename: str, meta: Dict[str, str], path: str) -> UploadPackageResult:
        pass

    @abstractmethod
    def collect_all_published_packages(self) -> List[PkgRef]:
        pass

    @abstractmethod
    def local_index_is_up_to_date(self, path: str) -> bool:
        pass

    @abstractmethod
    def upload_index(self, path: str) -> UploadIndexResult:
        pass

    @abstractmethod
    def download_index(self, path: str) -> DownloadIndexResult:
        pass


def basic_model_get_default(basic_model_cls: BaseModel, key: str):
    assert key in basic_model_cls.__fields__
    return basic_model_cls.__fields__[key].default


METHOD = TypeVar('METHOD')


def record_error_if_raises(method: METHOD) -> METHOD:

    @functools.wraps(method)
    def decorated(self, *args, **kwargs):
        try:
            ret = method(self, *args, **kwargs)
            return ret
        except:
            self.record_error(traceback.format_exc())
            raise

    return decorated
