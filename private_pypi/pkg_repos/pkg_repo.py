from abc import abstractmethod
from enum import Enum, auto
import functools
import importlib
import pkgutil
import traceback
from typing import Dict, List, Tuple, TypeVar, Type

from pydantic import BaseModel


####################
# Static I/O types #
####################
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


####################################
# Interfaces of package repository #
####################################
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


class BackendRegistration:
    type: str = ''
    pkg_repo_config_cls: Type[PkgRepoConfig] = PkgRepoConfig
    pkg_repo_secret_cls: Type[PkgRepoSecret] = PkgRepoSecret
    pkg_repo_cls: Type[PkgRepo] = PkgRepo
    pkg_ref_cls: Type[PkgRef] = PkgRef


class BackendInstanceManager:

    def __init__(self) -> None:
        self._type_to_registration = {}

        # Namespace package root.
        root_module = importlib.import_module('.', 'private_pypi.backends')
        # Find all submodules.
        for module_info in pkgutil.iter_modules(
                root_module.__path__,  # type: ignore
                root_module.__name__ + '.',
        ):
            # Load module.
            module = importlib.import_module(module_info.name)

            # Find the registration class.
            registration = None
            for obj in module.__dict__.values():
                if issubclass(obj, BackendRegistration) and obj is not BackendRegistration:
                    registration = obj

            if registration is None:
                continue

            # Type validation.
            assert registration.type

            assert issubclass(registration.pkg_repo_config_cls, PkgRepoConfig) \
                    and registration.pkg_repo_config_cls is not PkgRepoConfig

            assert issubclass(registration.pkg_repo_secret_cls, PkgRepoSecret) \
                    and registration.pkg_repo_secret_cls is not PkgRepoSecret

            assert issubclass(registration.pkg_repo_cls, PkgRepo) \
                    and registration.pkg_repo_cls is not PkgRepo

            assert issubclass(registration.pkg_ref_cls, PkgRef) \
                    and registration.pkg_ref_cls is not PkgRef

            self._type_to_registration[registration.type] = registration


####################
# Helper functions #
####################
def basic_model_get_default(basic_model_cls: BaseModel, key: str):
    assert key in basic_model_cls.__fields__
    return basic_model_cls.__fields__[key].default


_METHOD = TypeVar('_METHOD')


def record_error_if_raises(method: _METHOD) -> _METHOD:

    @functools.wraps(method)
    def decorated(self, *args, **kwargs):
        try:
            ret = method(self, *args, **kwargs)
            return ret
        except:
            self.record_error(traceback.format_exc())
            raise

    return decorated
