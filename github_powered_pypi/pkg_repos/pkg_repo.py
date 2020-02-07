from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Dict, Optional, Tuple


@dataclass
class PkgRepoConfig:
    name: str


@dataclass
class PkgRepoSecret:
    raw: str


@dataclass
class LocalPaths:
    stat: Optional[str] = None
    cache: Optional[str] = None


class UploadPackageStatus(Enum):
    SUCCEEDED = auto()
    FAILED = auto()
    TASK_CREATED = auto()


@dataclass
class UploadPackageResult:
    status: UploadPackageStatus
    message: str = ''
    task_id: Optional[str] = None


class DownloadPackageStatus(Enum):
    pass  # TODO


@dataclass
class DownloadPackageResult:
    pass  # TODO


@dataclass
class PkgRepo:
    config: PkgRepoConfig
    secret: PkgRepoSecret
    local_paths: LocalPaths

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
    def upload_package(self, name: str, meta: Dict[str, str], path: str) -> UploadPackageResult:
        pass

    @abstractmethod
    def view_task_upload_package(self, name: str, task_id: str) -> UploadPackageResult:
        pass

    @abstractmethod
    def download_package(self, name: str, output: str) -> DownloadPackageResult:
        pass

    @abstractmethod
    def view_task_download_package(self, name: str, task_id: str) -> DownloadPackageResult:
        pass

    @abstractmethod
    def collect_all_published_packages(self):
        pass

    @abstractmethod
    def upload_index(self, path: str):
        pass

    @abstractmethod
    def download_index(self, output: str):
        pass
