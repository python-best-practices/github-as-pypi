from typing import Optional, Tuple

import shortuuid

from private_pypi_testkit import TestKit
from private_pypi.backends.backend import (
        BackendInstanceManager,
        PkgRepoConfig,
        PkgRepoSecret,
        PkgRepo,
        LocalPaths,
)
from private_pypi.backends.file_system.impl import (
        FILE_SYSTEM_TYPE,
        FileSystemConfig,
        FileSystemSecret,
        FileSystemPkgRepo,
        FileSystemPkgRef,
)


class FileSystemTestKit(TestKit):

    @classmethod
    def setup_pkg_repo(cls) -> Tuple[PkgRepoConfig, PkgRepoSecret, PkgRepoSecret]:
        name = f'fs-{shortuuid.uuid()}'
        raw_read_secret = 'foo'
        raw_write_secret = 'bar'

        pkg_repo_config = FileSystemConfig(
                name=name,
                read_secret=raw_read_secret,
                write_secret=raw_write_secret,
        )
        read_secret = FileSystemSecret(
                name=name,
                raw=raw_read_secret,
        )
        write_secret = FileSystemSecret(
                name=name,
                raw=raw_write_secret,
        )
        return pkg_repo_config, read_secret, write_secret


FileSystemTestKit.pytest_injection()
