from abc import abstractmethod
from dataclasses import dataclass
import tempfile
from typing import Optional, Tuple

from private_pypi.backends.backend import (
        BackendInstanceManager,
        PkgRepoConfig,
        PkgRepoSecret,
        PkgRepo,
        LocalPaths,
)


@dataclass
class RepoInfoForTest:
    pkg_repo_config_file: str
    admin_pkg_repo_secret_file: str
    root_folder: str
    read_secret: PkgRepoSecret
    write_secret: PkgRepoSecret


class TestKit:

    @classmethod
    @abstractmethod
    def setup_pkg_repo(cls) -> Tuple[PkgRepoConfig, PkgRepoSecret, PkgRepoSecret]:
        pass

    @classmethod
    def pytest_injection(cls):
        import pytest
        import inspect

        _caller_frame = inspect.currentframe().f_back

        def inject_to_caller(func):
            caller_globals = _caller_frame.f_globals
            caller_globals[func.__name__] = func
            return func

        def _create_repo_for_test(create_tmpdir):
            config_folder = create_tmpdir('config')

            pkg_repo_config, read_secret, write_secret = cls.setup_pkg_repo()
            pkg_repo_config_file = str(config_folder.join('config.toml'))
            admin_pkg_repo_secret_file = str(config_folder.join('admin_secret.toml'))

            BackendInstanceManager.dump_pkg_repo_configs(pkg_repo_config_file, [pkg_repo_config])
            BackendInstanceManager.dump_pkg_repo_secrets(admin_pkg_repo_secret_file, [write_secret])

            return RepoInfoForTest(
                    pkg_repo_config_file=pkg_repo_config_file,
                    admin_pkg_repo_secret_file=admin_pkg_repo_secret_file,
                    root_folder=str(create_tmpdir('root')),
                    read_secret=read_secret,
                    write_secret=write_secret,
            )

        @inject_to_caller
        @pytest.fixture(scope='session')
        def session_repo(tmpdir_factory):
            yield _create_repo_for_test(tmpdir_factory.mktemp)

        @inject_to_caller
        @pytest.fixture(scope='function')
        def function_repo(tmpdir):
            yield _create_repo_for_test(tmpdir.mkdir)
