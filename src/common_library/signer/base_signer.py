import glob
import logging
import os
import shutil
import traceback
import typing
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from datetime import datetime
from pathlib import Path

import pgpy
import rpm

from common_library.constants import SignStatusEnum
from common_library.errors import SignError
from common_library.signer.package_sign import (
    sign_deb_package,
    sign_dsc_package,
    sign_rpm_package,
)
from common_library.utils.file_utils import (
    download_file,
    hash_file,
    safe_mkdir,
)


class BaseSigner:
    def __init__(
        self,
        config,
        key_ids,
        gpg,
        codenotary_enabled=False,
        files_sign_cert_path=None,
        download_credentials=None,
    ):
        self._config = config
        self.__key_ids = key_ids
        self._gpg = gpg
        self._working_dir_path = Path(self._config.working_dir)
        self._notar_enabled = codenotary_enabled
        self.__files_sign_cert_path = files_sign_cert_path
        self.__download_credentials = download_credentials or {}

    def sign_loop(self):
        raise NotImplementedError('Needs to be implemented in child classes')

    def _check_signature(self, files, key_id):
        errors = []
        key_id_lower = key_id.lower()
        ts = rpm.TransactionSet()
        ts.setVSFlags(rpm._RPMVSF_NOSIGNATURES)
        subkeys = [i.lower() for i in self.__key_ids[key_id]["subkeys"]]

        def check(pkg_path: str) -> typing.Tuple[SignStatusEnum, str]:
            pkg_path = Path(pkg_path)
            if not pkg_path.exists():
                return SignStatusEnum.READ_ERROR, ''

            with pkg_path.open('rb') as fd:
                header = ts.hdrFromFdno(fd)
                signature = header[rpm.RPMTAG_SIGGPG]
                if not signature:
                    signature = header[rpm.RPMTAG_SIGPGP]
                if not signature:
                    return SignStatusEnum.NO_SIGNATURE, ''

            pgp_msg = pgpy.PGPMessage.from_blob(signature)
            sig = ''
            for signature in pgp_msg.signatures:
                sig = signature.signer.lower()
                if sig == key_id_lower:
                    return SignStatusEnum.SUCCESS, ''
                if subkeys and sig in subkeys:
                    return SignStatusEnum.SUCCESS, ''

            return SignStatusEnum.WRONG_SIGNATURE, sig

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check, file_): file_ for file_ in files}

            for future in as_completed(futures):
                pkg_path = futures[future]
                result, signature = future.result()
                if result == SignStatusEnum.READ_ERROR:
                    errors.append(f'Cannot read file {pkg_path}')
                elif result == SignStatusEnum.NO_SIGNATURE:
                    errors.append(f'Package {pkg_path} is not signed')
                elif result == SignStatusEnum.WRONG_SIGNATURE:
                    errors.append(
                        f'Package {pkg_path} is signed '
                        f'with the wrong key: {signature}'
                    )

        return errors

    @staticmethod
    def timedelta_seconds(start_time: datetime, finish_time: datetime) -> int:
        return int((finish_time - start_time).total_seconds())

    def _sign_build(self, task):
        """
        Signs packages from the specified task and uploads them to the server.

        Parameters
        ----------
        task : dict
            Sign task.
        """

        # We will need this one to map downloaded packages to the package info
        # from the task payload
        pkg_info_mapping = {}
        pkg_verification_mapping = {}

        def download_package(pkg: dict, platform=None):
            package_type = pkg.get('type', pkg.get('package_type','rpm'))
            if package_type in ('deb', 'dsc'):
                download_dir = debs_dir
            else:
                download_dir = rpms_dir
            pkg_path = self._download_package(download_dir, pkg, platform)

            pkg_info_mapping[pkg_path] = pkg
            return pkg, (
                pkg['id'],
                pkg.get('file_name', pkg.get('name')),
                pkg_path,
                platform,
            )

        stats = {'sign_task_start_time': str(datetime.utcnow())}
        pgp_keyid = task.get('keyid', task.get('pgp_keyid'))
        if not pgp_keyid:
            raise SignError('No pgp keyid')
        sign_files = task.get('sign_files', False)
        pgp_key_password = self.__key_ids[pgp_keyid]["password"]
        fingerprint = self.__key_ids[pgp_keyid]["fingerprint"]
        task_dir = self._working_dir_path.joinpath(str(task['id']))
        rpms_dir = task_dir.joinpath('rpms')
        debs_dir = task_dir.joinpath('debs')
        downloaded = []
        response_payload = {'build_id': task.get('build_id'), 'success': True}
        packages = {}
        start_time = datetime.utcnow()

        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = []
                if isinstance(task['packages'], dict):
                    futures = [
                        executor.submit(
                            lambda pkg=pkg, plat=platform: download_package(
                                pkg,
                                plat,
                            )
                        )
                        for platform, pkgs in task['packages'].items()
                        for pkg in pkgs
                    ]
                else:
                    futures = [
                        executor.submit(download_package, package)
                        for package in task['packages']
                    ]

                for future in as_completed(futures):
                    package, downloaded_info = future.result()
                    # Preparing the payload for returning to web server
                    signed_package = package.copy()
                    signed_package['fingerprint'] = fingerprint
                    signed_package.pop('download_url')
                    packages[package['id']] = signed_package
                    downloaded.append(downloaded_info)
            # Since grpcio library used in immudb client is not thread-safe,
            # we move its usage outside the multithreaded workflow
            for pkg_path, pkg_info in pkg_info_mapping.items():
                if self._notar_enabled and pkg_info.get('cas_hash'):
                    verification = self.verify_artifact(pkg_path)
                    if not verification:
                        raise SignError(
                            f'Package {pkg_info} cannot be verified'
                        )
                    pkg_verification_mapping[pkg_path] = verification

            finish_time = datetime.utcnow()
            stats['download_packages_time'] = self.timedelta_seconds(
                start_time, finish_time
            )
            start_time = datetime.utcnow()
            packages_to_sign = []
            for package in glob.glob(
                os.path.join(rpms_dir, '**/*.rpm'),
                recursive=True,
            ):
                packages_to_sign.append(package)
                if len(packages_to_sign) % 50 == 0:
                    sign_rpm_package(
                        ' '.join(packages_to_sign),
                        pgp_keyid,
                        pgp_key_password,
                        sign_files=sign_files,
                        sign_files_cert_path=self.__files_sign_cert_path,
                    )
                    packages_to_sign = []
            if packages_to_sign:
                sign_rpm_package(
                    ' '.join(packages_to_sign),
                    pgp_keyid,
                    pgp_key_password,
                    sign_files=sign_files,
                    sign_files_cert_path=self.__files_sign_cert_path,
                )
            for package_path in glob.glob(
                os.path.join(debs_dir, '**/*.deb'),
                recursive=True,
            ):
                sign_deb_package(
                    self._gpg,
                    package_path,
                    pgp_keyid,
                    pgp_key_password,
                )
            for package_path in glob.glob(
                os.path.join(debs_dir, '**/*.dsc'),
                recursive=True,
            ):
                sign_dsc_package(
                    self._gpg,
                    package_path,
                    pgp_keyid,
                    pgp_key_password,
                )

            finish_time = datetime.utcnow()
            stats['sign_packages_time'] = self.timedelta_seconds(
                start_time, finish_time
            )
            start_time = datetime.utcnow()
            # upload signed packages and report the task completion
            # Sort files for parallel and sequential upload by their size
            files_to_upload = set()
            parallel_upload_files = {}
            sequential_upload_files = {}
            packages_hrefs = {}
            files_to_check = list()
            for package_id, file_name, package_path, platform in downloaded:
                old_meta = pkg_verification_mapping.get(package_path)
                if self._notar_enabled and old_meta is not None:
                    cas_hash = self.notarize_artifact(package_path, old_meta)
                    packages[package_id]['cas_hash'] = cas_hash
                sha256 = hash_file(package_path, hash_type='sha256')
                if sha256 not in files_to_upload:
                    if (
                        self._config.parallel_upload
                        and os.stat(package_path).st_size
                        <= self._config.parallel_upload_file_size
                    ):
                        parallel_upload_files[sha256] = (
                            package_id,
                            file_name,
                            package_path,
                            platform,
                        )
                    else:
                        sequential_upload_files[sha256] = (
                            package_id,
                            file_name,
                            package_path,
                            platform,
                        )
                    files_to_upload.add(sha256)
                    if '.rpm' in package_path:
                        files_to_check.append(package_path)
                packages[package_id]['sha256'] = sha256

            finish_time = datetime.utcnow()
            stats['notarization_packages_time'] = self.timedelta_seconds(
                start_time, finish_time
            )
            start_time = datetime.utcnow()

            sign_errors = self._check_signature(files_to_check, pgp_keyid)
            finish_time = datetime.utcnow()
            stats['signature_check_packages_time'] = self.timedelta_seconds(
                start_time, finish_time
            )
            if sign_errors:
                error_message = (
                    'Errors during checking packages '
                    'signatures: \n{}'.format('\n'.join(sign_errors))
                )
                logging.error(error_message)
                raise SignError(error_message)

            start_time = datetime.utcnow()
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {
                    executor.submit(
                        self._upload_artifact,
                        package_path,
                        task['id'],
                        platform,
                        package_id,
                        file_name,
                    ): package_id
                    for package_id, file_name, package_path, platform in parallel_upload_files.values()
                }
                for future in as_completed(futures):
                    result = future.result()
                    package_id = futures[future]
                    package_name = packages[package_id].get('name')
                    packages[package_id]['href'] = result.href
                    packages_hrefs[package_name] = result.href
            for (
                p_id,
                file_name,
                pkg_path,
                platform,
            ) in sequential_upload_files.values():
                uploaded = self._upload_artifact(
                    pkg_path,
                    task['id'],
                    platform,
                    p_id,
                    file_name,
                )
                if uploaded:
                    packages[p_id]['href'] = uploaded.href
                    packages_hrefs[file_name] = uploaded.href
            if self._config.parallel_upload:
                # Fill href for packages of the same architecture
                for id_, package in packages.items():
                    if not package.get('href'):
                        packages[id_]['href'] = packages_hrefs[
                            package.get('name')
                        ]
            response_payload['packages'] = list(packages.values())
            finish_time = datetime.utcnow()
            stats['upload_packages_time'] = self.timedelta_seconds(
                start_time, finish_time
            )
            response_payload['stats'] = stats
        except Exception:
            error_message = traceback.format_exc()
            response_payload['success'] = False
            response_payload['error_message'] = error_message
        finally:
            logging.info('Response payload:')
            logging.info(response_payload)
            self._report_signed_build(task['id'], response_payload)
            if os.path.exists(task_dir):
                shutil.rmtree(task_dir)
            # Explicit deletion to avoid memory leaks
            del pkg_info_mapping
            del pkg_verification_mapping

    def _report_signed_build(self, *args, **kwargs):
        raise NotImplementedError('Needs to be implemented in child classes')

    def verify_artifact(self, *args):
        raise NotImplementedError('Needs to be implemented in child classes')

    def notarize_artifact(self, *args, **kwargs):
        raise NotImplementedError('Needs to be implemented in child classes')

    def _download_package(
        self,
        download_dir,
        package,
        platform=None,
        try_count=3,
    ):
        """
        Downloads the specified package from the Build System server and checks
        the download file checksum.

        Parameters
        ----------
        download_dir : str
            Download directory base path.
        package : dict
            Package information.
        platform : str
            Build platform name.
        try_count : int, optional
            The number of download tries before aborting.

        Returns
        -------
        str
            Downloaded file path.

        Raises
        ------
        errors.ConnectionError
            If the package download is failed.
        """
        platform_dir = platform if platform else ''
        package_dir = os.path.join(
            download_dir, platform_dir, str(package['id'])
        )
        safe_mkdir(package_dir)
        package_name = package.get('file_name', package.get('name'))
        package_path = os.path.join(package_dir, package_name)
        download_url = package['download_url']
        last_exc = None
        for i in range(1, try_count + 1):
            logging.debug('Downloading %s %d/%d', download_url, i, try_count)
            try:
                download_file(
                    download_url,
                    package_path,
                    **self.__download_credentials,
                )
                return package_path
            except Exception as e:
                last_exc = e
                logging.error(
                    'Cannot download %s: %s.\nTraceback:\n%s',
                    download_url,
                    str(e),
                    traceback.format_exc(),
                )
        raise last_exc

    def _upload_artifact(self, *args, **kwargs):
        raise NotImplementedError('Needs to be implemented in child classes')
