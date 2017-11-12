import threading
import glob
import requests
import re
from zipfile import ZipFile
from socket import gethostname
import shutil
import json
import urllib.request
import tempfile
import hashlib
import os
import os.path
import subprocess
import signal
import sys
import logging
import time
import os
import yaml

from worker.imagebuilder import ImageBuilder
from utils.imagemeta import ImageMeta
from utils.common import create_folder, get_hash, get_folder, setup_gnupg, usign_sign, get_pubkey
from utils.config import Config
from utils.database import Database

MAX_TARGETS=0

class Worker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__)
        self.log.info("log initialized")
        self.config = Config()
        self.log.info("config initialized")
        self.database = Database()
        self.log.info("database initialized")
        self.worker_id = None
        self.imagebuilders = []

    def worker_register(self):
        worker_name = gethostname()
        worker_address = ""
        worker_pubkey = get_pubkey()
        self.log.info("register worker '%s' '%s' '%s'", worker_name, worker_address, worker_pubkey)
        self.worker_id = str(self.database.worker_register(worker_name, worker_address, worker_pubkey))

    def worker_add_skill(self, imagebuilder):
        self.database.worker_add_skill(self.worker_id, *imagebuilder, 'ready')

    def add_imagebuilder(self):
        self.log.info("adding imagebuilder")
        imagebuilder_request = None

        while not imagebuilder_request:
            imagebuilder_request = self.database.worker_needed()
            if not imagebuilder_request:
                self.heartbeat()
                time.sleep(5)
                continue

            self.log.info("found worker_needed %s", imagebuilder_request)
            for imagebuilder_setup in self.imagebuilders:
                if len(set(imagebuilder_setup).intersection(imagebuilder_request)) == 4:
                    self.log.info("already handels imagebuilder")
                    return

            self.distro, self.release, self.target, self.subtarget = imagebuilder_request
            self.log.info("worker serves %s %s %s %s", self.distro, self.release, self.target, self.subtarget)
            imagebuilder = ImageBuilder(self.distro, str(self.release), self.target, self.subtarget)
            self.log.info("initializing imagebuilder")
            if imagebuilder.run():
                self.log.info("register imagebuilder")
                self.worker_add_skill(imagebuilder.as_array())
                self.imagebuilders.append(imagebuilder.as_array())
                self.log.info("imagebuilder initialzed")
            else:
                # manage failures
                # add in skill status
                pass
        self.log.info("added imagebuilder")

    def destroy(self, signal=None, frame=None):
        self.log.info("destroy worker %s", self.worker_id)
        self.database.worker_destroy(self.worker_id)
        sys.exit(0)

    def run(self):
        self.log.info("register worker")
        self.worker_register()
        self.log.debug("setting up gnupg")
        setup_gnupg()
        while True:
            self.log.debug("severing %s", self.imagebuilders)
            build_job_request = None
            for imagebuilder in self.imagebuilders:
                build_job_request = self.database.get_build_job(*imagebuilder)
                if build_job_request:
                    break

            if build_job_request:
                self.log.debug("found build job")
                self.last_build_id = build_job_request[0]
                image = Image(*build_job_request[2:9])
                self.log.debug(image.as_array())
                if not image.build():
                    self.log.warn("build failed for %s", image.as_array())
            else:
                # heartbeat should be more less than 5 seconds
                if len(self.imagebuilders) < MAX_TARGETS or MAX_TARGETS == 0:
                    self.add_imagebuilder()
                self.heartbeat()
                time.sleep(5)

    def heartbeat(self):
        self.log.debug("heartbeat %s", self.worker_id)
        self.database.worker_heartbeat(self.worker_id)

class Image(ImageMeta):
    def __init__(self, distro, release, target, subtarget, profile, packages=None, network_profile=""):
        super().__init__(distro, release, target, subtarget, profile, packages.split(" "), network_profile)

    def build(self):
        imagebuilder_path = os.path.abspath(os.path.join("imagebuilder", self.distro, self.target, self.subtarget))
        self.imagebuilder = ImageBuilder(self.distro, self.release, self.target, self.subtarget)

        self.log.info("use imagebuilder %s", self.imagebuilder.path)


        with tempfile.TemporaryDirectory(dir=get_folder("tempdir")) as self.build_path:
            already_created = False

            # only add manifest hash if special packages
            extra_image_name = ""
            if not self.vanilla:
                extra_image_name = self.request_hash

            cmdline = ['make', 'image', "-j", str(os.cpu_count())]
            cmdline.append('PROFILE=%s' % self.profile)
            if self.network_profile:
                self.log.debug("add network_profile %s", self.network_profile)
                extra_image_name = "{}-{}".format(extra_image_name, self.network_profile.replace("/", "-").replace(".", "_"))
                self.network_profile_packages()
                cmdline.append('FILES=%s' % self.network_profile_path)
            self.log.warning(extra_image_name)
            cmdline.append('EXTRA_IMAGE_NAME=%s' % extra_image_name)
            if not self.vanilla:
                self.diff_packages()
            cmdline.append('PACKAGES=%s' % ' '.join(self.packages))
            cmdline.append('BIN_DIR=%s' % self.build_path)

            self.log.info("start build: %s", " ".join(cmdline))

            env = os.environ.copy()
            if not self.database.subtarget_outdated(self.distro, self.release, self.target, self.subtarget):
                env = dict(os.environ, NO_UPDATE="1")

            proc = subprocess.Popen(
                cmdline,
                cwd=self.imagebuilder.path,
                stdout=subprocess.PIPE,
                shell=False,
                stderr=subprocess.STDOUT,
                env=env
            )

            self.log_output, erros = proc.communicate()
            returnCode = proc.returncode
            if returnCode == 0:
                self.log.info("build successfull")
                self.manifest_hash = hashlib.sha256(open(glob.glob(os.path.join(self.build_path, '*.manifest'))[0],'rb').read()).hexdigest()[0:15]
                self.manifest_id = self.database.add_manifest(self.manifest_hash)
                self.parse_manifest()
                self.image_hash = get_hash(" ".join(self.as_array_build()), 15)

                # changeing the database but keep files leads to an error
                entry_missing = False
                self.database.c.execute("select 1 from images where image_hash = ?", self.image_hash)
                if self.database.c.rowcount == 0:
                    entry_missing = True

                path_array = [get_folder("downloaddir"), self.distro, self.release, self.target, self.subtarget, self.profile]
                if not self.vanilla:
                    path_array.append(self.manifest_hash)

                self.store_path = os.path.join(*path_array)
                self.store_path = "/tmp/workers/"
                create_folder(self.store_path)

                with ZipFile(os.path.join(self.store_path, self.request_hash + ".zip"), 'w') as archive:
                    for filename in os.listdir(self.build_path):
                        filename_output = filename.replace("lede", self.distro)
                        # this is done for libremesh as imagebuilder release is different from lime release
                        filename_output = filename_output.replace(self.imagebuilder.imagebuilder_release, self.release)
                        filename_output = filename_output.replace(self.request_hash, self.manifest_hash)
                        if not os.path.exists(os.path.join(self.store_path, filename_output)) or entry_missing:
                            self.log.info("move file %s", filename_output)
                            shutil.move(os.path.join(self.build_path, filename), os.path.join(self.store_path, filename_output))
                            archive.write(os.path.join(self.store_path, filename_output), arcname=filename_output)
                        else:
                            self.log.info("file %s exists so image was created before", filename_output)
                            already_created = True
                            break

                if not already_created or entry_missing:
                    sysupgrade_files = [ "*-squashfs-sysupgrade.bin", "*-squashfs-sysupgrade.tar",
                        "*-squashfs.trx", "*-squashfs.chk", "*-squashfs.bin",
                        "*-squashfs-sdcard.img.gz", "*-combined-squashfs*"]

                    sysupgrade = None

                    for sysupgrade_file in sysupgrade_files:
                        if not sysupgrade:
                            sysupgrade = glob.glob(os.path.join(self.store_path, sysupgrade_file))
                        else:
                            break

                    if not sysupgrade:
                        self.log.error("created image was to big")
                        self.store_log(os.path.join(get_folder("downloaddir"), "faillogs", self.request_hash))
                        self.database.set_image_requests_status(self.request_hash, 'imagesize_fail')
                        return False

                    self.path = sysupgrade[0]
                    sysupgrade_image = os.path.basename(self.path)

                    self.subtarget_in_name = self.subtarget in sysupgrade_image
                    self.profile_in_name = self.profile in sysupgrade_image

                    # ath25/generic/generic results in lede-17.01.4-ath25-generic-squashfs-sysupgrade...
                    if (self.profile == self.subtarget and
                            "{}-{}".format(self.subtarget, self.profile) not in sysupgrade_image):
                        self.subtarget_in_name = False

                    name_array = [self.distro]

                    # snapshot build are no release
                    if self.release != "snapshot":
                        name_array.append(self.release)

                    if not self.vanilla:
                        name_array.append(self.manifest_hash)

                    # add network_profile to name if set
                    if self.network_profile:
                        name_array.append(self.network_profile.replace("/", "-").replace(".", "_"))

                    name_array.append(self.target)

                    if self.subtarget_in_name:
                        name_array.append(self.subtarget)

                    if self.profile_in_name:
                        name_array.append(self.profile)

                    self.name = "-".join(name_array)

                    if self.config.get("sign_images"):
                        if usign_sign(self.path):
                            self.log.info("signed %s", self.path)
                        else:
                            self.database.set_image_requests_status(self.request_hash, 'signing_fail')
                            return False

                    self.gen_checksum()
                    self.gen_filesize()
                    self.store_log(self.path)
                    self.log.debug("image: {} {} {} {} {} {} {} {}".format(
                            self.image_hash,
                            self.as_array_build(),
                            self.checksum,
                            self.filesize,
                            sysupgrade_image.replace(self.name + "-", ""),
                            self.subtarget_in_name,
                            self.profile_in_name,
                            self.vanilla))
                    self.database.add_image(
                            self.image_hash,
                            self.as_array_build(),
                            self.checksum,
                            self.filesize,
                            sysupgrade_image.replace(self.name + "-", ""),
                            self.subtarget_in_name,
                            self.profile_in_name,
                            self.vanilla)
                self.database.done_build_job(self.request_hash, self.image_hash)
                return True
            else:
                self.log.info("build failed")
                self.database.set_image_requests_status(self.request_hash, 'build_fail')
                self.store_log(os.path.join(get_folder("downloaddir"), "faillogs", self.request_hash))
                return False

    def store_log(self, path):
        self.log.debug("write log to %s", path)
        log_file = open(path + ".log", "a")
        log_file.writelines(json.dumps(self.as_array(), indent=4, sort_keys=True))
        log_file.write("\n\n")
        log_file.writelines(self.log_output.decode('utf-8'))

    def gen_checksum(self):
        self.checksum = hashlib.md5(open(self.path,'rb').read()).hexdigest()
        self.log.debug("got md5sum %s for %s", self.checksum, self.path)

    def gen_filesize(self):
        self.filesize = os.stat(self.path).st_size

    def network_profile_packages(self):
        extra_packages = os.path.join(self.network_profile_path, 'PACKAGES')
        if os.path.exists(extra_packages):
            with open(extra_packages, "r") as extra_packages_file:
                self.packages.extend(extra_packages_file.read().split())

    def diff_packages(self):
        profile_packages = self.vanilla_packages
        for package in self.packages:
            if package in profile_packages:
                profile_packages.remove(package)
        for remove_package in profile_packages:
            self.packages.append("-" + remove_package)

    def parse_manifest(self):
        manifest_pattern = r"(.+) - (.+)\n"
        with open(glob.glob(os.path.join(self.build_path, '*.manifest'))[0], "r") as manifest_file:
            manifest_packages = re.findall(manifest_pattern, manifest_file.read())
            self.database.add_manifest_packages(self.manifest_hash, manifest_packages)

    def upload_image(self):
        url = os.path.join(self.config.get("update_server"), "upload-image")
        data = {
                "request_hash": self.request_hash,
                "worker_id": self.worker_id
                }
        files = {
                'archive': open(archive_file, 'rb'),
                'signature': open(archive_file + ".sig", 'rb')
                }


    # check if image exists
    def created(self):
        return os.path.exists(self.path)
