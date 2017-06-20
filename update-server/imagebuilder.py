from util import create_folder, get_statuscode
import logging
import tarfile
from database import Database
import re
import shutil
import urllib.request
import tempfile
import logging
import hashlib
import os
import os.path
from config import Config
import subprocess

class ImageBuilder():
    def __init__(self, distro, version, target, subtarget):
        self.log = logging.getLogger(__name__)
        self.database = Database()
        self.config = Config()
        self.distro = distro
        self.version = version
        self.release = version
        self.imagebuilder_version = version
        if distro != "lede":
            self.imagebuilder_version = self.config.get("imagebuilder_latest")
        self.target = target
        self.subtarget = subtarget
        self.root = os.path.dirname(os.path.realpath(__file__))
        self._gen_name()
    
    def _gen_name(self, remove_subtarget=False):
        name_array = ["lede-imagebuilder", self.imagebuilder_version, self.target]
        # some imagebuilders have -generic removed
        if not remove_subtarget:
            name_array.append(self.subtarget)

        self.name = "-".join(name_array)
        self.name += ".Linux-x86_64"
        self.path = os.path.join("imagebuilder", self.distro, self.version, self.target, self.subtarget, self.name)


    def run(self):
        self.default_packages = self.database.get_default_packages(self.distro, self.release, self.target, self.subtarget)
        if not self.default_packages:
            self.parse_profiles()
            self.default_packages = self.database.get_default_packages(self.distro, self.release, self.target, self.subtarget)

        self.available_packages= self.database.get_available_packages(self.distro, self.release, self.target, self.subtarget)
        if not self.available_packages:
            self.parse_packages()
            self.available_packages= self.database.get_available_packages(self.distro, self.release, self.target, self.subtarget)

        self.pkg_arch = self.parse_packages_arch()
        logging.debug("found package arch %s", self.pkg_arch)
        self.add_custom_repositories()
        self.add_custom_makefile()
        self.log.info("initialized imagebuilder %s", self.name)

    # this is ugly due to the fact that some imagebuilders have -generic
    # removed in their download names, more generic apporach needed
    def created(self):
        if os.path.exists(os.path.join(self.path, "Makefile")):
            return True
        else:
            self._gen_name(True)
            if os.path.exists(os.path.join(self.path, "Makefile")):
                return True
            else:
                self._gen_name()
                return False

        return 
    def parse_packages_arch(self):
        logging.debug("parse_packages_arch")
        with open(os.path.join(self.path, ".config"), "r") as config:
            for line in config:
                if line.startswith("CONFIG_TARGET_ARCH_PACKAGES"):
                    return re.match(r'.*"(.+)"', line).group(1)

    def add_custom_repositories(self):
        self.log.info("adding custom repositories")
        with open(os.path.join(self.path, "repositories.conf"), "w") as repositories:
            with open(os.path.join(self.root, "repositories.conf"), "r") as custom_repositories:
                custom_repositories = custom_repositories.read()
                custom_repositories = re.sub(r"{{ release }}", self.version, custom_repositories)
                custom_repositories = re.sub(r"{{ target }}", self.target, custom_repositories)
                custom_repositories = re.sub(r"{{ subtarget }}", self.subtarget, custom_repositories)
                custom_repositories = re.sub(r"{{ pkg_arch }}", self.pkg_arch, custom_repositories)
                repositories.write(custom_repositories)

    def add_custom_makefile(self):
        self.log.info("adding custom Makefile")
        shutil.copyfile(os.path.join(self.root, "Makefile"), os.path.join(self.path, "Makefile"))

    def download_url(self):
        return os.path.join(self.config.get("imagebuilder_url"), self.imagebuilder_version, "targets", self.target, self.subtarget, self.name) + ".tar.xz"

    def setup(self): 
        ## will be read from config file later
        self.log.info("downloading imagebuilder %s", self.name)
        print(self.download_url())
        if get_statuscode(self.download_url()) != 404:
            self.download(self.download_url())
        else:
            self._gen_name(True)
            if get_statuscode(self.download_url()) != 404:
                self.log.debug("remove -generic from url")
                self.download(self.download_url())

    def download(self, url):
        with tempfile.TemporaryDirectory() as tar_folder:
            create_folder(os.path.dirname(self.path))
            tar_path = os.path.join(tar_folder, "imagebuilder.tar.xz")
            self.log.info("downloading %s", url)
            urllib.request.urlretrieve(url, tar_path)
            tar = tarfile.open(tar_path)
            tar.extractall(path=tar_folder)
            tar.close()
            shutil.move(os.path.join(tar_folder, self.name), self.path)
            return True
        return False


    def parse_profiles(self):
        cmdline = ['make', 'info']
        self.log.info("receive profiles for %s/%s", self.target, self.subtarget)

        proc = subprocess.Popen(
            cmdline,
            cwd=self.path,
            stdout=subprocess.PIPE,
            shell=False,
            stderr=subprocess.STDOUT
        )

        output, erros = proc.communicate()
        returnCode = proc.returncode
        output = output.decode('utf-8')
        if returnCode == 0:
            default_packages_pattern = r"(.*\n)*Default Packages: (.+)\n"
            default_packages = re.match(default_packages_pattern, output, re.M).group(2)
            logging.debug("default packages: %s", default_packages)
            profiles_pattern = r"(.+):\n    (.+)\n    Packages: (.*)\n"
            profiles = re.findall(profiles_pattern, output)
            if not profiles:
                profiles = []
#            print(output)
            self.database.insert_profiles(self.distro, self.release, self.target, self.subtarget, (default_packages, profiles))
        else:
            logging.error("could not receive profiles of %s/%s", self.target, self.subtarget)


    def parse_packages(self):
        self.log.info("receive packages for %s/%s", self.target, self.subtarget)

        cmdline = ['make', 'package_list']
        proc = subprocess.Popen(
            cmdline,
            cwd=self.path,
            stdout=subprocess.PIPE,
            shell=False,
            stderr=subprocess.STDOUT
        )

        output, erros = proc.communicate()
        returnCode = proc.returncode
        output = output.decode('utf-8')
       # print(output)
        if returnCode == 0:
            packages = re.findall(r"^(.+?) - (.+?) - .*\n", output)
            self.database.insert_packages(self.distro, self.release, self.target, self.subtarget, packages)
            return packages
        else:
            print(output)
            self.log.info("could not receive packages of %s/%s", self.target, self.subtarget)