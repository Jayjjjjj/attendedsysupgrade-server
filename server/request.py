from http import HTTPStatus
import logging
import json

from utils.config import Config
from utils.database import Database
from utils.common import get_latest_release

class Request():
    def __init__(self, request_json):
        self.log = logging.getLogger(__name__)
        self.config = Config()
        self.request_json = request_json
        self.response_dict = {}
        self.database = Database()

    def run(self):
        bad_request = self.check_bad_request()
        if bad_request:
            return bad_request

    def check_bad_request(self):
        if not self.vaild_request():
            self.log.info("received invaild request")
            self.response_dict["error"] = "missing parameters - need %s" % " ".join(self.needed_values)
            return self.respond(), HTTPStatus.BAD_REQUEST

        if not "distro" in self.request_json:
            self.distro = "lede"
        else:
            self.distro = self.request_json["distro"].lower()

            if not self.distro in self.config.get("distributions").keys():
                self.log.info("update request unknown distro")
                self.response_dict["error"] = "unknown distribution %s" % self.distro
                return self.respond(), HTTPStatus.BAD_REQUEST

        if not "version" in self.request_json:
            self.release = get_latest_release(self.distro)
        else:
            self.release = self.request_json["version"].lower()

            if not self.release in self.database.get_releases(self.distro):
                self.response_dict["error"] = "unknown release %s" % self.release
                return self.respond(), HTTPStatus.BAD_REQUEST

    def check_bad_target(self):
        self.target = self.request_json["target"]
        self.subtarget = self.request_json["subtarget"]

        subtarget_check =  self.database.get_subtargets(self.distro, self.release, self.target, self.subtarget)
        if not len(subtarget_check) == 1:
            self.response_dict["error"] = "unknown target %s/%s" % (self.target, self.subtarget)
            return self.respond(), HTTPStatus.BAD_REQUEST
        elif not subtarget_check[0][2] == "1": # [2] is supported flag
            self.response_dict["error"] = "target currently not supported %s/%s" % (self.target, self.subtarget)
            return self.respond(), HTTPStatus.BAD_REQUEST

        if self.database.subtarget_outdated(self.distro, self.release, self.target, self.subtarget):
            self.log.debug("subtarget %s/%s not outdated - no need to setup imagebuilder", self.target, self.subtarget)
            if not self.database.imagebuilder_status(self.distro, self.release, self.target, self.subtarget) == 'ready':
                self.log.debug("imagebuilder not ready")
                return self.respond(), HTTPStatus.CREATED

        return False

    def vaild_request(self):
        # needed params to check sysupgrade
        for value in self.needed_values:
            if not value in self.request_json:
                return False
        return True

    def respond(self):
        self.log.debug(self.response_dict)
        return json.dumps(self.response_dict)

    # if local version is newer than received returns true
    def release_latest(self, latest, external):
        return LooseVersion(external) >= LooseVersion(latest)

    def check_bad_packages(self):
        self.packages = None
        if "packages" in self.request_json:
            self.packages = self.request_json["packages"]
            available_packages = self.database.get_packages_available(self.distro, self.release, self.target, self.subtarget).keys()
            for package in self.packages:
                if package in ["kernel", "libc", "base-files"]: # these tend to cause problems, even tho always installed
                    pass # kernel is not an installable package, but installed...
                elif package not in available_packages:
                    logging.warning("could not find package {}/{}/{}/{}/{}".format(self.distro, self.release, self.target, self.subtarget, package))
                    self.response_dict["error"] = "could not find package '{}' for requested target".format(package)
                    return self.respond(), HTTPStatus.BAD_REQUEST
        return False
