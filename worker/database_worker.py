import datetime
import pyodbc
import logging
import json

from utils.common import get_hash
from utils.database import DatabaseMeta
from utils.config import Config

class Database(DatabaseMeta):
    def __init__(self):
        # python3 immport pyodbc; pyodbc.drivers()
        #self.cnxn = pyodbc.connect("DRIVER={SQLite3};SERVER=localhost;DATABASE=test.db;Trusted_connection=yes")
        self.log = logging.getLogger(__name__)
        self.log.info("log initialized")
        self.config = Config()
        self.log.info("config initialized")
        connection_string = "DRIVER={};SERVER={};DATABASE={};UID={};PWD={};PORT={}".format(
                self.config.get("database_type"), self.config.get("database_address"), self.config.get("database_name"), self.config.get("database_user"),
                self.config.get("database_pass"), self.config.get("database_port"))
        self.cnxn = pyodbc.connect(connection_string)
        self.c = self.cnxn.cursor()
        self.log.info("database connected")

    def commit(self):
        """simply commits changes to database"""

        self.cnxn.commit()

    def insert_hash(self, hash, packages):
        sql = "INSERT INTO packages_hashes (hash, packages) VALUES (?, ?)"
        self.c.execute(sql, (hash, " ".join(packages)))
        self.commit()

    def delete_profiles(self, distro, release, target, subtarget, profiles):
        self.log.debug("delete profiles of %s/%s/%s/%s", distro, release, target, subtarget)
        subtarget_id = self.c.execute("""delete from profiles_table
            where subtarget_id = (select id from subtargets where
            subtargets.distro = ? and
            subtargets.release = ? and
            subtargets.target = ? and
            subtargets.subtarget = ?)""", distro, release, target, subtarget)
        self.commit()

    def insert_profiles(self, distro, release, target, subtarget, packages_default, profiles):

        self.log.debug("insert_profiles %s/%s/%s/%s", distro, release, target, subtarget)
        self.c.execute("INSERT INTO packages_default VALUES (?, ?, ?, ?, ?);", distro, release, target, subtarget, packages_default)

        sql = "INSERT INTO packages_profile VALUES (?, ?, ?, ?, ?, ?, ?);"
        for profile in profiles:
            profile_name, profile_model, profile_packages = profile
            self.log.debug("insert '%s' '%s' '%s'", profile_name, profile_model, profile_packages)
            self.c.execute(sql, distro, release, target, subtarget, profile_name, profile_model, profile_packages)
        self.commit()

    def check_profile(self, distro, release, target, subtarget, profile):
        self.log.debug("check_profile %s/%s/%s/%s/%s", distro, release, target, subtarget, profile)
        self.c.execute("""SELECT profile FROM profiles
            WHERE distro=? and release=? and target=? and subtarget = ? and profile = ?
            LIMIT 1;""",
            distro, release, target, subtarget, profile)
        if self.c.rowcount == 1:
            return self.c.fetchone()[0]
        else:
            self.log.debug("use wildcard profile search")
            profile = '%' + profile
            self.c.execute("""SELECT profile FROM profiles
                WHERE distro=? and release=? and target=? and subtarget = ? and profile LIKE ?
                LIMIT 1;""",
                distro, release, target, subtarget, profile)
            if self.c.rowcount == 1:
                return self.c.fetchone()[0]
        return False

    def check_model(self, distro, release, target, subtarget, model):
        self.log.debug("check_model %s/%s/%s/%s/%s", distro, release, target, subtarget, model)
        self.c.execute("""SELECT profile FROM profiles
            WHERE distro=? and release=? and target=? and subtarget = ? and lower(model) = lower(?);""",
            distro, release, target, subtarget, model)
        if self.c.rowcount == 1:
            return self.c.fetchone()[0]
        return False

    def get_image_packages(self, distro, release, target, subtarget, profile, as_json=False):
        self.log.debug("get_image_packages for %s/%s/%s/%s/%s", distro, release, target, subtarget, profile)
        sql = "select packages from packages_image where distro = ? and release = ? and target = ? and subtarget = ? and profile = ?"
        self.c.execute(sql, distro, release, target, subtarget, profile)
        response = self.c.fetchone()
        if response:
            packages = response[0].rstrip().split(" ")
            if as_json:
                return json.dumps({"packages": packages})
            else:
                return packages
        else:
            return response

    def outdated_package_index(self, distro, release, target, subtarget):
        self.log.debug("insert packages of {}/{}/{}/{}".format(distro, release, target, subtarget))
        sql = """select 1 from subtargets
            where distro = ? and
            release = ? and
            target = ? and
            subtarget = ? and
            package_sync < NOW() - INTERVAL '1 day';"""
        self.c.execute(sql, distro, release, target, subtarget)
        if self.c.rowcount == 1:
            return True
        else:
            return False

    def insert_packages_available(self, distro, release, target, subtarget, packages):
        self.log.debug("insert packages of {}/{}/{}/{}".format(distro, release, target, subtarget))
        sql = """update subtargets set package_sync = NOW()
            where distro = ? and
            release = ? and
            target = ? and
            subtarget = ?;"""
        self.c.execute(sql, distro, release, target, subtarget)

        sql = """INSERT INTO packages_available VALUES (?, ?, ?, ?, ?, ?);"""
        for package in packages:
            name, version = package
            self.c.execute(sql, distro, release, target, subtarget, name, version)
        self.commit()

    def get_packages_available(self, distro, release, target, subtarget):
        self.log.debug("get_available_packages for %s/%s/%s/%s", distro, release, target, subtarget)
        self.c.execute("""SELECT name, version
            FROM packages_available
            WHERE distro=? and release=? and target=? and subtarget=?;""",
            distro, release, target, subtarget)
        response = {}
        for name, version in self.c.fetchall():
            response[name] = version
        return response

    def insert_subtargets(self, distro, release, target, subtargets):
        self.log.info("insert subtargets %s/%s ", target, " ".join(subtargets))
        sql = "INSERT INTO subtargets (distro, release, target, subtarget) VALUES (?, ?, ?, ?);"
        for subtarget in subtargets:
            self.c.execute(sql, distro, release, target, subtarget)

        self.commit()

    def get_subtargets(self, distro, release, target="%", subtarget="%"):
        self.log.debug("get_subtargets {} {} {} {}".format(distro, release, target, subtarget))
        return self.c.execute("""SELECT target, subtarget, supported FROM subtargets
            WHERE distro = ? and release = ? and target LIKE ? and subtarget LIKE ?;""",
            distro, release, target, subtarget).fetchall()

    def check_request(self, request):
        self.log.debug("check_request")
        request_array = request.as_array()
        request_hash = get_hash(" ".join(request_array), 12)
        sql = """select id, request_hash, status from image_requests
            where request_hash = ?"""
        self.c.execute(sql, request_hash)
        if self.c.rowcount > 0:
            return self.c.fetchone()
        else:
            self.log.debug("add build job")
            sql = """INSERT INTO image_requests
                (request_hash, distro, release, target, subtarget, profile, packages_hash, network_profile)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            self.c.execute(sql, request_hash, *request_array)
            self.commit()
            return(0, '', 'requested')

    def request_imagebuilder(self, distro, release, target, subtarget):
        sql = """INSERT INTO image_requests
            (distro, release, target, subtarget, status)
            VALUES (?, ?, ?, ?, ?)"""
        self.c.execute(sql, distro, release, target, subtarget, "imagebuilder")
        self.commit()

    def add_image(self, image_hash, image_array, checksum, filesize):
        self.log.debug("add image %s", image_array)
        sql = """INSERT INTO images
            (image_hash, distro, release, target, subtarget, profile, manifest_hash, network_profile, checksum, filesize, build_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        self.c.execute(sql, image_hash, *image_array, checksum, filesize, datetime.datetime.now())
        self.commit()
        sql = """select id from images where image_hash = ?"""
        self.c.execute(sql, image_hash)
        if self.c.rowcount > 0:
            return self.c.fetchone()[0]
        else:
            return False

    def add_manifest(self, manifest_hash):
        sql = """INSERT INTO manifest_table (hash) VALUES (?) ON CONFLICT DO NOTHING;"""
        self.c.execute(sql, manifest_hash)
        self.commit()
        sql = """select id from manifest_table where hash = ?;"""
        self.c.execute(sql, manifest_hash)
        return self.c.fetchone()[0]

    def add_manifest_packages(self, manifest_hash, packages):
        self.log.debug("add manifest packages")
        for package in packages:
            name, version = package
            sql = """INSERT INTO manifest_packages (manifest_hash, name, version) VALUES (?, ?, ?);"""
            self.c.execute(sql, manifest_hash, name, version)
        self.commit()

    def get_build_job(self, distro='%', release='%', target='%', subtarget='%'):
        self.log.debug("get build job %s %s %s %s", distro, release, target, subtarget)
        sql = """UPDATE image_requests
            SET status = 'building'
            FROM packages_hashes
            WHERE image_requests.packages_hash = packages_hashes.hash and
                distro LIKE ? and
                release LIKE ? and
                target LIKE ? and
                subtarget LIKE ? and
                image_requests.id = (
                    SELECT MIN(id)
                    FROM image_requests
                    WHERE status = 'requested' and
                    distro LIKE ? and
                    release LIKE ? and
                    target LIKE ? and
                    subtarget LIKE ?
                )
            RETURNING image_requests.id, image_hash, distro, release, target, subtarget, profile, packages_hashes.packages, network_profile;"""
        self.c.execute(sql, distro, release, target, subtarget, distro, release, target, subtarget)
        if self.c.description:
            self.log.debug("found image request")
            self.commit()
            return self.c.fetchone()
        self.log.debug("no image request")
        return None

    def set_image_requests_status(self, image_request_hash, status):
        self.log.info("set image {} status to {}".format(image_request_hash, status))
        sql = """UPDATE image_requests
            SET status = ?
            WHERE request_hash = ?;"""
        self.c.execute(sql, status, image_request_hash)
        self.commit()

    def done_build_job(self, request_hash, image_hash):
        self.log.info("done build job: rqst %s img %s", request_hash, image_hash)
        sql = """UPDATE image_requests SET
            status = 'created',
            image_hash = ?
            WHERE request_hash = ?;"""
        self.c.execute(sql, image_hash, request_hash)
        self.commit()

    def imagebuilder_status(self, distro, release, target, subtarget):
        sql = """select 1 from worker_imagebuilder
            WHERE distro=? and release=? and target=? and subtarget=?;"""
        self.c.execute(sql, distro, release, target, subtarget)
        if self.c.rowcount > 0:
            return "ready"
        else:
            self.log.debug("add imagebuilder request")
            sql = """insert into imagebuilder_requests
                (distro, release, target, subtarget)
                VALUES (?, ?, ?, ?)"""
            self.c.execute(sql, distro, release, target, subtarget)
            self.commit()
            return 'requested'

    def set_imagebuilder_status(self, distro, release, target, subtarget, status):
        sql = """UPDATE imagebuilder SET status = ?
            WHERE distro=? and release=? and target=? and subtarget=?"""
        self.c.execute(sql, status, distro, release, target, subtarget)
        self.commit()

    def get_imagebuilder_request(self):
        sql = """UPDATE imagebuilder
            SET status = 'initialize'
            WHERE status = 'requested' and id = (
                SELECT MIN(id)
                FROM imagebuilder
                WHERE status = 'requested'
                )
            RETURNING distro, release, target, subtarget;"""
        self.c.execute(sql)
        if self.c.description:
            self.commit()
            return self.c.fetchone()
        else:
            return None

    def worker_active_subtargets(self):
        self.log.debug("worker active subtargets")
        sql = """select distro, release, target, subtarget from worker_skills_subtargets, subtargets
                where worker_skills_subtargets.subtarget_id = subtargets.id"""
        self.c.execute(sql)
        result = self.c.fetchall()
        return result

    def worker_needed(self):
        self.log.info("get needed worker")
        sql = """(select * from imagebuilder_requests union
            select distro, release, target, subtarget
                from worker_needed, subtargets
                where worker_needed.subtarget_id = subtargets.id) limit 1"""
        self.c.execute(sql)
        result = self.c.fetchone()
        self.log.debug("need worker for %s", result)
        return result

    def worker_register(self, name=datetime.datetime.now(), address=""):
        self.log.info("register worker %s %s", name, address)
        sql = """INSERT INTO worker (name, address, heartbeat)
            VALUES (?, ?, ?)
            RETURNING id;"""
        self.c.execute(sql, name, address, datetime.datetime.now())
        self.commit()
        return self.c.fetchone()[0]

    def worker_destroy(self, worker_id):
        self.log.info("destroy worker %s", worker_id)
        sql = """delete from worker where id = ?"""
        self.c.execute(sql, worker_id)
        self.commit()

    def worker_add_skill(self, worker_id, distro, release, target, subtarget, status):
        self.log.info("register worker skill %s %s", worker_id, status)
        sql = """INSERT INTO worker_skills
            select ?, subtargets.id, ? from subtargets
            WHERE distro = ? and release = ? and target LIKE ? and subtarget = ?;
            delete from imagebuilder_requests
            WHERE distro = ? and release = ? and target LIKE ? and subtarget = ?;"""
        self.c.execute(sql, worker_id, status, distro, release, target, subtarget, distro, release, target, subtarget)
        self.commit()

    def worker_heartbeat(self, worker_id):
        self.log.debug("heartbeat %s", worker_id)
        sql = "UPDATE worker SET heartbeat = NOW() WHERE id = ?"
        self.c.execute(sql, worker_id)
        self.commit()
