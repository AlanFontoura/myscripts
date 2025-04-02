import sys
import logging
import argparse
import getpass
import json
import requests

from drf_client.connection import Api as RestApi, DEFAULT_HEADERS, RestResource
from drf_client.exceptions import HttpClientError

LOG = logging.getLogger(__name__)


class D1g1tRestResource(RestResource):
    def post(self, data=None, **kwargs):
        """
        Overwrite RestResource 'post' method to handle
         d1g1t 202 'waiting' response status
        """
        if data:
            payload = json.dumps(data)
        else:
            payload = None
        url = self.url()
        headrs = self._get_headers()
        counter = 100
        resp = requests.post(url, data=payload, headers=headrs)
        while resp.status_code == 202 and counter > 0:
            resp = requests.post(url, data=payload, headers=headrs)
            counter -= 1
        return self._process_response(resp)


class D1g1tApi(RestApi):
    def _get_resource(self, **kwargs):
        """Overwrite to use custom D1g1tResource class"""
        return D1g1tRestResource(**kwargs)

    def d1g1t_login(self, password, username):
        assert "LOGIN" in self.options
        data = {"username": username, "password": password}
        url = "{0}/{1}".format(self.base_url, self.options["LOGIN"])

        payload = json.dumps(data)
        r = requests.post(url, data=payload, headers=DEFAULT_HEADERS)
        if r.status_code in [200, 201]:
            content = json.loads(r.content.decode())
            self.token = content["token"]
            self.username = username
            return True

        return False

    def refresh_login(self) -> bool:
        """
        token needs to be refreshed every 4hrs or so!
        :return:
        """
        api_auth = self.api.auth.login.refresh
        r = api_auth.post({"token": self.token})
        if r.status_code in [200, 201]:
            content = json.loads(r.content.decode())
            self.token = content["token"]
            return True


class BaseMain(object):
    parser = None
    args = None
    api = None
    options = {
        "DOMAIN": None,
        "API_PREFIX": "api/v1",
        "TOKEN_TYPE": "jwt",
        "TOKEN_FORMAT": "JWT {token}",
        "LOGIN": "auth/login/",
        "LOGOUT": "auth/logout/",
    }
    logging_level = logging.INFO

    def __init__(self):
        """
        Initialize Logging configuration
        Initialize argument parsing
        Process any extra arguments
        Only hard codes one required argument: --user
        Additional arguments can be configured by overwriting the add_extra_args() method
        Logging configuration can be changed by overwritting the config_logging() method
        """
        self.parser = argparse.ArgumentParser(description=__doc__)
        self.parser.add_argument(
            "-u",
            "--user",
            dest="username",
            type=str,
            required=False,
            help="Username used for login",
        )
        self.parser.add_argument(
            "-s",
            "--server",
            dest="server",
            type=str,
            required=False,
            help="Server Domain Name to use",
        )

        self.add_extra_args()

        self.args = self.parser.parse_args()
        self.config_logging()

    @staticmethod
    def _critical_exit(msg):
        LOG.error(msg)
        sys.exit(1)

    def main(self):
        """
        Main function to call to initiate execution.
        1. Get domain name and use to instantiate Api object
        2. Call before_login to allow for work before logging in
        3. Logging into the server
        4. Call after_loging to do actual work with server data
        """
        self.domain = self.get_domain()
        self.options["DOMAIN"] = self.domain
        self.api = D1g1tApi(self.options)
        self.before_login()
        ok = self.login()
        if ok:
            self.after_login()
        else:
            raise HttpClientError("Your login attempt was unseccessful!")

    # Following functions can be overwritten if needed
    # ================================================

    def config_logging(self):
        """
        Overwrite to change the way the logging package is configured
        :return: Nothing
        """
        logging.basicConfig(
            level=self.logging_level,
            format="[%(asctime)-15s] %(levelname)-6s %(message)s",
            datefmt="%d/%b/%Y %H:%M:%S",
        )

    def add_extra_args(self):
        """
        Overwrite to change the way extra arguments are added to the args resp_prsr
        :return: Nothing
        """
        pass

    def get_domain(self) -> str:
        """
        Figure out server domain URL based on --server and --customer args
        """
        if "https://" not in self.args.server:
            return f"https://{self.args.server}"
        return self.args.server

    def login(self) -> bool:
        """
        Get password from user and login
        """
        password = getpass.getpass()
        ok = self.api.d1g1t_login(username=self.args.username, password=password)
        if ok:
            LOG.info("Welcome {0}".format(self.args.username))
        return ok

    def refresh_login(self) -> None:
        """
        token needs to be refreshed every 4hrs or so!
        :return:
        """
        api_auth = self.api.auth.login.refresh
        tok = api_auth._store["token"]
        r = api_auth.post({"token": tok})
        api_auth._store["token"] = r["token"]
        LOG.info("Token refreshed")

    def before_login(self):
        """
        Overwrite to do work after parsing, but before logging in to the server
        This is a good place to do additional custom argument checks
        :return: Nothing
        """
        pass

    def after_login(self):
        """
        This function MUST be overwritten to do actual work after logging into the Server
        :return: Nothing
        """
        LOG.warning("No actual work done")
