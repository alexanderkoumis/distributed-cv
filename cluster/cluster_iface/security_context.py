#!/usr/bin/python
"""A security context module.
"""

# Open-Source/Free Imports
import csv
import os

# Private-IP Imports Carlis/Koumis.
# import private_ip_lib

class SecurityContext(object):
    """Abstract Security Context Object.
    """
    def __init__(self):
        pass


def read_external_keyfile(key_file, mapping):
    """Read a key_file into a mapping.
    """
    with open(key_file, 'r') as file_d:
        key_reader = csv.reader(file_d, delimiter='=')
        for key, value in key_reader:
            mapping[key] = value

class AwsSecurityContext(SecurityContext):
    """A Security Context for AWS services.
    """
    ACCESS_KEY = 'AWSAccessKeyId'
    SECRET_KEY = 'AWSSecretKey'
    AWS_KEYS = {ACCESS_KEY: None, SECRET_KEY: None}
    KEY_FILE_PATH = ''

    def __init__(self):
        aws_acess_kenv = 'AWS_ACCESS_KEY_ID'
        aws_secret_kenv = 'AWS_SECRET_ACCESS_KEY'

        if not self.KEY_FILE_PATH:
            # We don't know anything about the environment.
            self._set_keyfile_path()
            read_external_keyfile(self.KEY_FILE_PATH, self.AWS_KEYS)
            self.access_key_id = self.AWS_KEYS[self.ACCESS_KEY]
            self.secret_access_key = self.AWS_KEYS[self.SECRET_KEY]

        if not (os.getenv(aws_acess_kenv) and os.getenv(aws_secret_kenv)):
            # Set environmental variables if they don't exist.
            os.environ[aws_acess_kenv] = self.AWS_KEYS[self.ACCESS_KEY]
            os.environ[aws_secret_kenv] = self.AWS_KEYS[self.SECRET_KEY]

    def _set_keyfile_path(self):
        """Get paths in the environment to the keyfile.
        """
        if not self.KEY_FILE_PATH:
            # We don't have the path of the keyfile, get it.
            self.HOME = os.environ['HOME']
            self.KEY_FILE_PATH = os.path.join(self.HOME,
                                              '.ssh/rootkey.csv')


