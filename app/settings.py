from dataclasses import dataclass
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()


# Metaclass for adding postfix to environment variable names
class SettingsMeta(type):
    def __new__(cls, name, bases, dct, postfix=None):
        if postfix:
            for key in dct:
                if not key.startswith('__') and not callable(dct[key]):
                    env_var = os.getenv(f"{key}{postfix}")
                    if env_var is not None:
                        dct[key] = env_var
        return super().__new__(cls, name, bases, dct)


# All modes
@dataclass()
class BaseSettings(metaclass=SettingsMeta):
    EL_PORT = int(os.getenv('EL_PORT'))
    EL_AWS_REGION = os.getenv('EL_AWS_REGION')
    EL_AWS_PROFILE = os.getenv('EL_AWS_PROFILE')
    EL_AWS_SERVICE = os.getenv('EL_AWS_SERVICE')
    EL_USE_IAM = bool(int(os.getenv('EL_USE_IAM')))
    EL_DEFAULT_SCROLL = os.getenv('EL_DEFAULT_SCROLL')
    EL_DEFAULT_BATCH_SIZE = os.getenv('EL_DEFAULT_BATCH_SIZE')
    NUM_THREADS = int(os.getenv('NUM_THREADS'))
    AF_API_VERSION = os.getenv('AF_API_VERSION')


# DEV mode
class DevSettings(BaseSettings, postfix='__DEV'):
    EL_HOST = os.getenv('EL_HOST')
    DB_URL = os.getenv('DB_URL')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')


# TEST mode
class TestSettings(BaseSettings, postfix='__TEST'):
    EL_HOST = os.getenv('EL_HOST')
    DB_URL = os.getenv('DB_URL')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')


# PROD mode
class ProdSettings(BaseSettings, postfix='__PROD'):
    EL_HOST = os.getenv('EL_HOST')
    DB_URL = os.getenv('DB_URL')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')


settings_map = {
    'DEV': DevSettings,
    'TEST': TestSettings,
    'PROD': ProdSettings
}


def get_settings(env="DEV"):
    return settings_map[env]()
