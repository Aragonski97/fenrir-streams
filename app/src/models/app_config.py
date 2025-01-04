import os
from pydantic_settings import BaseSettings
from pydantic import Field



ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '../..'))


class AppConfiguration(BaseSettings):
    #env
    app_env: str
    stream_name: str
    elk_es_log_url: str = "<not implemented yet>, but could easily be done to do so"

    # kafka
    input_topic: str
    output_topic: str
    kafka_payload_key: str
    registry_url: str
    kafka_bootstrap_server: str

    # runtime
    number_of_coroutines: int  # number of workers, higher usually means more performance, but test for your purposes

    # databases
    databases: list


    class Config:
        case_sensitive = False
        secrets_dir = '/run/secrets'
        env_file = os.path.join(ROOT_DIR, '.env')


app_config = AppConfiguration()

