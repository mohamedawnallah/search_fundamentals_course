import os
from opensearchpy import OpenSearch

def connect_opensearch(host='127.0.0.1',port=9200, auth=('OS_NAME','OS_PASSWORD')):
    username, password = validate_os_credentials(auth)
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_compress=True,
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False
        )
    return client

def validate_os_credentials(auth):
    username_env, password_env = auth
    if username_env in os.environ and password_env in os.environ:
        username = os.environ[username_env]
        password = os.environ[password_env]
        return (username, password)
    else:
        raise ValueError(f'Please initialize the env credentials {username_env} and {password_env} for opensearch')
