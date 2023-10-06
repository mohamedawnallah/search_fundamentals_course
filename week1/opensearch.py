from flask import g, current_app
from conn import connect_opensearch

# Create an OpenSearch client instance and put it into Flask shared space for use by the application
def get_opensearch(host='127.0.0.1',port=9200, auth=('OS_NAME','OS_PASSWORD')):
    if 'opensearch' not in g:
        client = connect_opensearch(host, port, auth)
        g.opensearch = client
    return g.opensearch
