# From https://github.com/dshvadskiy/search_with_machine_learning_course/blob/main/index_products.py
import opensearchpy
import requests
from lxml import etree

import click
import glob
from opensearchpy.helpers import bulk
import logging

from time import perf_counter
import concurrent.futures

from conn import connect_opensearch

mappings = {
    "productId": "productId/text()",
    "sku": "sku/text()",
    "name":  "name/text()",
    "type": "type/text()",
    "startDate": "startDate/text()",
    "active": "active/text()",
    "regularPrice": "regularPrice/text()",
    "salePrice": "salePrice/text()",
    "artistName": "artistName/text()",
    "onSale": "onSale/text()",
    "digital": "digital/text()",
    "frequentlyPurchasedWith": "frequentlyPurchasedWith/*/text()",
    "accessories": "accessories/*/text()",
    "relatedProducts": "relatedProducts/*/text()",
    "crossSell": "crossSell/text()",
    "salesRankShortTerm": "salesRankShortTerm/text()",
    "salesRankMediumTerm": "salesRankMediumTerm/text()",
    "salesRankLongTerm": "salesRankLongTerm/text()",
    "bestSellingRank": "bestSellingRank/text()",
    "url": "url/text()",
    "categoryPath": "categoryPath/*/name/text()",
    "categoryPathIds": "categoryPath/*/id/text()",
    "categoryLeaf": "categoryPath/category[last()]/id/text()",
    "categoryPathCount": "count(categoryPath/*/name)",
    "customerReviewCount": "customerReviewCount/text()",
    "customerReviewAverage": "customerReviewAverage/text()",
    "inStoreAvailability": "inStoreAvailability/text()",
    "onlineAvailability": "onlineAvailability/text()",
    "releaseDate": "releaseDate/text()",
    "shippingCost": "shippingCost/text()",
    "shortDescription": "shortDescrption/text()",
    "shortDescriptionHtml": "shortDescriptionHtml/text()",
    "class": "class/text()",
    "classId": "classId/text()",
    "subclass": "subclass/text()",
    "subclassId": "subclassId/text()",
    "department": "department/text()",
    "departmentId": "departmentId/text()",
    "bestBuyItemId": "bestBuyItemId/text()",
    "description": "description/text()",
    "manufacturer": "manufacturer/text()",
    "modelNumber": "modelNumber/text()",
    "image": "image/text()",
    "condition": "condition/text()",
    "inStorePickup": "inStorePickup/text()",
    "homeDelivery": "homeDelivery/text()",
    "quantityLimit": "quantityLimit/text()",
    "color": "color/text()",
    "depth": "depth/text()",
    "height": "height/text()",
    "weight": "weight/text()",
    "shippingWeight": "shippingWeight/text()",
    "width": "width/text()",
    "longDescription": "longDescription/text()",
    "longDescriptionHtml": "longDescriptionHtml/text()",
    "features": "features/*/text()",
}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(format='%(levelname)s:%(message)s')

def get_opensearch(host='127.0.0.1',port=9200, auth=('OS_NAME','OS_PASSWORD')):
    client = connect_opensearch(host, port, auth)
    return client

def index_file(file, index_name):
    docs_indexed = 0
    logger.info(f'Processing file : {file}')
    tree = etree.parse(file)
    root = tree.getroot()
    products = root.findall("./product")
    docs = []
    for product in products:
        doc = {}
        for product_field in mappings:
            xpath_expr = mappings[product_field]
            result = product.xpath(xpath_expr)
            if '*' in xpath_expr:
                doc[product_field] = result
            else:
                joined = ''.join(result)
                doc[product_field] = joined if joined else None

        if 'productId' not in doc or not doc['productId']:
            continue

        docs.append({'_index': index_name, '_id': doc['productId'], 'id': doc['productId'], '_source': doc})
    
    for i in range(0, len(docs), 2000):
        docs_subset = docs[i:i+2000]
        bulk(client, docs_subset)
    docs_indexed = len(docs)
    return docs_indexed

@click.command()
@click.option('--source_dir', '-s', help='XML files source directory')
@click.option('--index_name', '-i', default="bbuy_products", help="The name of the index to write to")
@click.option('--workers', '-w', default=8, help="The number of workers to use to process files")
def main(source_dir: str, index_name: str, workers: int):
    global client
    client = get_opensearch()
    files = glob.glob(source_dir + '/*.xml')
    docs_indexed = 0
    start = perf_counter()
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(index_file, file, index_name) for file in files]
        for future in concurrent.futures.as_completed(futures):
            docs_indexed += future.result()

    finish = perf_counter()
    logger.info(f'Done. Total docs: {docs_indexed} in {(finish - start)/60} minutes')

if __name__ == "__main__":
    main()