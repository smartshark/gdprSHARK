import argparse
import json
import logging
import logging.config
import os
import sys
import timeit

from pycoshark.utils import create_mongodb_uri_string, get_base_argparser
from pymongo import MongoClient
from gdprSHARK.gdprshark import load_email_dict, update_db_with_email_filter


def setup_logging(default_path=os.path.dirname(os.path.realpath(__file__)) + "/loggerConfiguration.json",
                  default_level=logging.INFO):
    """
    Setup logging configuration

    :param default_path: path to the logger configuration
    :param default_level: defines the default logging level if configuration file is not found(default:logging.INFO)
    """
    path = default_path
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def start():
    start = timeit.default_timer()
    setup_logging()
    logger = logging.getLogger("main")
    logger.info("Starting gdprSHARK...")

    parser = get_base_argparser('', '0.0.1')
    parser.add_argument('--fields', help='Database fields to manipulate',
                        default="commit.message,message.body,message.subject,issue.desc,issue_comment.comment,"
                        "pull_request.description,pull_request_commit.message,pull_request_comment.comment")

    parser.add_argument('--debug', help='Sets the debug level.', default='DEBUG',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

    args = parser.parse_args()
    uri = create_mongodb_uri_string(args.db_user, args.db_password, args.db_hostname,
                                    args.db_port, args.db_authentication, args.ssl)
    client = MongoClient(uri)
    db_handle = client[args.db_database]

    valid_collections = db_handle.list_collection_names()
    if "people" not in valid_collections:
        error_msg = f"'The 'people' collection does not exist in the database. "\
                    f"Therefore, no email address list can be generated."
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    for db_field_string in args.fields.split(","):
        db_collection, db_field = db_field_string.split(".")
        if db_collection not in valid_collections:
            error_msg = f"'{db_collection}' collection does not exist in the database."
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    email_dict = load_email_dict(db_handle, logger)

    for db_field_string in args.fields.split(","):
        db_collection, db_field = db_field_string.split(".")
        update_db_with_email_filter(db_handle, db_collection, db_field, email_dict, logger)


if __name__ == "__main__":
    start()
