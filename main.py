import os
import logging
import logging.config
import json
import sys
import argparse
import timeit

from gdprSHARK.gdprshark import fetch_data, create_and_clean_email_dict, find_and_replace_email
from pycoshark import mongomodels

from mongoengine import connect
from pycoshark.mongomodels import Project
from pycoshark.utils import create_mongodb_uri_string
from pycoshark.utils import get_base_argparser


def setup_logging(default_path=os.path.dirname(os.path.realpath(__file__))+"/loggerConfiguration.json",
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


def str_to_mongomodel(str):
    """Returns mongomodel class named by input string."""
    try:
        return getattr(sys.modules["pycoshark.mongomodels"], str)
    except AttributeError as e:
        raise RuntimeError(f"Error occured: {e}")



def start():
    start = timeit.default_timer()
    setup_logging()
    logger = logging.getLogger("main")
    logger.info("Starting gdprSHARK...")

    parser = get_base_argparser('', '0.0.1')
    parser.add_argument('--fields', help='Database fields to manipulate',
                        default="Commit.message,Message.body,Message.subject,Issue.desc,IssueComment.comment,"
                                "PullRequest.description,PullRequestCommit.message,PullRequestComment.comment")

    parser.add_argument('--debug', help='Sets the debug level.', default='DEBUG',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'])

    args = parser.parse_args()
    uri = create_mongodb_uri_string(args.db_user, args.db_password, args.db_hostname, args.db_port, args.db_authentication, args.ssl)

    connect(args.db_database, host=uri)

    logger.info("load email addresses")
    email_list = fetch_data(mongomodels.People, ["id", "email"], logger, None, 100000)
    email_dict = create_and_clean_email_dict(email_list)

    for db_field_string in args.fields.split(","):
        db_collection, db_field = db_field_string.split(".")
        logger.info(f"start loading and replacing of {db_collection}.{db_field}")
        fields_list = fetch_data(str_to_mongomodel(db_collection), ["id", db_field], logger, None, 100000)

        find_and_replace_email(str_to_mongomodel(db_collection), db_field, fields_list, email_dict, logger)



if __name__ == "__main__":
    start()

