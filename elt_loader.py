import sys
import argparse
import logging
import logging.handlers


from ingestion.extract import DataProvider
from ingestion.transform import DataTransformation
from config.settings import LOGGER_NAME

from ingestion.load import ClientDataLoad


__DELTA_LOAD__ = 'delta-load'
__RECOVERY_LOAD__ = 'recovery-load'


def init_logging(logging_file):
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s[%(name)s] - %(message)s')
    formatter_debug_error = logging.Formatter('%(asctime)s - %(levelname)s[%(name)s]'
                                              '[%(module)s.%(funcName)s] - %(message)s')
    # INFO Handler
    fh = logging.handlers.TimedRotatingFileHandler(logging_file + '_INFO', when='midnight')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # DEBUG Handler
    dh = logging.handlers.TimedRotatingFileHandler(logging_file + '_DEBUG', when='midnight')
    dh.setLevel(logging.DEBUG)
    dh.setFormatter(formatter_debug_error)
    logger.addHandler(dh)
    # WARNING Handler
    wh = logging.handlers.TimedRotatingFileHandler(logging_file + '_WARNING', when='midnight')
    wh.setLevel(logging.WARNING)
    wh.setFormatter(formatter_debug_error)
    logger.addHandler(wh)
    # ERROR Handler
    eh = logging.handlers.TimedRotatingFileHandler(logging_file + '_ERROR', when='midnight')
    eh.setLevel(logging.ERROR)
    eh.setFormatter(formatter_debug_error)
    logger.addHandler(eh)
    # STREAM Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def init_parser():
    """

    :return:
    """
    # Parser
    parser = argparse.ArgumentParser(description='ETL for Load Trips', add_help=True)

    # Entities Group
    entities_group = parser.add_argument_group('Entities to be loaded')
    entities_group.add_argument(
        '--all-entities',
        action='store_true',
        help='transform and load all entities'
    )
    entities_group.add_argument(
        '--jobsity',
        action='store_true',
        help='transform and load trips'
    )

    # Common arguments
    common_group = parser.add_argument_group('Common arguments')
    common_group.add_argument(
        '-l',
        '--log-file',
        type=str,
        required=True,
        help='path to log file'
    )

    return parser

def get_all(inicial_load = False):
    logger = logging.getLogger(LOGGER_NAME)
    try:
        get_trips(inicial_load=inicial_load)
    except Exception as e:
        return logger.error('Export trips with error ' + str(e))
    return logger.info('Export trips with sucess')

def get_trips(inicial_load = False):
    data_load = ClientDataLoad()
    data_provider = DataProvider(inicial_load=inicial_load)
    df = data_provider.get_data()
    # no records, go to next entitie
    if df.empty:
        return True
    data_transform = DataTransformation()
    df = data_transform.transform_data(df)
    message = data_load.load_data(df)
    return message

def main():
    try:
        parser = init_parser()
        args, unknown_args = parser.parse_known_args()
        print_help = False

        if args.all_entities:
            args.jobsity = True

        if args.jobsity:
            init_logging(args.log_file)
            logger = logging.getLogger(LOGGER_NAME)
            logger.info('ETL Started')

            # Entities to create
            jobsity = None

            logger.info('ETL Arguments ')
            logger.debug('ETL Arguments: {}'.format(args))

            if args.jobsity:
                get_all()

            logger = logging.getLogger(LOGGER_NAME)
            logger.info('ETL Finished')
            sys.exit(0)

        else:
            print_help = True

        if print_help:
            logger = logging.getLogger(LOGGER_NAME)
            logger.error('Invalid usage of command etl_loader')
            parser.print_help()
            sys.exit(-1)

    except Exception as e:
        logger = logging.getLogger(LOGGER_NAME)
        logger.error(e)
        raise e


if __name__ == '__main__':
    main()
