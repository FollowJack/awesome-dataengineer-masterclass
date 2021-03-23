import logging


logging.basicConfig(filename='example.log', level=logging.DEBUG)


def x():
    logging.debug('22222 This message should go to the log file')
    logging.info('22222 So should this')
    logging.warning('22222 And this, too')
    logging.error('22222 And non-ASCII stuff, too, like Øresund and Malmö')
