#!/usr/bin/env python

import os
import sys
import json
import logging
from argparse import ArgumentParser
from time import sleep


import pika


logger = None
SYSTEM = 'PROTO'
COMPONENT = 'work-manager'
WORK_QUEUE = None
STATUS_QUEUE = None


# Standard logging filter for using Mesos
class LoggingFilter(logging.Filter):
    def __init__(self, system='', component=''):
        super(LoggingFilter, self).__init__()

        self.system = system
        self.component = component

    def filter(self, record):
        record.system = self.system
        record.component = self.component

        return True


# Standard logging formatter with special execption formatting
class ExceptionFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        std_fmt = ('%(asctime)s.%(msecs)03d'
                   ' %(levelname)-8s'
                   ' %(system)s'
                   ' %(component)s'
                   ' %(message)s')
        std_datefmt = '%Y-%m-%dT%H:%M:%S'

        if fmt is not None:
            std_fmt = fmt

        if datefmt is not None:
            std_datefmt = datefmt

        super(ExceptionFormatter, self).__init__(fmt=std_fmt,
                                                 datefmt=std_datefmt)

    def formatException(self, exc_info):
        result = super(ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


# Configure the message logging components
def setup_logging(args):

    global logger

    # Setup the logging level
    logging_level = logging.INFO
    if args.debug:
        logging_level = args.debug

    handler = logging.StreamHandler(sys.stdout)
    msg_formatter = ExceptionFormatter()
    msg_filter = LoggingFilter(SYSTEM, COMPONENT)

    handler.setFormatter(msg_formatter)
    handler.addFilter(msg_filter)

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def retrieve_command_line():
    """Read and return the command line arguments
    """

    description = 'Prototype Work Manager'
    parser = ArgumentParser(description=description)

    parser.add_argument('--job-filename',
                        action='store',
                        dest='job_filename',
                        required=False,
                        metavar='TEXT',
                        help='JSON job file to use')

    parser.add_argument('--dev-mode',
                        action='store_true',
                        dest='dev_mode',
                        required=False,
                        default=False,
                        help='Run in developer mode')

    parser.add_argument('--debug',
                        action='store',
                        dest='debug',
                        required=False,
                        type=int,
                        default=0,
                        choices=[0,10,20,30,40,50],
                        metavar='DEBUG_LEVEL',
                        help='Log debug messages')

    return parser.parse_args()


def get_messaging_connection_string():
    # Get the connection string
    # Example: amqp://<username>:<password>@<host>:<port>
    connection_var = 'PROTO_MESSAGING_SERVICE_CONNECTION_STRING'
    connection_string = os.environ.get(connection_var, None)
    if not connection_string:
        raise RuntimeError('You must specify {} in the environment'
                           .format(connection_var))

    return connection_string


def get_jobs(job_filename):
    """Reads jobs from a known job file location
    """

    jobs = list()

    if job_filename and os.path.isfile(job_filename):
        with open(job_filename, 'r') as input_fd:
            data = input_fd.read()

        job_dict = json.loads(data)
        del data

        for job in job_dict['jobs']:
            jobs.append(job)

        os.unlink(job_filename)

    return jobs


def main():

    global WORK_QUEUE
    global STATUS_QUEUE

    work_queue_var = 'PROTO_WORK_QUEUE'
    WORK_QUEUE = os.environ.get(work_queue_var, None)
    if not WORK_QUEUE:
        raise RuntimeError('You must specify {} in the environment'
                           .format(work_queue_var))

    status_queue_var = 'PROTO_STATUS_QUEUE'
    STATUS_QUEUE = os.environ.get(status_queue_var, None)
    if not STATUS_QUEUE:
        raise RuntimeError('You must specify {} in the environment'
                           .format(status_queue_var))

    args = retrieve_command_line()

    # Configure logging
    setup_logging(args)

    # Get the connection string
    connection_string = get_messaging_connection_string()

    # Create the connection parameters
    connection_parms = pika.connection.URLParameters(connection_string)

    queue_properties = properties=pika.BasicProperties(delivery_mode=2)

    logger.info('Beginning Processing')

    try:
        while(True):
            try:
                # Create the connection
                connection = pika.BlockingConnection(connection_parms)

                try:
                    # Open a channel
                    channel = connection.channel()
                    # Create/assign the queue to use
                    channel.queue_declare(queue=WORK_QUEUE, durable=True)

                    jobs = get_jobs(args.job_filename)
                    for job in jobs:
                        message_json = json.dumps(job, ensure_ascii=False)

                        if channel.basic_publish(exchange='',
                                                 routing_key=WORK_QUEUE,
                                                 body=message_json,
                                                 properties=queue_properties,
                                                 mandatory=True):
                            print('Queued Message = {}'.format(message_json))
                        else:
                            # TODO TODO TODO - Something needs to be done when this fails
                            print('Returned Message = {}'.format(message_json))

                finally:
                    # Make sure the channel gets closed
                    try:
                        channel.close()
                    except pika.exceptions.ChannelClosed:
                        pass

            finally:
                # Make sure the connection gets closed
                try:
                    connection.close()
                except pika.exceptions.ConnectionClosed:
                    pass

            sleep(60)

    except KeyboardInterrupt:
        pass

    logger.info('Terminated Processing')


if __name__ == '__main__':
    main()
