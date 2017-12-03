#!/usr/bin/env python
import os
import sys
import csv
import pika
import json


def emit_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='ISA',
                             exchange_type='topic')

    routing_key = 'input-file.row.read'
    channel.basic_publish(exchange='ISA',
                          routing_key=routing_key,
                          body=json.dumps(message))
    print(" [x] Sent %r:%r" % (routing_key, message))
    connection.close()


def main():
    dir_path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(dir_path, 'intercom_export.csv'), newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=':', quotechar='|')

        keys = next(reader)
        for row in reader:
            print(' [>] Read line: %s' % row)
            emit_message(row[1])


if __name__ == '__main__':
    sys.exit(main())