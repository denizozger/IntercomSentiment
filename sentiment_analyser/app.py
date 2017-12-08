#!/usr/bin/env python
import sys
import pika
import json
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer


sid = SentimentIntensityAnalyzer()


def emit_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='ISA',
                             exchange_type='topic')

    routing_key = 'sentiment-analysis.complete'
    channel.basic_publish(exchange='ISA',
                          routing_key=routing_key,
                          body=json.dumps(message))
    print(" [x] Sent %r:%r" % (routing_key, message))
    connection.close()


def map_scores_to_visuals(score):
    if 1 >= score >= 0.66:
        return '😃'
    elif 0.66 >= score >= 0.33:
        return '😊'
    elif 0.33 >= score > 0:
        return '🙂'
    elif score == 0:
        return '😐'
    elif 0 > score >= -0.33:
        return '🙁'
    elif -0.33 >= score >= -0.66:
        return '😖'
    elif -0.66 >= score >= -1:
        return '😫'
    else:
        return '¯\_(ツ)_/¯'


def analyse_sentence(sentence):
    ss = sid.polarity_scores(sentence)
    compound_score = next(ss[x] for x in sorted(ss) if x == 'compound')
    print('%s %s ' % (map_scores_to_visuals(compound_score), sentence))


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='ISA',
                             exchange_type='topic')

    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='ISA',
                       queue=queue_name,
                       routing_key='input-file.row.read')

    print(' [*] Waiting for logs. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        message = body.decode('ascii')
        # print(" [x] %r:%r" % (method.routing_key, message))
        analyse_sentence(message)

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


if __name__ == '__main__':
    sys.exit(main())