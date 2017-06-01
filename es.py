#!/usr/bin/python

# -*- coding: utf-8 -*-

import argparse

from elasticsearch import Elasticsearch

import yaml

from action import ACTIONS


def parse_conf(path):
    try:
        with open(path) as fd:
            conf = yaml.safe_load(fd)

        if not conf:
            raise ValueError('Empty file')

    except IOError:
        print '{} not found on disk.'.format(path)
    except ValueError as exc:
        print 'Invalid conf: {}'.format(exc)
    else:
        return conf


def initialize_es_connexion(conf, profile):
    try:
        p = conf[profile]
        http_auth = '{}:{}'.format(p.get('username'), p.get('password'))
        es = Elasticsearch(host=p.get('host'),
                           port=p.get('port'),
                           use_ssl=p.get('use_ssl'),
                           http_auth=http_auth)

        assert es.ping()

    except KeyError:
        print 'The profile {} does not exist'.format(profile)
    except AssertionError:
        print 'The cluster {} is not pingable'.format(p.get('host'))
    else:
        return es


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--profile',
                        required=True,
                        help='Elasticsearch profile from config.yaml')
    parser.add_argument('-c',
                        '--conf',
                        required=False,
                        default="./config.yaml",
                        help='Location of the yaml conf file')
    subparsers = parser.add_subparsers(help="actions")

    for action_cls in ACTIONS:
        action_cls.parser(subparsers)

    args = parser.parse_args()

    conf = parse_conf(args.conf)

    if not conf:
        return

    es = initialize_es_connexion(conf, args.profile)

    if not es:
        return

    args.action_cls(es).do(args)


if __name__ == '__main__':
    main()
