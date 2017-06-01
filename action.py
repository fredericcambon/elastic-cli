# -*- coding: utf-8 -*-

import json
import yaml

from elasticsearch_dsl import Search

from utils import validate_command, deep_get_attr


class Action(object):
    """Core object to subclass to implement actions."""
    name = None

    def __init__(self, es):
        self.es = es

    @classmethod
    def parser(cls, subparsers):
        pa = subparsers.add_parser(cls.name)
        pa.set_defaults(action_cls=cls)

        return pa

    def do(self, *args, **kwargs):
        raise NotImplementedError


class QuickSearch(Action):
    """
    CLI to do simple searches using ES
    """
    name = 'search'

    @classmethod
    def parser(cls, subparsers):
        p = super(QuickSearch, cls).parser(subparsers)
        p.add_argument('-s',
                       '--save',
                       required=False,
                       action='store_true',
                       help='Save the results of this search')
        p.add_argument('-i',
                       '--index',
                       type=str,
                       required=False,
                       default='*',
                       help='Index to hit, default *')
        p.add_argument('-f',
                       '--filters',
                       required=False,
                       default=[],
                       nargs='*',
                       help='`filter` term')
        p.add_argument('-m',
                       '--matches',
                       required=False,
                       default=[],
                       nargs='*',
                       help='`match` term')
        p.add_argument('-c',
                       '--count',
                       required=False,
                       type=int,
                       default=10,
                       help='number of documents returned')
        p.add_argument('--fields',
                       required=False,
                       default=[],
                       nargs='*',
                       help='fields to be returned, default all')
        p.add_argument('--format',
                       required=False,
                       default='json',
                       help='output format (json, yaml)')
        return p

    def do(self, args):
        search = Search(using=self.es, index=args.index)

        for f in args.filters:
            k, v = f.split(':')
            search = search.filter('term', **{k: v})

        for f in args.matches:
            k, v = f.split(':')
            search = search.query('match', **{k: v})

        response = search[:args.count]

        if not response:
            print u'¯\_(ツ)_/¯ Try again'

        data_format = {
            False: lambda hit: hit,
            True: lambda hit: {f: deep_get_attr(hit, f) for f in args.fields}
        }[bool(args.fields)]

        output_format = {
            'json': lambda data: json.dumps(data),
            'yaml': lambda data: yaml.safe_dump(data,
                                                allow_unicode=True,
                                                default_flow_style=False),
            'raw': lambda data: data.values()
        }[args.format]

        for hit in response:
            print output_format(data_format(hit.to_dict()))


class CatIndices(Action):
    name = 'cat_indices'

    def do(self, *args, **kwargs):
        print "ALIASES"
        try:
            print self.es.cat.aliases()
        except:
            print u'No aliases'

        print "INDICES"
        print self.es.cat.indices()


class CatMapping(Action):
    name = 'cat_mapping'

    def do(self, *args, **kwargs):
        print self.es.indices.get_mapping()


class RestoreSnapshot(Action):
    name = 'restore'

    @classmethod
    def parser(cls, subparsers):
        p = super(RestoreSnapshot, cls).parser(subparsers)
        p.add_argument('snapshot')
        return p

    def do(self, args):
        self.es.snapshot.restore(repository='elastic-logs', snapshot=args.snapshot)


class CatSnapshots(Action):
    name = 'cat_snapshots'

    def do(self, args):
        snapshots = self.es.snapshot.get(repository='elastic-logs', snapshot='*')
        print u'\n'.join([s['snapshot'] for s in snapshots['snapshots']])


class DeleteIndex(Action):
    name = 'delete_index'

    @classmethod
    def parser(cls, subparsers):
        p = super(DeleteIndex, cls).parser(subparsers)
        p.add_argument('index')
        return p

    def do(self, args):
        if validate_command(u'Are you sure you want to delete {}'.format(args.index)):
            self.es.indices.delete(args.index)
            print 'OK'


class ToggleRefreshInternal(Action):
    name = 'toggle_refresh_interval'
    long_refresh_interval = u'3600s'
    short_refresh_interval = u'1s'

    @classmethod
    def parser(cls, subparsers):
        p = super(ToggleRefreshInternal, cls).parser(subparsers)
        p.add_argument('-i',
                       '--index',
                       type=str,
                       required=True,
                       default='*',
                       help='Index to update')

    def do(self, args):
        settings = self.es.indices.get_settings(index=args.index)
        cur_refresh_interval = settings[args.index]['settings']['index']['refresh_interval']
        msg, refresh_interval = {
            self.long_refresh_interval: ('Enabling refresh_interval', self.short_refresh_interval),
            self.short_refresh_interval: ('Disabling refresh_interval', self.long_refresh_interval)
        }[cur_refresh_interval]

        print msg

        self.es.indices.put_settings(body={'refresh_interval': refresh_interval}, index=args.index)


ACTIONS = [DeleteIndex, CatIndices, QuickSearch, RestoreSnapshot, CatSnapshots, CatMapping, ToggleRefreshInternal]
