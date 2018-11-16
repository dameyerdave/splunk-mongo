import sys, time, re, os, sys
from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from datetime import datetime
import dateutil.parser
from itertools import chain
import backports.configparser as configparser
from pymongo import MongoClient
from bson.json_util import dumps

#@Configuration(streaming=True, local=True, generates_timeorder=True)
@Configuration()
class MongoConnectCommand(GeneratingCommand):
    """ %(synopsis)

    ##Syntax

    %(syntax)

    ##Description

    %(description)

    ##TODO:

    """
    
    s = Option(require=False)
    db = Option(require=False, default='test') 
    col = Option(require=False, default='tweets') 

    _mongo_conf = configparser.ConfigParser()
    _mongo_conf.read(os.path.dirname(__file__) + '/../default/mongo.conf')
    _props_conf = configparser.ConfigParser()
    _props_conf.read(os.path.dirname(__file__) + '/../default/props.conf')
    _transforms_conf = configparser.ConfigParser()
    _transforms_conf.read(os.path.dirname(__file__) + '/../default/transforms.conf')

    _client = MongoClient(
	    host='127.0.0.1', 
	    port=27017,
        username='mongo',
	    password='secret',
	    authSource='admin')

    kv = re.compile(r"\b(\w+)\s*?=\s*([^=]*)(?=\s+\w+\s*=|$)")
    re_alias = re.compile(r"(\w+) as (\w+)")

    # Add more strings that confuse the parser in the list
    UNINTERESTING = set(chain(dateutil.parser.parserinfo.JUMP,
                      dateutil.parser.parserinfo.PERTAIN,
                      ['a']))

    _extracts = {}
    _transforms = {}
    _aliases = {}

    def _get_date(self, tokens):
        for end in xrange(len(tokens), 0, -1):
            region = tokens[:end]
            if all(token.isspace() or token in self.UNINTERESTING
                   for token in region):
                continue
            text = ''.join(region)
            try:
                date = dateutil.parser.parse(text)
                return end, date
            except ValueError:
                pass

    def find_dates(self, text, max_tokens=50, allow_overlapping=False):
        tokens = filter(None, re.split(r'(\S+|\W+)', text))
        skip_dates_ending_before = 0
        for start in xrange(len(tokens)):
            region = tokens[start:start + max_tokens]
            result = self._get_date(region)
            if result is not None:
                end, date = result
                if allow_overlapping or end > skip_dates_ending_before:
                    skip_dates_ending_before = end
                    yield date

    def init(self):
        # Initialize sourcetypes, props, aliases and transforms
        for sourcetype in self._props_conf:
            for key, value in self._props_conf[sourcetype].items():
                if key.startswith('extract-'):
                    if not sourcetype in self._extracts:
                        self._extracts[sourcetype] = []
                    self._extracts[sourcetype].append(re.compile(value.replace('?<', '?P<')))
                if key.startswith('report-'):
                    if not sourcetype in self._transforms:
                        self._transforms[sourcetype] = []
                    if value in self._transforms_conf:
                        delim = self._transforms_conf[value]['DELIMS'].replace('"', '')
                        fields = self._transforms_conf[value]['FIELDS'].replace('"', '').split(',')
                        transform = {}
                        transform['delim'] = delim
                        transform['fields'] = fields
                        self._transforms[sourcetype].append(transform)
                if key.startswith('fieldalias-'):
                    if not sourcetype in self._aliases:
                        self._aliases[sourcetype] = {}
                    match = self.re_alias.match(value)
                    if match:
                        field, alias = match.groups()
                        self._aliases[sourcetype][field] = alias

        # Initialize database
        self.database = self._client[self.db]
        #self.collection = self.database[self.col]

    def generate(self):
        self.init()

        rets = []
        fields = {}

        q = {}
        if self.s:
            q = { '$text': { '$search': self.s } }
        collections = self.col.split(',')
        for collection in collections:
            for doc in self.database[collection].find(q):
                ret = {}
                try:
                    try:
                        for date in self.find_dates(doc['_time'], allow_overlapping=False):
                            ret['_time'] = date.strftime("%s.%f")
                            #break
                    except Exception as e:
                        ret['_raw'] = "Error: %s." % str(e)
                        #break
                    if not '_time' in ret:
                        ret['_time'] = time.time()
                    ret['_raw'] = str(doc['message']) if 'message' in doc else dumps(doc)
                    ret['source'] = doc['source'] if 'source' in doc else self.col
                    ret['sourcetype'] = doc['sourcetype'] if 'sourcetype' in doc else 'legacy'
                    sourcetype = ret['sourcetype']
                    for field in doc:
                        ret[field] = doc[field]
                    for (field, value) in self.kv.findall(ret['_raw']):
                        ret[field] = value.replace('"', '')
                    if sourcetype in self._extracts:
                        for extract in self._extracts[sourcetype]:
                            match = extract.search(ret['_raw'])
                            if match:
                                for field, value in match.groupdict().items():
                                    ret[field] = value
                    if sourcetype in self._transforms:
                        for transform in self._transforms[sourcetype]:
                            f = 0
                            for value in (list(reader([ret['_raw']], delimiter=str(transform['delim'])))[0]):
                                if f >= len(transform['fields']):
                                    break
                                if transform['fields'][f] != '':
                                    ret[transform['fields'][f]] = value
                                f = f + 1
                    if sourcetype in self._aliases:
                        for field, value in ret.items():
                            if field in self._aliases[sourcetype]:
                                ret[self._aliases[sourcetype][field]] = ret[field]
                    for field in ret:
                        if not field in fields:
                            fields[field] = 1
                except Exception as e:
                    ret['_raw'] = "Error: %s." % str(e)
                    pass
                rets.append(ret)
        for ret in rets:
            for field in fields:
                if not field in ret:
                    ret[field] = ''
            yield ret

dispatch(MongoConnectCommand, sys.argv, sys.stdin, sys.stdout, __name__)
