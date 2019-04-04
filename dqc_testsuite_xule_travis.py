# Copyright 2015-2019 Altova GmbH
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__copyright__ = 'Copyright 2015-2019 Altova GmbH'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# Executes the XBRL US Data Quality Committee conformance test suite (Travis CI format).
#
# This script drives Altova RaptorXML+XBRL to execute the DQC test suite files in
# https://github.com/DataQualityCommittee/dqc_us_rules/blob/master/.travis.yml
#
# Example usage:
#
# Show available options
#   raptorxmlxbrl script dqc_testsuite_xule_travis.py -h
# Create a CSV summary file
#   raptorxmlxbrl script dqc_testsuite_xule_travis.py /path/to/dqc_us_rules/ --log dqc_testsuite.log --csv-report dqc_testsuite.csv
# Create an XML summary file
#   raptorxmlxbrl script dqc_testsuite_xule_travis.py /path/to/dqc_us_rules/ --log dqc_testsuite.log --xml-report dqc_testsuite.xml
# Run only specific testcases
#   raptorxmlxbrl script dqc_testsuite_xule_travis.py /path/to/dqc_us_rules/ --log dqc_testsuite.log --csv-report dqc_testsuite.xml --testcase DQC_0004 DQC_0005

import argparse
import collections
import concurrent.futures
import datetime
import json
import logging
import multiprocessing
import os
import pickle
import re
import tempfile
import time
import urllib.parse
import urllib.request
import yaml
import zipfile

from altova_api.v2 import xml, xsd, xbrl, beta, ProductInfo
xbrl.xule = beta.xbrl.xule

Result = collections.namedtuple('Result', ['code', 'message', 'severity'])

class ValidationError(Exception):
    """User-defined exception representing a validation error."""

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

def load_instance(file,catalog=None):
    if file.endswith('.htm'):
        docs, log = xbrl.InlineXBRLDocumentSet.transform_xbrl_from_url(file,catalog=catalog)
        if log.has_errors():
            raise ValidationError(log)
        inst = docs[None]
    else:
        inst, log = xbrl.Instance.create_from_url(file,catalog=catalog)
        if log.has_errors():
            raise ValidationError(log)
    return inst        
        
def write_doc(path, content, mode='wb'):
    dir, file = os.path.split(path)
    if not os.path.exists(dir):
        os.makedirs(dir)
    f = open(path, mode)
    f.write(content)
    f.close()

def download_url(url, path):
    content = urllib.request.urlopen(url).read()
    write_doc(path, content, 'wb')

def remote_url_to_path(url, target_dir):
    url_parts = urllib.parse.urlparse(url)
    path = url_parts.path[1:] if url_parts.path.startswith('/') else url_parts.path
    return os.path.join(target_dir, url_parts.netloc, path)

def is_remote(url):
    url_parts = urllib.parse.urlparse(url)
    return len(url_parts.scheme)>0 and url_parts.scheme != 'file'

def collect_remote_urls(testsuite, catalog=None):
    for i, entry in enumerate(testsuite['variations']):
        print('%d/%d'%(i+1,len(testsuite['variations'])), entry['file'])
        if is_remote(entry['file']):
            yield entry['file']
        try:
            inst = load_instance(entry['file'] if is_remote(entry['file']) else os.path.join(root_dir,entry['file']), catalog=catalog)
            for doc in inst.dts.documents:
                if is_remote(doc.uri):
                    yield doc.uri
        except Exception as ex:
            print(ex)        
        
def download_remote_urls(testsuite, target_dir, catalog=None):
    entries = []
    urls = [
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/master/dqc_us_rules/resources/DQC_US_0011/dqc_0011.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/master/dqc_us_rules/resources/DQC_US_0015/dqc_15_concepts.csv',        
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v6/dqc_us_rules/resources/DQC_US_0011/dqc_0011.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v6/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2015_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v6/dqc_us_rules/resources/DQC_US_0015/dqc_15_dei_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0011/dqc_0011.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2016_concepts.csv',        
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2017_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2018_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0015/dqc_15_dei_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v7/dqc_us_rules/resources/DQC_US_0015/dqc_15_srt_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0011/dqc_0011.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2016_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2017_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0015/dqc_15_usgaap_2018_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0015/dqc_15_dei_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0015/dqc_15_srt_concepts.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0079/dqc_0079.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_US_0079/dqc_0079.csv',
        'https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/dqc_us_rules/resources/DQC_IFRS_0080/dqc_0080_ifrs_2018_concepts.csv',
    ]
    urls.extend(list(collect_remote_urls(testsuite, catalog=catalog)))
    for url in urls:
        print(url)
        path = remote_url_to_path(url, target_dir)
        print(path)
        if not os.path.exists(path):
            download_url(url, path)
        entries.append((url, path))
    return entries

def write_catalog(catalog_dir, entries):
    with open(os.path.join(catalog_dir,'catalog.xml'),'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<catalog xmlns="urn:oasis:names:tc:entity:xmlns:xml:catalog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n')
        for url, path in entries:
            f.write('\t<uri name="{url}" uri=".{path}"/>\n'.format(url=url, path=path[len(catalog_dir):].replace('\\','/')))
        f.write("</catalog>")

def create_catalog(testsuite, root_dir, catalog=None):
    catalog_dir = os.path.join(root_dir, 'tests', 'input')
    entries = download_remote_urls(testsuite, catalog_dir, catalog=catalog)
    write_catalog(catalog_dir, entries)                

def load_catalog(root_dir):
    catalog_path = os.path.join(root_dir, 'tests', 'input', 'catalog.xml')
    logging.info('Loading catalog %s', catalog_path)
    catalog, log = xml.Catalog.create_from_url(catalog_path)
    # Check for any fatal errors
    if not catalog:
        raise ValidationError(log)
    return catalog

def setup_xule_processor(root_dir, catalog=None):
    xp = xbrl.xule.Processor(catalog=catalog)
    with open(os.path.join(root_dir,'plugin','xule','rulesetMap.json')) as f:
        for ns, path in json.load(f).items():
            ruleset_path = os.path.join(root_dir,'dqc_us_rules',path.split('?')[0].split('/dqc_us_rules/')[-1])
            logging.info('Loading ruleset %s', ruleset_path)            
            try:
                xp.add_ruleset(ruleset_path, ns)
            except:
                logging.exception('Error loading ruleset %s', ruleset_path)
    return xp

def load_results(file):
    inst, log = xml.Instance.create_from_url(file)
    if log.has_errors():
        raise Exception(log)
    
    results = set()
    for entry in inst.document_element.element_children():
        if 'DQC' in entry.find_attribute('code').normalized_value:
            message = entry.find_child_element('message')
            results.add(Result(
                entry.find_attribute('code').normalized_value,
                message.text_content(),
                message.find_attribute('severity').normalized_value
            ))
    return results

def load_testsuite(root_dir):
    index_path = os.path.join(root_dir, ".travis.yml")
    logging.info('Loading testsuite %s', index_path)
    variations = []
    with open(index_path) as f:
        y = yaml.load(f)
        vars = dict(re.match('([^=]+)=(.+)',x).groups() for x in y['env']['global'])
        for row in y['env']['matrix']:
            infiles = json.loads(re.search('INFILES=\'([^\']*)\'',row).group(1))
            exfiles = re.search('EXFILES=(.*)',row).group(1).split(',')
            for infile, exfile in zip(infiles,exfiles):
                if not is_remote(infile['file']):
                    infile['file'] = os.path.join(root_dir, infile['file'])
                for var in vars:
                    exfile = exfile.replace('$'+var,vars[var])
                variations.append({**infile, 'expected': os.path.join(root_dir, exfile)})
    return {
        'uri': index_path,
        'variations': variations
    }

def execute_variation(variation, xp, catalog):
    logging.info('[%s: %s] Validating instance', variation['file'], variation['xule_run_only'])
    inst = load_instance(variation['file'], catalog)

    logging.info('[%s: %s] Start executing rule', variation['file'], variation['xule_run_only'])
    results = set()
    for result in xp.execute(inst,variation['xule_run_only']):
        if variation['xule_run_only'] in result.effective_rule_name:
            if result.rule_focus:
                focus_element = result.rule_focus.element.source_element if result.rule_focus.element.document.uri.startswith('generated://') else result.rule_focus.element
                uri = focus_element.document.unmapped_uri
                if not is_remote(uri):
                    uri = uri.split('/')[-1]
                line = focus_element.line_number
                results.add(Result(
                    result.effective_rule_name,
                    '[{code}] {msg} - {uri} {line}'.format(code=result.effective_rule_name, msg=result.message, uri=uri, line=line),
                    result.severity.name.lower()
                ))
            else:
                results.add(Result(
                    result.effective_rule_name,
                    '[{code}] {msg} - '.format(code=result.effective_rule_name, msg=result.message),
                    result.severity.name.lower()
                ))
    
    expected = load_results(variation['expected'])
    if results == expected:
        conformance = 'PASS'
    else:
        actual = sorted(results)
        expected = sorted(expected)
        if len(actual) != len(expected):
            conformance = 'INVALID NUMBER OF EVALUATIONS'
        else:
            for i in range(len(actual)):
                if actual[i] != expected[i]:
                    if actual[i].message.split(' -')[:-1] == expected[i].message.split(' -')[:-1]:
                        conformance = 'INVALID LINE LOCATION'
                        logging.debug('INVALID LINE LOCATION, %d' %(i))
                        break
                    else:
                        conformance = 'INVALID'
                        logging.debug('INVALID: %s <=> %s' %(actual[i], expected[i]))

    logging.info('[%s: %s] Finished executing rule: %s', variation['file'], variation['xule_run_only'], conformance)
    logging.debug('[%s: %s] Results:\n%s', variation['file'], variation['xule_run_only'], results)
    return conformance, results

def execute_testsuite(testsuite, args):

    catalog = load_catalog(args.dir)
    xp = setup_xule_processor(args.dir, catalog)

    logging.info('Start executing %s variations', len(testsuite['variations']))
    start = time.time()  
    
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = {}
        for variation in testsuite['variations']:
            if args.variation_uris and variation['file'] not in args.variation_uris:
                continue
            futures[executor.submit(execute_variation, variation, xp, catalog)] = variation
        
        for future in concurrent.futures.as_completed(futures,):
            variation = futures[future]
            variation_key = (variation['file'], variation['xule_run_only'])
            try:
                if variation_key in results:
                    logging.warning('[%s: %s] Duplicate variation', *variation_key)
                results[variation_key] = future.result()
            except:
                results[variation_key] = 'EXCEPTION', collections.Counter()
                logging.exception('[%s: %s] Exception raised during testcase execution:', *variation_key)
                                
    runtime = time.time() - start
    logging.info('Finished executing testcase variations in %fs', runtime)                                
    return results, runtime
       
def calc_conformance(results):
    """Returns a tuple with the number of total and failed testcase variations and the conformance as percentage."""
    total = len(results)
    failed = sum(1 for status, _ in results.values() if status != 'PASS')
    conformance = (total-failed)*100/total if total > 0 else 100
    return total, failed, conformance

def format_uri(uri, base, relative_uris):
    return uri[len(base)+1:] if relative_uris and uri.startswith(base) else uri

def write_csv_report(path, testsuite, results, runtime, relative_uris):
    """Writes testsuite run results to csv file."""
    total, failed, conformance = calc_conformance(results)
    with open(path, 'w') as csvfile:
        testsuite_path, testsuite_index = os.path.split(testsuite['uri'])

        csvfile.write('Date,Total,Failed,Conformance,Runtime,Testsuite,Instance,Rule,Status\n')
        csvfile.write('"{:%Y-%m-%d %H:%M:%S}",{},{},{:.2f},{:.1f},{}\n'.format(datetime.datetime.now(), total, failed, conformance, runtime, testsuite['uri']))
        for variation in testsuite['variations']:
            variation_key = (variation['file'], variation['xule_run_only'])
            if variation_key in results:
                instance_uri = format_uri(variation['file'], testsuite_path, relative_uris)
                status, _ = results[variation_key]
                csvfile.write(',,,,,,{},{},{}\n'.format(instance_uri, variation['xule_run_only'], status))

def xml_escape(str):
    return str.replace('<', '&lt;').replace('&', '&amp;').replace('"', '&quot;')

def write_xml_report(path, testsuite, results, runtime, relative_uris):
    """Writes testsuite run results to xml file."""
    total, failed, conformance = calc_conformance(results)
    with open(path, 'w') as xmlfile:
        testsuite_path, testsuite_index = os.path.split(testsuite['uri'])
        testsuite_uri = testsuite['uri'] if not relative_uris else testsuite_index

        xmlfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xmlfile.write('<testsuite\n\txmlns="http://www.altova.com/testsuite/results"\n')
        if relative_uris:
            xmlfile.write('\txml:base="{}/"\n'.format(testsuite_path))
        xmlfile.write('\turi="{}"\n\ttotal="{}"\n\tfailed="{}"\n\tconformance="{}"\n\truntime="{}"\n\texecution-date="{:%Y-%m-%dT%H:%M:%S}"\n\tprocessor="{}">\n'.format(
            testsuite_uri, total, failed, conformance, runtime, datetime.datetime.now(), ProductInfo.full_product_name))
        for variation in testsuite['variations']:
            variation_key = (variation['file'], variation['xule_run_only'])
            if variation_key in results:
                instance_uri = format_uri(variation['file'], testsuite_path, relative_uris)
                xmlfile.write('\t<variation\n\t\t\tinstance="{}"\n\t\t\trule="{}">\n'.format(instance_uri, variation['xule_run_only']))
                status, _ = results[variation_key]
                xmlfile.write('\t\t<result\n\t\t\t\tstatus="{}" />\n'.format(status))
                xmlfile.write('\t</variation>\n')
        xmlfile.write('</testsuite>\n')

def print_results(testsuite, results, runtime):
    """Writes testsuite run summary to console."""
    total, failed, conformance = calc_conformance(results)
    for variation in testsuite['variations']:
        variation_key = (variation['file'], variation['xule_run_only'])
        if variation_key in results:
            status, _ = results[variation_key]
            if status != 'PASS':
                print('ERROR: Variation %s:%s %s' % (variation['file'], variation['xule_run_only'], status))
    print('Conformance: %.2f%% (%d failed testcase variations out of %d)' % (conformance, failed, total))

def run_xbrl_testsuite(args):
    """Load and execute the conformance testsuite."""
    try:
        testsuite = load_testsuite(args.dir)
        if args.create_catalog:
            create_catalog(testsuite, args.dir)
        results, runtime = execute_testsuite(testsuite, args)
        logging.info('Start generating testsuite report')
        if args.csv_file:
            write_csv_report(args.csv_file, testsuite, results, runtime, args.relative_uris)
        if args.xml_file:
            write_xml_report(args.xml_file, testsuite, results, runtime, args.relative_uris)
        if not args.csv_file and not args.xml_file:
            print_results(testsuite, results, runtime)
        logging.info('Finished generating testsuite report')
    except:
        logging.exception('Testsuite run aborted with exception:')

def setup_logging(args):
    """Initializes Python logging module."""
    if args.log_file:
        levels = {'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename=args.log_file, filemode='w', level=levels[args.log_level])
    else:
        logging.getLogger().addHandler(logging.NullHandler())
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
    logging.getLogger().addHandler(console)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Execute the XBRL US DQC conformance testsuite (.travis.yml) using Altova RaptorXML+XBRL')
    parser.add_argument('dir', metavar='DIR', help='main testsuite directory (including .travis.yml)')
    parser.add_argument('-l', '--log', metavar='LOG_FILE', dest='log_file', help='log output file')
    parser.add_argument('--log-level', metavar='LOG_LEVEL', dest='log_level', choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO', help='log level (ERROR|WARNING|INFO|DEBUG)')
    parser.add_argument('--csv-report', metavar='CSV_FILE', dest='csv_file', help='write testsuite results to csv')
    parser.add_argument('--xml-report', metavar='XML_FILE', dest='xml_file', help='write testsuite results to xml')
    parser.add_argument('--relative-uris', dest='relative_uris', action='store_true', help='write testcase uris relative to testsuite index file')
    parser.add_argument('-v', '--variation', metavar='VARIATION_URI', dest='variation_uris', nargs='*', help='limit execution to only this variation uri')
    parser.add_argument('-w', '--workers', metavar='MAX_WORKERS', type=int, dest='max_workers', default=multiprocessing.cpu_count(), help='limit number of workers')
    parser.add_argument('--create-catalog', dest='create_catalog', action='store_true', help='download all remote files and create a catalog for them')
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_args()

    # Setup logging
    setup_logging(args)

    # Run the testsuite
    run_xbrl_testsuite(args)

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    logging.info('Finished testsuite run in %fs', end-start)
