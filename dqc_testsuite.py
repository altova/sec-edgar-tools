# Copyright 2015 Altova GmbH
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
__copyright__ = 'Copyright 2015 Altova GmbH'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# Executes the XBRL US Data Quality Committee conformance test suite.
#
# This script drives Altova RaptorXML+XBRL to execute the DQC test suite files in http://github.com/DataQualityCommittee/dqc_us_rules/blob/master/tests/test_suite/DQC_Testcases_Release_All_V1.zip. See http://github.com/DataQualityCommittee/dqc_us_rules/tree/master/tests/test_suite for more information.
#
# Example usage:
#
# Show available options
#   raptorxmlxbrl script dqc_testsuite.py /path/to/DQC_Testcases_Release_All_V1/index.xml -h
# Create a CSV summary file
#   raptorxmlxbrl script dqc_testsuite.py /path/to/DQC_Testcases_Release_All_V1/index.xml --log dqc_testsuite.log --csv-report dqc_testsuite.csv
# Create an XML summary file
#   raptorxmlxbrl script dqc_testsuite.py /path/to/DQC_Testcases_Release_All_V1/index.xml --log dqc_testsuite.log --xml-report dqc_testsuite.xml
# Run only specific testcases
#   raptorxmlxbrl script dqc_testsuite.py /path/to/DQC_Testcases_Release_All_V1/index.xml --log dqc_testsuite.log --csv-report dqc_testsuite.xml --testcase "DQC_0004." "DQC_0005."

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl
import dqc_validation

import argparse,collections,concurrent.futures,datetime,logging,multiprocessing,re,tempfile,time,urllib.parse,urllib.request,zipfile

re_error_code = re.compile(r'\[(DQC\.US\.\d+\.\d+)\] ')

class ValidationError(Exception):
    """User-defined exception representing a validation error."""
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return str(self.value)

def attr_val(elem,attr_name):
    """Returns the value of attribute *attr_name* on element *elem* or None if no such attribute does not exists."""
    attr = elem.find_attribute(attr_name)
    if attr:
        val = attr.schema_normalized_value
        if val is None:
            val = attr.normalized_value
        return val
    return None

def attr_val_bool(elem,attr_name):
    """Returns the boolean value of attribute *attr_name* on element *elem* or None if no such attribute does not exists."""
    attr = elem.find_attribute(attr_name)
    return attr.normalized_value.strip() in ('1','true') if attr else None

def elem_val(elem):
    """Returns the text value of element *elem*."""
    val = elem.schema_normalized_value
    if val is None:
        text = []
        for child in elem.children:
            if isinstance(child,xml.CharDataInformationItem):
                text.append(child.value)
        val = ''.join(text)
    return val

def parse_variation(variation_elem):
    """Parses the <variation> element and returns a dict containing meta-information about the given variation.""" 
    
    variation = {
        'id': attr_val(variation_elem,'id'),
    }

    for elem in variation_elem.element_children():
        if elem.local_name == 'name':
            variation['name'] = elem_val(elem)
        elif elem.local_name == 'description':
            variation['description'] = elem.serialize(omit_start_tag=True)
        elif elem.local_name == 'data':
            data = {
                'instances': [],
                'linkbases': [],
                'schemas': [],
            }
            for elem2 in elem.element_children():
                if elem2.local_name in ('instance','linkbase','schema'):
                    uri = elem_val(elem2)
                    data[elem2.local_name+'s'].append(urllib.parse.urljoin(elem2.base_uri,uri))
                    if attr_val_bool(elem2,'readMeFirst'):
                        data['readMeFirst'] = urllib.parse.urljoin(elem2.base_uri,uri)
                else:
                    logging.warning('Testcase file %s contains unknown <data> child element <%s>',elem2.document.uri,elem2.local_name)
            variation['data'] = data
        elif elem.local_name == 'results':
            results = {
                'blockedMessageCodes': attr_val(elem,'blockedMessageCodes'),
                'errors': {}
            }
            for elem2 in elem.element_children():
                if elem2.local_name == 'error':
                    results['errors'][elem_val(elem2)] = {
                        'severity': attr_val(elem2,'severity'),
                        'count': int(attr_val(elem2,'count'))
                    }
                elif elem2.local_name == 'result':
                    pass
                else:
                    logging.warning('Testcase file %s contains unknown <results> child element <%s>',elem2.document.uri,elem2.local_name)
            variation['results'] = results
        else:
            logging.warning('Testcase file %s contains unknown <variation> child element <%s>',elem.document.uri,elem.local_name)
  
    return variation

def load_testcase(testcase_uri):
    """Loads the testcase file and returns a dict with the testcase meta-information."""
    logging.info('Loading testcase %s',testcase_uri)
    
    # Load the testcase file
    instance, log = xml.Instance.create_from_url(testcase_uri)
    # Check for any fatal errors
    if not instance:
        raise ValidationError('\n'.join(error.text for error in log))
    testcase_elem = instance.document_element
    
    testcase = {
        'uri': instance.uri,
    }    
        
    # Iterate over all <testcase> child elements
    variations = []
    variation_ids = set()
    for elem in testcase_elem.element_children():
        if elem.local_name == 'creator':
            creator = {}
            for elem2 in elem.element_children():
                if elem2.local_name == 'name':
                    creator['name'] = elem_val(elem2)
                elif elem2.local_name == 'email':
                    creator['email'] = elem_val(elem2)
            testcase['creator'] = creator
        elif elem.local_name == 'number':
            testcase['number'] = elem_val(elem)
        elif elem.local_name == 'ruleIdentifier':
            testcase['ruleIdentifier'] = elem_val(elem)
        elif elem.local_name == 'description':
            testcase['description'] = elem.serialize(omit_start_tag=True)
        elif elem.local_name == 'ruleMessage':
            testcase['ruleMessage'] = elem_val(elem)
        elif elem.local_name == 'variation':
            variation = parse_variation(elem)
            variations.append(variation)            
            if variation['id'] in variation_ids:
                logging.warning('Testcase file %s contains variations with duplicate id %s',testcase_uri,variation['id'])
        else:
            logging.warning('Testcase file %s contains unknown <testcase> child element <%s>',elem.document.uri,elem.local_name)
    testcase['variations'] = variations

    return testcase

def load_testsuite(index_uri):
    """Loads the testcases specified in the given testsuite index file and returns a dict with all testcase meta-information."""
    logging.info('Loading testsuite index %s',index_uri)
    
    # Load the testcase index file
    instance, log = xml.Instance.create_from_url(index_uri)
    # Check for any fatal errors
    if not instance:
        raise ValidationError('\n'.join(error.text for error in log))
    documentation_elem = instance.document_element

    testsuite = {
        'uri': instance.uri,
        'name': attr_val(documentation_elem,'name'),
        'date': attr_val(documentation_elem,'date')
    }    
        
    # Iterate over all <testcase> child elements and parse the testcase file
    testcases = []
    for testcases_elem in documentation_elem.element_children():
        if testcases_elem.local_name == 'testcases':
            root = urllib.parse.urljoin(testcases_elem.base_uri,attr_val(testcases_elem,'root')+'/')
            for testcase_elem in testcases_elem.element_children():
                if testcase_elem.local_name == 'testcase':
                    # Get the value of the @uri attribute and make any relative uris absolute to the base uri
                    uri = urllib.parse.urljoin(root,attr_val(testcase_elem,'uri'))
                    # Load the testcase file
                    testcases.append(load_testcase(uri))
    testsuite['testcases'] = testcases
            
    return testsuite
    
def instance_name_from_zip(path):
    """Determines the instance filename within a SEC EDGAR zip archive."""
    re_instance_name = re.compile(r'.+-\d{8}\.xml')
    for name in zipfile.ZipFile(path).namelist():
        if re_instance_name.fullmatch(name):
            return name
    raise RuntimeError('Zip archive does not contain a valid SEC instance file.')

def execute_variation(testcase,variation):
    """Peforms the actual XBRL instance or taxonomy validation and returns 'PASS' if the actual outcome is conformant with the result specified in the variation."""
    logging.info('[%s] Start executing variation',variation['id'])
    
    if 'readMeFirst' in variation['data']:
        if variation['data']['readMeFirst'].endswith('.zip'):
            tmpzip = tempfile.NamedTemporaryFile(suffix='.zip',delete=False).name
            logging.info('Downloading archive %s to %s',variation['data']['readMeFirst'],tmpzip)
            urllib.request.urlretrieve(variation['data']['readMeFirst'],tmpzip)
            uri = 'file:{0}%7Czip/{1}'.format(urllib.request.pathname2url(tmpzip),instance_name_from_zip(tmpzip))
        else:
            uri = variation['data']['readMeFirst']
    else:
        raise RuntimeError('Unknown entry point in variation %s' % variation['id'])

    logging.info('[%s] Validating instance %s',variation['id'],uri)
    instance, error_log = xbrl.Instance.create_from_url(uri,error_limit=200)
    dqc_validation.validate(instance,error_log,{'suppressErrors': variation['results']['blockedMessageCodes']})
    if error_log.has_errors() and logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug('[%s] Error log:\n%s',variation['id'],'\n'.join(error.text for error in error_log))

    error_counts = collections.Counter()
    for error in error_log:
        if error.severity == xml.ErrorSeverity.ERROR:
            m = re_error_code.search(error.text)
            if m:
                error_counts[m.group(1)] += 1
            else:
                error_counts['other'] += 1

    passed = False if len(variation['results']['errors']) == 0 and error_log.has_errors() else True
    for code, error in variation['results']['errors'].items():
        if error['count'] != error_counts[code]:
            passed = False

    logging.info('[%s] Finished executing variation: %s, %s',variation['id'],'PASS' if passed else 'FAIL',dict(error_counts))
    return 'PASS' if passed else 'FAIL', error_counts

def execute_testsuite(testsuite,args):
    """Runs all testcase variations in parallel and returns a dict with the results of each testcase variation."""
    logging.info('Start executing %s variations in %d testcases',sum(len(testcase['variations']) for testcase in testsuite['testcases']),len(testsuite['testcases']))
    start = time.time()
  
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        
        # Schedule processing of all variations as futures
        futures = {}
        for testcase in testsuite['testcases']:
            if args.testcase_numbers and testcase['number'] not in args.testcase_numbers:
                continue
            for variation in testcase['variations']:
                if args.variation_ids and variation['id'] not in args.variation_ids:
                    continue
                futures[executor.submit(execute_variation,testcase,variation)] = (testcase['uri'],variation['id'])
        
        # Wait for all futures to finish
        for future in concurrent.futures.as_completed(futures):
            variation_key = futures[future]
            try:
                results[variation_key] = future.result()
            except:
                results[variation_key] = 'EXCEPTION',collections.Counter()
                logging.exception('Exception raised during testcase execution:')

    runtime = time.time() - start
    logging.info('Finished executing testcase variations in %fs',runtime)
    return results,runtime

def calc_conformance(results):
    """Returns a tuple with the number of total and failed testcase variations and the conformance as percentage."""
    total = len(results)
    failed = sum(1 for status,_ in results.values() if status != 'PASS')
    conformance = (total-failed)*100/total
    return total,failed,conformance

def write_csv_report(path,testsuite,results,runtime,relative_uris):
    """Writes testsuite run results to csv file."""
    total,failed,conformance = calc_conformance(results)
    with open(path,'w') as csvfile:
        testsuite_path, testsuite_index = testsuite['uri'].rsplit('/',1)

        csvfile.write('Date,Total,Failed,Conformance,Runtime,Testsuite,Testcase,Variation,ReadMeFirst,Status,Actual,Expected,Blocked,Warnings\n')
        csvfile.write('"{:%Y-%m-%d %H:%M:%S}",{},{},{:.2f},{:.1f},{}\n'.format(datetime.datetime.now(),total,failed,conformance,runtime,testsuite['uri']))
        for testcase in testsuite['testcases']:
            csvfile.write(',,,,,,%s\n'%testcase['number'])
            for variation in testcase['variations']:
                variation_key = (testcase['uri'],variation['id'])
                if variation_key in results:
                    instance_uri = variation['data']['readMeFirst'] if not relative_uris else variation['data']['readMeFirst'][len(testsuite_path)+1:]          
                    status, error_counts = results[variation_key]
                    actual = ' '.join('%dx%s'%(count,code) for code, count in sorted(error_counts.items()))
                    expected = ' '.join('%dx%s'%(error['count'],code) for code, error in sorted(variation['results']['errors'].items()))
                    blocked = variation['results']['blockedMessageCodes'].replace('|',' ') if variation['results']['blockedMessageCodes'] else ''
                    warnings = ''
                    if status == 'PASS' and len(variation['results']['errors']) != len(error_counts):
                        additional_errors = set(error_counts.keys()) - set(variation['results']['errors'])
                        warnings = 'Additional errors %s reported' % ' '.join(sorted(additional_errors))
                    csvfile.write(',,,,,,,{},{},{},{},{},{},{}\n'.format(variation['id'],instance_uri,status,actual,expected,blocked,warnings))

def xml_escape(str):
    return str.replace('<','&lt;').replace('&','&amp;').replace('"','&quot;')

def write_xml_report(path,testsuite,results,runtime,relative_uris):
    """Writes testsuite run results to xml file."""
    total,failed,conformance = calc_conformance(results)
    with open(path,'w') as xmlfile:
        testsuite_path, testsuite_index = testsuite['uri'].rsplit('/',1)
        testsuite_uri = testsuite['uri'] if not relative_uris else testsuite_index
    
        xmlfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xmlfile.write('<testsuite\n\txmlns="http://www.altova.com/testsuite/results"\n')
        if relative_uris:
            xmlfile.write('\txml:base="{}/"\n'.format(testsuite_path))      
        xmlfile.write('\turi="{}"\n\tname="{}"\n\ttotal="{}"\n\tfailed="{}"\n\tconformance="{}"\n\truntime="{}"\n\texecution-date="{:%Y-%m-%dT%H:%M:%S}"\n\tprocessor="Altova RaptorXML+XBRL Server">\n'.format(testsuite_uri,testsuite['name'],total,failed,conformance,runtime,datetime.datetime.now()))
        for testcase in testsuite['testcases']:
            testcase_uri = testcase['uri'] if not relative_uris else testcase['uri'][len(testsuite_path)+1:]
            xmlfile.write('\t<testcase\n\t\turi="{}"\n\t\tnumber="{}"\n\t\truleIdentifier="{}">\n'.format(testcase_uri,testcase['number'],testcase['ruleIdentifier']))
            for variation in testcase['variations']:
                variation_key = (testcase['uri'],variation['id'])
                if variation_key in results:
                    instance_uri = variation['data']['readMeFirst'] if not relative_uris else variation['data']['readMeFirst'][len(testsuite_path)+1:]
                    xmlfile.write('\t\t<variation\n\t\t\tid="{}"\n\t\t\tname="{}"\n\t\t\tinstance="{}">\n'.format(variation['id'],xml_escape(variation['name']),instance_uri))
                    status, error_counts = results[variation_key]
                    actual = ' '.join('%dx%s'%(count,code) for code, count in sorted(error_counts.items()))
                    expected = ' '.join('%dx%s'%(error['count'],code) for code, error in sorted(variation['results']['errors'].items()))
                    blocked = variation['results']['blockedMessageCodes'].replace('|',' ') if variation['results']['blockedMessageCodes'] else ''
                    if status == 'PASS' and len(variation['results']['errors']) != len(error_counts):
                        additional_errors = ' '.join(set(error_counts.keys()) - set(variation['results']['errors']))
                        xmlfile.write('\t\t\t<result\n\t\t\t\tstatus="{}"\n\t\t\t\tactual="{}"\n\t\t\t\texpected="{}"\n\t\t\t\tblocked="{}"\n\t\t\t\tadditional="{}"/>\n'.format(status,actual,expected,blocked,additional_errors))
                    else:
                        xmlfile.write('\t\t\t<result\n\t\t\t\tstatus="{}"\n\t\t\t\tactual="{}"\n\t\t\t\texpected="{}"\n\t\t\t\tblocked="{}"/>\n'.format(status,actual,expected,blocked))
                    xmlfile.write('\t\t</variation>\n')
            xmlfile.write('\t</testcase>\n')
        xmlfile.write('</testsuite>\n')

def print_results(testsuite,results,runtime):
    """Writes testsuite run summary to console."""    
    total,failed,conformance = calc_conformance(results)
    for testcase in testsuite['testcases']:
        for variation in testcase['variations']:
            variation_key = (testcase['uri'],variation['id'])
            if variation_key in results:
                status, error_counts = results[variation_key]
                if status != 'PASS':
                    actual = ' '.join('%dx%s'%(count,code) for code, count in sorted(error_counts.items()))
                    expected = ' '.join('%dx%s'%(error['count'],code) for code, error in sorted(variation['results']['errors'].items()))
                    blocked = variation['results']['blockedMessageCodes'].replace('|',' ') if variation['results']['blockedMessageCodes'] else ''
                    print('ERROR: Testcase %s, variation %s FAILED; actual [%s]; expected [%s]; blocked [%s]' % (testcase['number'], variation['id'], actual, expected, blocked))
                elif len(variation['results']['errors']) != len(error_counts):
                    additional_errors = set(error_counts.keys()) - set(variation['results']['errors'])
                    print('Warning: Testcase %s, variation %s had additional errors: [%s]' % (testcase['number'], variation['id'], ' '.join(sorted(additional_errors))))
    print('Conformance: %.2f%% (%d failed testcase variations out of %d)' % (conformance,failed,total))

def run_xbrl_testsuite(uri,args):
    """Load and execute the conformance testsuite."""
    try:
        testsuite = load_testsuite(uri)
        results, runtime = execute_testsuite(testsuite,args)
        logging.info('Start generating testsuite report')
        if args.csv_file:
            write_csv_report(args.csv_file,testsuite,results,runtime,args.relative_uris)
        if args.xml_file:
            write_xml_report(args.xml_file,testsuite,results,runtime,args.relative_uris)
        if not args.csv_file and not args.xml_file:
            print_results(testsuite,results,runtime)
        logging.info('Finished generating testsuite report')
    except:
        logging.exception('Testsuite run aborted with exception:')

def setup_logging(args):
    """Initializes Python logging module."""
    if args.log_file:
        levels = {'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=args.log_file,filemode='w',level=levels[args.log_level])
    else:
        logging.getLogger().addHandler(logging.NullHandler())
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
    logging.getLogger().addHandler(console)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Execute the XBRL US DQC conformance testsuite using Altova RaptorXML+XBRL')
    parser.add_argument('uri', metavar='INDEX', help='main testsuite index file')
    parser.add_argument('-l','--log', metavar='LOG_FILE', dest='log_file', help='log output file')
    parser.add_argument('--log-level', metavar='LOG_LEVEL', dest='log_level', choices=['ERROR','WARNING','INFO','DEBUG'], default='INFO', help='log level (ERROR|WARNING|INFO|DEBUG)')
    parser.add_argument('--csv-report', metavar='CSV_FILE', dest='csv_file', help='write testsuite results to csv')
    parser.add_argument('--xml-report', metavar='XML_FILE', dest='xml_file', help='write testsuite results to xml')
    parser.add_argument('--relative-uris', dest='relative_uris', action='store_true', help='write testcase uris relative to testsuite index file')
    parser.add_argument('-t','--testcase', metavar='TESTCASE_NUMBER', dest='testcase_numbers', nargs='*', help='limit execution to only this testcase number')
    parser.add_argument('-v','--variation', metavar='VARIATION_ID', dest='variation_ids', nargs='*', help='limit execution to only this variation id')
    parser.add_argument('-w','--workers', metavar='MAX_WORKERS', type=int, dest='max_workers', default=multiprocessing.cpu_count(), help='limit number of workers')
    return parser.parse_args()
    
def main():
    # Parse command line arguments
    args = parse_args()
    
    # Setup logging
    setup_logging(args)    
    
    # Run the testsuite
    run_xbrl_testsuite(args.uri,args)

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    logging.info('Finished testsuite run in %fs',end-start)
