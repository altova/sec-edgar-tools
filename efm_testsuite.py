# Copyright 2015, 2016 Altova GmbH
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
__copyright__ = 'Copyright 2015, 2016 Altova GmbH'
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# Executes the SEC EDGAR public test suite (http://www.sec.gov/info/edgar/ednews/efmtest/efm-37-160614.zip).
# See http://www.sec.gov/spotlight/xbrl/interactive_data_test_suite.shtml for more information.
#
# Example usage:
#   raptorxmlxbrl script efm_testsuite.py /path/to/efm-37-160614/conf/testcases.xml --log=log.txt --csv-report=report.csv
#
# Show available options
#   raptorxmlxbrl script efm_testsuite.py -h
# Create a CSV summary file
#   raptorxmlxbrl script efm_testsuite.py /path/to/efm-37-160614/conf/testcases.xml --log efm_testsuite.log --csv-report efm_testsuite.csv
# Create an XML summary file
#   raptorxmlxbrl script efm_testsuite.py /path/to/efm-37-1606143/conf/testcases.xml --log efm_testsuite.log --xml-report efm_testsuite.xml
# Run only specific testcases
#   raptorxmlxbrl script efm_testsuite.py /path/to/efm-37-160614/conf/testcases.xml --log efm_testsuite.log --csv-report efm_testsuite.xml --testcase "605-01" "605-02"

import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl
import efm_validation

import argparse,collections,concurrent.futures,datetime,logging,multiprocessing,os,re,time,urllib.parse

re_error_code = re.compile(r'\[EFM\.(\d+\.\d+(\.\d+))?\] ')

xhtml_inlinexbrl_xsd = xsd.Schema.create_from_url('http://www.xbrl.org/2013/inlineXBRL/xhtml-inlinexbrl-1_1.xsd')[0]

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

def exhibit_type(elem):
    """Returns the exhibit type of the file."""
    val = attr_val(elem,'exhibitType')
    if val is None:
        return 'EX-101'
    return val

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
        'name': '',
        'description': '',
        'references': [],
    }

    for elem in variation_elem.element_children():
        if elem.local_name == 'name':
            variation['name'] = elem_val(elem)
        elif elem.local_name == 'description':
            variation['description'] = elem.serialize(omit_start_tag=True)
        elif elem.local_name == 'reference':
            variation['references'].append(attr_val(elem,'specification'))
        elif elem.local_name == 'data':
            data = {
                'instances': [],
                'linkbases': [],
                'schemas': [],
                'images': [],
                'parameters': [],
            }
            for elem2 in elem.element_children():
                if elem2.local_name in ('instance','linkbase','schema','image'):
                    uri = elem_val(elem2)
                    data[elem2.local_name+'s'].append({
                        'uri': urllib.parse.urljoin(elem2.base_uri,uri),
                        'exhibitType': exhibit_type(elem2),
                        'readMeFirst': attr_val_bool(elem2,'readMeFirst'),
                    })
                    if attr_val_bool(elem2,'readMeFirst'):
                        data['readMeFirst'] = urllib.parse.urljoin(elem2.base_uri,uri)
                elif elem2.local_name == 'parameter':
                    data['parameters'].append({
                        'name': attr_val(elem2,'name'),
                        'datatype': attr_val(elem2,'datatype'),
                        'value': attr_val(elem2,'value'),
                    })
                else:
                    logging.warning('Testcase file %s contains unknown <data> child element <%s>',elem2.document.uri,elem2.local_name)
            variation['data'] = data
        elif elem.local_name == 'result':
            result = {
                'expected': attr_val(elem,'expected'),
                'asserts': []
            }
            for elem2 in elem.element_children():
                if elem2.local_name == 'assert':
                    result['asserts'].append({
                        'name': attr_val(elem2,'name'),
                        'num': attr_val(elem2,'num'),
                        'severity': attr_val(elem2,'severity'),
                        'countSatisfied': attr_val(elem2,'countSatisfied'),
                        'countNotSatisfied': attr_val(elem2,'countNotSatisfied'),
                        'frd': attr_val(elem2,'frd'),
                    })
                elif elem2.local_name == 'instance':
                    # store uri of reference result instance for comparison
                    uri = elem_val(elem2)
                    result['instance'] = urllib.parse.urljoin(elem2.base_uri,uri)
                else:
                    logging.warning('Testcase file %s contains unknown <result> child element <%s>',elem2.document.uri,elem2.local_name)
            variation['result'] = result
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
        'references': [],
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
        elif elem.local_name == 'name':
            testcase['name'] = elem_val(elem)
        elif elem.local_name == 'description':
            testcase['description'] = elem.serialize(omit_start_tag=True)
        elif elem.local_name == 'reference':
            testcase['references'].append(attr_val(elem,'specification'))
        elif elem.local_name == 'variation':
            variation = parse_variation(elem)
            variations.append(variation)
            if variation['id'] in variation_ids:
                logging.warning('Testcase file %s contains variations with duplicate id %s',testcase_uri,variation['id'])
            variation_ids.add(variation['id'])
        else:
            logging.warning('Testcase file %s contains unknown <testcase> child element <%s>',elem.document.uri,elem.local_name)

    testcase['variations'] = variations

    return testcase

def load_testsuite(index_uri):
    """Loads the testcases specified in the given testsuite index file and returns a dict with all testcase meta-information."""
    logging.info('Start loading testsuite index %s',index_uri)
    start = time.time()

    # Load the testcase index file
    instance, log = xml.Instance.create_from_url(index_uri)
    # Check for any fatal errors
    if not instance:
        raise ValidationError('\n'.join(error.text for error in log))
    testcases_elem = instance.document_element

    testsuite = {
        'uri': instance.uri,
        'name': attr_val(testcases_elem,'name'),
        'date': attr_val(testcases_elem,'date')
    }

    # Iterate over all <testcase> child elements and parse the testcase file
    testcases = []
    for testcase_elem in testcases_elem.element_children():
        if testcase_elem.local_name == 'testcase':
            # Get the value of the @uri attribute and make any relative uris absolute to the base uri
            uri = urllib.parse.urljoin(testcase_elem.base_uri,attr_val(testcase_elem,'uri'))
            # Load the testcase file
            testcases.append(load_testcase(uri))
    testsuite['testcases'] = testcases

    runtime = time.time() - start
    logging.info('Finished loading testsuite index %s in %fs',index_uri,runtime)
    return testsuite


def hash_element_content(elem,refs):
    if len(list(elem.element_children())) == 0:
        text = elem.text_content()
        if text:
            if '/>' in text or '</' in text:
                instance, log = xml.Instance.create_from_buffer(('<root>%s</root>'%text).encode('utf-8'))
                if not log.has_errors():
                    s = set()
                    for child in instance.document_element.element_children():
                        s.add(hash_element(child))
                    return frozenset(s)
            elif elem.schema_actual_value is not None and not isinstance(elem.schema_actual_value, xsd.string):
                return elem.schema_actual_value

            text = text.strip()
            try:
                text = float(text)
            except:
                pass
        return text

    s = set()
    for child in elem.element_children():
        s.add(hash_element(child,refs))
    return frozenset(s)


def hash_element(elem,refs=None):
    d = {
        'name': elem.qname,
        'value': hash_element_content(elem,refs)
    }
    exclude_attrs = [xml.QName('label',efm_validation.xlink_namespace), xml.QName('from',efm_validation.xlink_namespace), xml.QName('to',efm_validation.xlink_namespace), xml.QName('order')]
    if elem.qname == xml.QName('schemaRef', efm_validation.link_namespace):
        exclude_attrs.append(xml.QName('arcrole', efm_validation.xlink_namespace))
    if elem.qname != xml.QName('footnote', efm_validation.link_namespace):
        exclude_attrs.append(xml.QName('lang',efm_validation.xml_namespace))
    for attr in elem.attributes:
        if attr.qname not in exclude_attrs:
            value = attr.schema_actual_value if attr.schema_actual_value is not None else attr.normalized_value
            d[attr.qname] = value
            if refs is not None:
                if attr.qname == xml.QName('contextRef'):
                    refs['contexts'].add(value)
                elif attr.qname == xml.QName('unitRef'):
                    refs['units'].add(value)
                elif attr.qname == xml.QName('role', efm_validation.xlink_namespace):
                    refs['roleRefs'].add(value)
                elif attr.qname == xml.QName('arcrole', efm_validation.xlink_namespace):
                    refs['arcroleRefs'].add(value)
    return frozenset(d.items())

def hash_footnoteLink(link,refs):
    arcs = []
    labels = {}
    for child in link.element_children():
        if child.qname == xml.QName('loc',efm_validation.link_namespace):
            labels.setdefault(child.find_attribute(xml.QName('label',efm_validation.xlink_namespace)).normalized_value,[]).append(child)
        elif child.qname == xml.QName('footnote',efm_validation.link_namespace):
            labels.setdefault(child.find_attribute(xml.QName('label',efm_validation.xlink_namespace)).normalized_value,[]).append(child)
            refs['roleRefs'].add(child.find_attribute(xml.QName('role',efm_validation.xlink_namespace)).normalized_value)
        elif child.qname == xml.QName('footnoteArc',efm_validation.link_namespace):
            arcs.append(child)
            refs['arcroleRefs'].add(child.find_attribute(xml.QName('arcrole',efm_validation.xlink_namespace)).normalized_value)
        else:
            raise Exception('Unexpected element '+str(child.qname))

    s = set()
    for arc in arcs:
        for _from in labels[arc.find_attribute(xml.QName('from',efm_validation.xlink_namespace)).normalized_value]:
            for _to in labels[arc.find_attribute(xml.QName('to',efm_validation.xlink_namespace)).normalized_value]:
                s.add(frozenset({
                    'arc': hash_element(arc),
                    'from': hash_element(_from),
                    'to': hash_element(_to),
                }.items()))
    return frozenset(s)

def hash_instance(elem):
    refs = {'contexts': set(), 'units': set(), 'roleRefs': set(), 'arcroleRefs': set(), 'footnoteLinks': set()}
    refElems = {'contexts': {}, 'units': {}, 'roleRefs': {}, 'arcroleRefs': {}, 'footnoteLinks': {}}
    s = set()
    for child in elem.element_children():
        if child.qname == xml.QName('schemaRef',efm_validation.link_namespace):
            s.add(hash_element(child))
        elif child.qname == xml.QName('linkbaseRef',efm_validation.link_namespace):
            s.add(hash_element(child))
        elif child.qname == xml.QName('roleRef',efm_validation.link_namespace):
            refElems['roleRefs'][child.find_attribute('roleURI').normalized_value] = hash_element(child)
        elif child.qname == xml.QName('arcroleRef',efm_validation.link_namespace):
            refElems['arcroleRefs'][child.find_attribute('arcroleURI').normalized_value] = hash_element(child)
        elif child.qname == xml.QName('context',efm_validation.xbrli_namespace):
            refElems['contexts'][child.find_attribute('id').normalized_value] = hash_element(child)
        elif child.qname == xml.QName('unit',efm_validation.xbrli_namespace):
            refElems['units'][child.find_attribute('id').normalized_value] = hash_element(child)
        elif child.qname == xml.QName('footnoteLink',efm_validation.link_namespace):
            role = child.find_attribute(xml.QName('role',efm_validation.xlink_namespace)).normalized_value
            refs['roleRefs'].add(role)
            refs['footnoteLinks'].add(role)
            refElems['footnoteLinks'][role] = refElems['footnoteLinks'].get(role,frozenset()) | hash_footnoteLink(child,refs)
        else:
            s.add(hash_element(child,refs))
    for key in refs.keys():
        for ref in refs[key]:
            if ref in refElems[key]:
                s.add(refElems[key][ref])
    return frozenset(s)

def cmp_output(l, r):
    hash_left = hash_instance(l)
    hash_right = hash_instance(r)
    return hash_left == hash_right

def execute_variation(testcase,variation):
    """Peforms the actual XBRL instance or taxonomy validation and returns 'PASS' if the actual outcome is conformant with the result specified in the variation."""
    logging.info('[%s%s] Start executing variation',testcase['number'],variation['id'])

    if len(variation['data']['instances']) > 1:
        logging.info('[%s%s] Skipped multiple instance variation',testcase['number'],variation['id'])
        return 'SKIP',collections.Counter()
    if any(_assert['num'] in ('60302','60310') for _assert in variation['result']['asserts']):
        logging.info('[%s%s] Skipped variation containing submission check',testcase['number'],variation['id'])
        return 'SKIP',collections.Counter()

    if 'readMeFirst' not in variation['data']:
        raise RuntimeError('Unknown entry point in variation %s%s' % (testcase['number'],variation['id']))

    uri = variation['data']['readMeFirst']
    logging.info('[%s%s] Validating instance %s',testcase['number'],variation['id'],uri)

    # Determine if UTR checks should be enabled
    bEnableUTR = False
    forceUtrValidationParam = [param for param in variation['data']['parameters'] if param['name'] == 'forceUtrValidation']
    if forceUtrValidationParam:
        bEnableUTR = forceUtrValidationParam[0]['value'] == 'true'
    else:
        dts = xbrl.taxonomy.DTS.create_from_url(schema['uri'] for schema in variation['data']['schemas'])[0]
        if dts and dts.resolve_concept(xml.QName('UTR','http://xbrl.sec.gov/dei/2014-01-31')):
            bEnableUTR = True

    bNotEDGARDependent = 'Not EDGAR Dependent' in testcase['name']
    has_ixbrl_warnings = False
    has_ixbrl_errors = False
    instance = None
    if os.path.splitext(uri)[1] == '.htm':
        # Do Inline XBRL transformation
        if bNotEDGARDependent:
            instances, error_log = xbrl.InlineXBRLDocumentSet.transform_xml_from_url(uri,utr=True)
        else:
            instance, error_log = xml.Instance.create_from_url(uri,schema=xhtml_inlinexbrl_xsd)
            efm_validation.validate_ixbrl(instance,error_log)
            if error_log.has_warnings():
                has_ixbrl_warnings = True
            if not error_log.has_errors():
                instances, error_log = xbrl.InlineXBRLDocumentSet.transform_xbrl_from_url(uri,utr=bEnableUTR)
                instance = instances.get(None)
        if error_log.has_errors():
            has_ixbrl_errors = True
    else:
        instance, error_log = xbrl.Instance.create_from_url(uri,utr=bEnableUTR)

    if not bNotEDGARDependent and not has_ixbrl_errors:
        efm_validation.validate(uri,instance,error_log,{param['name']:param['value'] for param in variation['data']['parameters']})

    if error_log.has_errors() and logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug('[%s%s] Error log:\n%s',testcase['number'],variation['id'],'\n'.join(error.text for error in error_log))

    error_counts = {
        'err': collections.Counter(),
        'wrn': collections.Counter(),
    }

    if has_ixbrl_errors:
        error_counts['err']['99999'] += 1
    elif has_ixbrl_warnings:
        error_counts['wrn']['99999'] += 1
    else:
        for error in error_log:
            if error.severity == xml.ErrorSeverity.ERROR:
                severity = 'err'
            elif error.severity == xml.ErrorSeverity.WARNING:
                severity = 'wrn'
            else:
                continue
            m = re_error_code.search(error.text)
            if m:
                code = '{}{:02d}{:02d}'.format(*[int(x) for x in m.group(1).split('.')])
                error_counts[severity][code] += 1
            else:
                error_counts[severity]['other'] += 1

    passed = False if not any(_assert['severity'] == 'err' for _assert in variation['result']['asserts']) and error_log.has_errors() else True
    for _assert in variation['result']['asserts']:
        if error_counts[_assert['severity']][_assert['num']] == 0:
            passed = False

    conformance = 'PASS' if passed else 'FAIL'

    if passed and instance is not None and 'instance' in variation['result'].keys():
        ref_instance, ref_error_log = xbrl.Instance.create_from_url(variation['result']['instance'])
        if ref_instance is not None:
            if not cmp_output(ref_instance.document_element, instance.document_element):
                conformance = 'OUTPUT MISMATCH'

    error_counts = error_counts['err'] + error_counts['wrn']
    logging.info('[%s%s] Finished executing variation: %s, %s',testcase['number'],variation['id'],conformance,dict(error_counts))
    return conformance, error_counts

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
                logging.exception('[%s%s] Exception raised during testcase execution:',variation_key[0],variation_key[1])

    runtime = time.time() - start
    logging.info('Finished executing testcase variations in %fs',runtime)
    return results,runtime

def calc_conformance(results):
    """Returns a tuple with the number of total and failed testcase variations and the conformance as percentage."""
    total = len(results)
    passed = failed = skipped = 0
    for status,_ in results.values():
        if status == 'PASS':
            passed +=1
        elif status == 'SKIP':
            skipped +=1
        else:
            failed += 1
    conformance = (total-failed)*100/total
    return total,failed,skipped,conformance

def write_csv_report(path,testsuite,results,runtime,relative_uris):
    """Writes testsuite run results to csv file."""
    total,failed,skipped,conformance = calc_conformance(results)
    with open(path,'w') as csvfile:
        testsuite_path, testsuite_index = os.path.split(testsuite['uri'])

        csvfile.write('Date,Total,Failed,Skipped,Conformance,Runtime,Testsuite,Testcase,Variation,ReadMeFirst,Status,Actual,Expected,Warnings\n')
        csvfile.write('"{:%Y-%m-%d %H:%M:%S}",{},{},{},{:.2f},{:.1f},{}\n'.format(datetime.datetime.now(),total,failed,skipped,conformance,runtime,testsuite['uri']))
        for testcase in testsuite['testcases']:
            csvfile.write(',,,,,,,%s\n'%testcase['number'])
            for variation in testcase['variations']:
                variation_key = (testcase['uri'],variation['id'])
                if variation_key in results:
                    instance_uri = variation['data']['readMeFirst'] if not relative_uris else variation['data']['readMeFirst'][len(testsuite_path)+1:]
                    status, error_counts = results[variation_key]
                    actual = ' '.join(code for code in sorted(error_counts))
                    expected = ' '.join(_assert['num'] for _assert in sorted(variation['result']['asserts'],key=lambda x:x['num']))
                    warnings = ''
                    if status == 'PASS' and len(variation['result']['asserts']) != len(error_counts):
                        additional_errors = set(error_counts.keys()) - set(_assert['num'] for _assert in variation['result']['asserts'])
                        warnings = 'Additional errors or warnings reported: %s' % ' '.join(sorted(additional_errors))
                    csvfile.write(',,,,,,,,{},{},{},{},{},{}\n'.format(variation['id'],instance_uri,status,actual,expected,warnings))

def xml_escape(str):
    return str.replace('<','&lt;').replace('&','&amp;').replace('"','&quot;')

def write_xml_report(path,testsuite,results,runtime,relative_uris):
    """Writes testsuite run results to xml file."""
    total,failed,skipped,conformance = calc_conformance(results)
    with open(path,'w') as xmlfile:
        testsuite_path, testsuite_index = os.path.split(testsuite['uri'])
        testsuite_uri = testsuite['uri'] if not relative_uris else testsuite_index

        xmlfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        xmlfile.write('<testsuite\n\txmlns="http://www.altova.com/testsuite/results"\n')
        if relative_uris:
            xmlfile.write('\txml:base="{}/"\n'.format(testsuite_path))
        xmlfile.write('\turi="{}"\n\tname="{}"\n\ttotal="{}"\n\tfailed="{}"\n\tskipped="{}"\n\tconformance="{}"\n\truntime="{}"\n\texecution-date="{:%Y-%m-%dT%H:%M:%S}"\n\tprocessor="Altova RaptorXML+XBRL Server">\n'.format(testsuite_uri,testsuite['name'],total,failed,skipped,conformance,runtime,datetime.datetime.now()))
        for testcase in testsuite['testcases']:
            testcase_uri = testcase['uri'] if not relative_uris else testcase['uri'][len(testsuite_path)+1:]
            xmlfile.write('\t<testcase\n\t\turi="{}"\n\t\tname="{}"\n\t\tnumber="{}">\n'.format(testcase_uri,xml_escape(testcase['name']),testcase['number']))
            for variation in testcase['variations']:
                variation_key = (testcase['uri'],variation['id'])
                if variation_key in results:
                    instance_uri = variation['data']['readMeFirst'] if not relative_uris else variation['data']['readMeFirst'][len(testsuite_path)+1:]
                    xmlfile.write('\t\t<variation\n\t\t\tid="{}"\n\t\t\tname="{}"\n\t\t\tinstance="{}">\n'.format(variation['id'],xml_escape(variation['name']),instance_uri))
                    status, error_counts = results[variation_key]
                    actual = ' '.join(code for code in sorted(error_counts))
                    expected = ' '.join(_assert['num'] for _assert in sorted(variation['result']['asserts'],key=lambda x:x['num']))
                    if status == 'PASS' and len(variation['result']['asserts']) != len(error_counts):
                        additional_errors = ' '.join(set(error_counts.keys()) - set(_assert['num'] for _assert in variation['result']['asserts']))
                        xmlfile.write('\t\t\t<result\n\t\t\t\tstatus="{}"\n\t\t\t\tactual="{}"\n\t\t\t\texpected="{}"\n\t\t\t\tadditional="{}"/>\n'.format(status,actual,expected,additional_errors))
                    else:
                        xmlfile.write('\t\t\t<result\n\t\t\t\tstatus="{}"\n\t\t\t\tactual="{}"\n\t\t\t\texpected="{}"/>\n'.format(status,actual,expected))
                    xmlfile.write('\t\t</variation>\n')
            xmlfile.write('\t</testcase>\n')
        xmlfile.write('</testsuite>\n')

def print_results(testsuite,results,runtime):
    """Writes testsuite run summary to console."""
    total,failed,skipped,conformance = calc_conformance(results)
    for testcase in testsuite['testcases']:
        for variation in testcase['variations']:
            variation_key = (testcase['uri'],variation['id'])
            if variation_key in results:
                status, error_counts = results[variation_key]
                if status != 'PASS':
                    actual = ' '.join(code for code in sorted(error_counts))
                    expected = ' '.join(_assert['num'] for _assert in sorted(variation['result']['asserts'],key=lambda x:x['num']))
                    print('ERROR: Testcase variation %s%s FAILED; actual [%s] != expected [%s]' % (testcase['number'], variation['id'], actual, expected))
                elif len(variation['result']['asserts']) != len(error_counts):
                    additional_errors = set(error_counts.keys()) - set(_assert['num'] for _assert in variation['result']['asserts'])
                    print('Warning: Testcase variation %s%s had additional errors or warnings: [%s]' % (testcase['number'], variation['id'], ' '.join(sorted(additional_errors))))
    print('Conformance: %.2f%% (%d total; %d failed; %d skipped)' % (conformance,total,failed,skipped))

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
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',filename=args.log_file,filemode='w',level=logging.DEBUG if args.log_level == 'DEBUG' else logging.INFO)
    else:
        logging.getLogger().addHandler(logging.NullHandler())
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    console.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
    logging.getLogger().addHandler(console)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Execute the SEC EDGAR public test suite using Altova RaptorXML+XBRL')
    parser.add_argument('uri', metavar='INDEX', help='main testsuite index file')
    parser.add_argument('-l','--log', metavar='LOG_FILE', dest='log_file', help='log output file')
    parser.add_argument('--log-level', metavar='LOG_LEVEL', dest='log_level', choices=['INFO','DEBUG'], default='INFO', help='log level (INFO|DEBUG)')
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