# Copyright 2015-2018 Altova GmbH
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
__copyright__ = "Copyright 2015-2018 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'
__version__ = '47'

# This script implements additional validation rules specified in the EDGAR Filer Manual (Volume II) EDGAR Filing (Version 47) (http://www.sec.gov/info/edgar/edmanuals.htm)
#
# The following script parameters can be additionally specified:
#
#   CIK                         The CIK of the registrant
#   submissionType              The EDGAR submission type, e.g. 10-K
#   cikList                     A list of CIKs separated with a comma ','
#   cikNameList                 A list of official registrant names for each CIK in cikList separated by '|Edgar|'
#   forceUtrValidation          Set to true to force-enable UTR validation
#   enableDqcValidation         Set to true to enable additional XBRL US Data Quality Committee checks (https://xbrl.us/home/data-quality/rules-guidance/)
#   edbody-url                  The path to the edbody.dtd used to validate the embedded HTML fragments
#   edgar-taxonomies-url        The path to the edgartaxonomies.xml which contains a list of taxonomy files that are allowed to be referenced from the company extension taxonomy
#
# Example invocations:
#
# Validate a single filing
#   raptorxmlxbrl valxbrl --script=efm_validation.py instance.xml
# Validate a single filing with additional options
#   raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=CIK:1234567890 instance.xml
# Validate a single filing using EFM and DQC rules
#   raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=enableDqcValidation:true instance.xml


import altova_api.v2 as altova
import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl

import collections
import os
import sys
import re
import bisect
import datetime
import decimal
import imghdr
from urllib.request import pathname2url
from urllib.parse import urljoin
from urllib.parse import urlparse

sys.path.append(os.path.dirname(__file__))
import dqc_validation

supported_document_types = {
    # EDGAR Form Types (Corporate Finance):
    '10', '10-K', '10-Q', '20-F', '40-F', '6-K', '8-K', 'F-1', 'F-10', 'F-3', 'F-4', 'F-9', 'S-1', 'S-11', 'S-3', 'S-4', 'POS AM', '10-KT', '10-QT', 'POS EX', '10/A', '10-K/A', '10-Q/A', '20-F/A', '40-F/A', '6-K/A', '8-K/A', 'F-1/A', 'F-10/A', 'F-3/A', 'F-4/A', 'F-9/A', 'S-1/A', 'S-11/A', 'S-3/A', 'S-4/A', '10-KT/A', '10-QT/A',
    # EDGAR Form Types (Investment Management):
    '485BPOS', '497', 'N-CSR', 'N-CSRS', 'N-Q', 'N-CSR/A', 'N-CSRS/A', 'N-Q/A',
    # EDGAR Exhibit Types (Trading & Markets):
    'K SDR', 'L SDR',
    # mentioned additionally by the testsuite
    'Other', '10-12B', '10-12B/A', '10-12G', '10-12G/A', '20FR12B', '20FR12B/A', '20FR12G', '20FR12G/A', '40FR12B', '40FR12B/A', '40FR12G', '40FR12G/A', '8-K12B', '8-K12B/A', '8-K12G3', '8-K12G3/A', '8-K15D5', '8-K15D5/A', 'F-10EF', 'F-10POS', 'F-1MEF', 'F-3ASR', 'F-3D', 'F-3DPOS', 'F-3MEF', 'F-4 POS', 'F-4EF', 'F-4MEF', 'F-6', 'F-9 POS', 'F-9EF', 'N-1A', 'N-1A/A', 'POS462B', 'POS462C', 'POSASR', 'S-11MEF', 'S-1MEF', 'S-20', 'S-3ASR', 'S-3D', 'S-3DPOS', 'S-3MEF', 'S-4 POS', 'S-4EF', 'S-4MEF', 'S-B', 'S-BMEF', 'SP 15D2', 'SP 15D2/A'
}

submission_types = {
    "10-12B": {"10-12B", "Other"},
    "10-12B/A": {"10-12B/A", "Other"},
    "10-12G": {"10-12G", "Other"},
    "10-12G/A": {"10-12G/A", "Other"},
    "10-K": {"10-K"},
    "10-K/A": {"10-K", "10-K/A"},
    "10-KT": {"10-KT", "10-K", "Other"},
    "10-KT/A": {"10-KT/A", "10-K", "Other", "10-KT"},
    "10-Q": {"10-Q"},
    "10-Q/A": {"10-Q/A", "10-Q"},
    "10-QT": {"10-QT", "10-Q", "Other"},
    "10-QT/A": {"10-QT/A", "10-Q", "Other", "10-QT"},
    "20-F": {"20-F"},
    "20-F/A": {"20-F/A", "20-F"},
    "20FR12B": {"20FR12B", "Other"},
    "20FR12B/A": {"20FR12B/A", "Other"},
    "20FR12G": {"20FR12G", "Other"},
    "20FR12G/A": {"20FR12G/A", "Other"},
    "40-F": {"40-F"},
    "40-F/A": {"40-F/A", "40-F"},
    "40FR12B": {"40FR12B", "Other"},
    "40FR12B/A": {"40FR12B/A", "Other"},
    "40FR12G": {"40FR12G", "Other"},
    "40FR12G/A": {"40FR12G/A", "Other"},
    "485BPOS": {"485BPOS"},
    "497": {"497", "Other"},
    "6-K": {"6-K"},
    "6-K/A": {"6-K/A", "6-K"},
    "8-K": {"8-K"},
    "8-K/A": {"8-K/A", "8-K"},
    "8-K12B": {"8-K12B", "Other"},
    "8-K12B/A": {"8-K12B/A", "Other"},
    "8-K12G3": {"8-K12G3", "Other"},
    "8-K12G3/A": {"8-K12G3/A", "Other"},
    "8-K15D5": {"8-K15D5", "Other"},
    "8-K15D5/A": {"8-K15D5/A", "Other"},
    "F-1": {"F-1"},
    "F-1/A": {"F-1/A", "F-1"},
    "F-10": {"F-10"},
    "F-10/A": {"F-10/A", "F-10"},
    "F-10EF": {"F-10EF", "Other"},
    "F-10POS": {"F-10POS", "Other"},
    "F-1MEF": {"F-1MEF"},
    "F-3": {"F-3"},
    "F-3/A": {"F-3/A", "F-3"},
    "F-3ASR": {"F-3ASR", "F-3"},
    "F-3D": {"F-3D", "F-3"},
    "F-3DPOS": {"F-3DPOS", "F-3"},
    "F-3MEF": {"F-3MEF"},
    "F-4": {"F-4"},
    "F-4 POS": {"F-4 POS", "F-4"},
    "F-4/A": {"F-4/A", "F-4"},
    "F-4EF": {"F-4EF", "F-4"},
    "F-4MEF": {"F-4MEF"},
    "F-9": {"F-9"},
    "F-9 POS": {"F-9 POS", "F-9"},
    "F-9/A": {"F-9/A", "F-9"},
    "F-9EF": {"F-9EF", "F-9"},
    "N-1A": {"N-1A"},
    "N-1A/A": {"N-1A/A", "Other"},
    "N-CSR": {"N-CSR"},
    "N-CSR/A": {"N-CSR/A"},
    "N-CSRS": {"N-CSRS"},
    "N-CSRS/A": {"N-CSRS/A"},
    "N-Q": {"N-Q"},
    "N-Q/A": {"N-Q/A"},
    "ARS": {"Other"},
    "POS AM": {"F-1", "F-3", "F-4", "F-6", "S-1", "S-3", "S-4", "S-11", "S-20", "S-B", "POS AM", "Other"},
    "POS EX": {"F-3", "F-4", "S-1", "S-3", "S-4", "POS EX", "Other"},
    "POS462B": {"F-1MEF", "F-3MEF", "F-4MEF", "S-1MEF", "S-3MEF", "S-11MEF", "S-BMEF", "POS462B", "POS462C", "Other"},
    "POSASR": {"F-3", "S-3", "POSASR", "Other"},
    "S-1": {"S-1"},
    "S-1/A": {"S-1/A", "S-1"},
    "S-1MEF": {"S-1MEF"},
    "S-11": {"S-11"},
    "S-11/A": {"S-11/A"},
    "S-11MEF": {"S-11MEF"},
    "S-3": {"S-3"},
    "S-3/A": {"S-3/A", "S-3"},
    "S-3ASR": {"S-3ASR", "S-3"},
    "S-3D": {"S-3D", "S-3"},
    "S-3DPOS": {"S-3DPOS", "S-3"},
    "S-3MEF": {"S-3MEF"},
    "S-4": {"S-4"},
    "S-4 POS": {"S-4 POS", "S-4"},
    "S-4/A": {"S-4/A", "S-4"},
    "S-4EF": {"S-4EF", "S-4"},
    "S-4MEF": {"S-4MEF"},
    "SP 15D2": {"SP 15D2"},
    "SP 15D2/A": {"SP 15D2/A"},
    "SDR": {"K SDR", "L SDR"}
}

xbrl21_roles = {
    'http://www.xbrl.org/2003/role/link',
    'http://www.xbrl.org/2003/role/label',
    'http://www.xbrl.org/2003/role/terseLabel',
    'http://www.xbrl.org/2003/role/verboseLabel',
    'http://www.xbrl.org/2003/role/positiveLabel',
    'http://www.xbrl.org/2003/role/positiveTerseLabel',
    'http://www.xbrl.org/2003/role/positiveVerboseLabel',
    'http://www.xbrl.org/2003/role/negativeLabel',
    'http://www.xbrl.org/2003/role/negativeTerseLabel',
    'http://www.xbrl.org/2003/role/negativeVerboseLabel',
    'http://www.xbrl.org/2003/role/zeroLabel',
    'http://www.xbrl.org/2003/role/zeroTerseLabel',
    'http://www.xbrl.org/2003/role/zeroVerboseLabel',
    'http://www.xbrl.org/2003/role/totalLabel',
    'http://www.xbrl.org/2003/role/periodStartLabel',
    'http://www.xbrl.org/2003/role/periodEndLabel',
    'http://www.xbrl.org/2003/role/documentation',
    'http://www.xbrl.org/2003/role/definitionGuidance',
    'http://www.xbrl.org/2003/role/disclosureGuidance',
    'http://www.xbrl.org/2003/role/presentationGuidance',
    'http://www.xbrl.org/2003/role/measurementGuidance',
    'http://www.xbrl.org/2003/role/commentaryGuidance',
    'http://www.xbrl.org/2003/role/exampleGuidance',
    'http://www.xbrl.org/2003/role/reference',
    'http://www.xbrl.org/2003/role/definitionRef',
    'http://www.xbrl.org/2003/role/disclosureRef',
    'http://www.xbrl.org/2003/role/mandatoryDisclosureRef',
    'http://www.xbrl.org/2003/role/recommendedDisclosureRef',
    'http://www.xbrl.org/2003/role/unspecifiedDisclosureRef',
    'http://www.xbrl.org/2003/role/presentationRef',
    'http://www.xbrl.org/2003/role/measurementRef',
    'http://www.xbrl.org/2003/role/commentaryRef',
    'http://www.xbrl.org/2003/role/exampleRef',
    'http://www.xbrl.org/2003/role/footnote',
    'http://www.xbrl.org/2003/role/calculationLinkbaseRef',
    'http://www.xbrl.org/2003/role/definitionLinkbaseRef',
    'http://www.xbrl.org/2003/role/labelLinkbaseRef',
    'http://www.xbrl.org/2003/role/presentationLinkbaseRef',
    'http://www.xbrl.org/2003/role/referenceLinkbaseRef'
}
xbrl21_arcroles = {
    'http://www.xbrl.org/2003/arcrole/general-special',
    'http://www.xbrl.org/2003/arcrole/essence-alias',
    'http://www.xbrl.org/2003/arcrole/similar-tuples',
    'http://www.xbrl.org/2003/arcrole/requires-element',
    'http://www.xbrl.org/2003/arcrole/summation-item',
    'http://www.xbrl.org/2003/arcrole/parent-child',
    'http://www.xbrl.org/2003/arcrole/concept-label',
    'http://www.xbrl.org/2003/arcrole/concept-reference',
    'http://www.xbrl.org/2003/arcrole/fact-footnote'
}
numeric_roles = {
    'http://www.xbrl.org/2003/role/positiveLabel',
    'http://www.xbrl.org/2003/role/positiveTerseLabel',
    'http://www.xbrl.org/2003/role/positiveVerboseLabel',
    'http://www.xbrl.org/2003/role/negativeLabel',
    'http://www.xbrl.org/2003/role/negativeTerseLabel',
    'http://www.xbrl.org/2003/role/negativeVerboseLabel',
    'http://www.xbrl.org/2003/role/zeroLabel',
    'http://www.xbrl.org/2003/role/zeroTerseLabel',
    'http://www.xbrl.org/2003/role/zeroVerboseLabel',
    'http://www.xbrl.org/2003/role/totalLabel',
    'http://www.xbrl.org/2009/role/negatedLabel',
    'http://www.xbrl.org/2009/role/negatedPeriodEndLabel',
    'http://www.xbrl.org/2009/role/negatedPeriodStartLabel',
    'http://www.xbrl.org/2009/role/negatedTotalLabel',
    'http://www.xbrl.org/2009/role/negatedNetLabel',
    'http://www.xbrl.org/2009/role/negatedTerseLabel',
    'http://xbrl.us/us-gaap/role/label/negated',
    'http://xbrl.us/us-gaap/role/label/negatedTotal',
    'http://xbrl.us/us-gaap/role/label/negatedPeriodStart',
    'http://xbrl.us/us-gaap/role/label/negatedPeriodEnd',
}

xml_namespace = 'http://www.w3.org/XML/1998/namespace'
xsi_namespace = 'http://www.w3.org/2001/XMLSchema-instance'
xs_namespace = 'http://www.w3.org/2001/XMLSchema'
link_namespace = 'http://www.xbrl.org/2003/linkbase'
xlink_namespace = 'http://www.w3.org/1999/xlink'
xl_namespace = 'http://www.xbrl.org/2003/XLink'
xbrli_namespace = 'http://www.xbrl.org/2003/instance'
xbrldt_namespace = 'http://xbrl.org/2005/xbrldt'
xbrldi_namespace = 'http://xbrl.org/2006/xbrldi'
xhtml_namespace = 'http://www.w3.org/1999/xhtml'
ix_namespace = 'http://www.xbrl.org/2013/inlineXBRL'
ixt_namespace = 'http://www.xbrl.org/inlineXBRL/transformation/2015-02-26'
ixtsec_namespace = 'http://www.sec.gov/inlineXBRL/transformation/2015-08-31'
ref2004_namespace = 'http://www.xbrl.org/2004/ref'
ref2006_namespace = 'http://www.xbrl.org/2006/ref'

qname_item = xml.QName('item', xbrli_namespace, 'xbrli')
qname_hypercubeItem = xml.QName('hypercubeItem', xbrldt_namespace, 'xbrldt')
qname_dimensionItem = xml.QName('dimensionItem', xbrldt_namespace, 'xbrldt')
qname_labelLink = xml.QName('labelLink', link_namespace, 'link')
qname_referenceLink = xml.QName('referenceLink', link_namespace, 'link')
qname_presentationLink = xml.QName('presentationLink', link_namespace, 'link')
qname_calculationLink = xml.QName('calculationLink', link_namespace, 'link')
qname_definitionLink = xml.QName('definitionLink', link_namespace, 'link')

qname_xs_anyURI = xml.QName('anyURI', xs_namespace, 'xs')
qname_xs_base64Binary = xml.QName('base64Binary', xs_namespace, 'xs')
qname_xs_hexBinary = xml.QName('hexBinary', xs_namespace, 'xs')
qname_xs_NOTATION = xml.QName('NOTATION', xs_namespace, 'xs')
qname_xs_QName = xml.QName('QName', xs_namespace, 'xs')
qname_xs_time = xml.QName('time', xs_namespace, 'xs')
qname_xs_token = xml.QName('token', xs_namespace, 'xs')
qname_xs_language = xml.QName('language', xs_namespace, 'xs')

midnight = datetime.time(0, 0, 0, 0)
hours24 = datetime.timedelta(hours=24)
re_company_uri = re.compile('http://([^/]+)/(([0-9]{4})([0-9]{2})([0-9]{2})|([0-9]{4})-([0-9]{2})-([0-9]{2}))')
re_authority = re.compile('http://([^/]+)/.*')
re_encoding = re.compile('encoding\\s*=\\s*(["\'])([A-Za-z0-9._-]*)\\1')
re_invalid_ascii = re.compile('[^0-9A-Za-z`~!@#$%&*().\\-+ {}[\\]|\\\\:;"\'<>,_?/=\t\n\r\f]')
re_xml_uri = re.compile('.*/[^-]+-[0-9]{8}.xml')
re_xsd_uri = re.compile('.*/[^-]+-[0-9]{8}.xsd')
re_lab_uri = re.compile('.*/[^-]+-[0-9]{8}_lab.xml')
re_ref_uri = re.compile('.*/[^-]+-[0-9]{8}_ref.xml')
re_pre_uri = re.compile('.*/[^-]+-[0-9]{8}_pre.xml')
re_cal_uri = re.compile('.*/[^-]+-[0-9]{8}_cal.xml')
re_def_uri = re.compile('.*/[^-]+-[0-9]{8}_def.xml')
re_consecutive_xml_whitespace = re.compile('[ \t\n\r]{2,}')
re_cik = re.compile('[0-9]{10}')
re_dei = re.compile('http://xbrl.us/dei/|http://xbrl.sec.gov/dei/')
re_gaap = re.compile('http://[^/]+/us-gaap/[0-9-]+')
re_ifrs = re.compile('http://xbrl.ifrs.org/taxonomy/[0-9-]+/ifrs-full')
re_rr = re.compile('http://xbrl.sec.gov/rr/[0-9-]+')
re_definition = re.compile('[0-9]+ - (Statement|Disclosure|Schedule|Document) -.*[^\s]')
re_html_stag = re.compile('<[:A-z_a-z][:A-z_a-z.0-9-]*(\s+.*)?/?>')
re_html_href = re.compile('((http://)?www.sec.gov/Archives/edgar/data/.+)|(#.+)|([^/.:]+)')
re_html_src = re.compile('([^/.:]+)\.(jpg|gif)')
re_period_start_or_end = re.compile('[pP]eriod(Start|End)')
re_display_none = re.compile('(.*;)?\s*display\s*:\s*none\s*(;.*)?')


def get_standard_namespace2uris(standard_taxonomies):
    standard_namespace2uris = collections.defaultdict(list)
    for entry in standard_taxonomies:
        if entry['AttType'] == 'SCH':
            standard_namespace2uris[entry['Namespace']].append(entry['Href'])
    return standard_namespace2uris

def get_standard_namespace2prefix(standard_taxonomies):
    standard_namespace2prefix = {}
    for entry in standard_taxonomies:
        if entry['Family'] != 'BASE' and entry['AttType'] == 'SCH' and 'Prefix' in entry:
            standard_namespace2prefix[entry['Namespace']] = entry['Prefix']
    return standard_namespace2prefix

def is_extension_document(instance_uri, doc):
    return instance_uri.rsplit('/', 1)[0] == doc.uri.rsplit('/', 1)[0]


def check_xml_base(elem, error_log):
    for attr in elem.attributes:
        if attr.local_name == 'base' and attr.namespace_name == xml_namespace:
            # 6.3.11 Attribute xml:base must not appear in any Interactive Data document.
            error_log.report(xbrl.Error.create('[EFM.6.3.11] Attribute {base} is not allowed.', base=attr))
    for child in elem.children:
        if isinstance(child, xml.ElementInformationItem):
            check_xml_base(child, error_log)


def check_valid_ascii(uri, catalog, error_log):
    # 5.2.1.1 Valid ASCII Characters
    with altova.open(uri, catalog=catalog, mode='r', encoding='ascii') as f:
        try:
            for line, s in enumerate(f):
                if line == 0 and s.startswith('<?xml'):
                    m = re_encoding.search(s)
                    if m and m.group(2).lower() not in ('ascii', 'us-ascii', 'iso-8859-1', 'utf-8'):
                        # For other encodings, do a quick check if it has the same byte representation for us-ascii characters
                        try:
                            if 'test'.encode('us-ascii') != 'test'.encode(m.group(2)):
                                raise UnicodeError('XML document is using \'{}\' encoding which is not compatible with \'US-ASCII\' encoding.'.format(m.group(2)))
                        except LookupError:
                            raise UnicodeError('XML document is using unknown \'{}\' encoding.'.format(m.group(2)))
                m = re_invalid_ascii.search(s)
                if m:
                    raise UnicodeError('Invalid ASCII character \'{0}\' found on line {1} column {2}.'.format('\\x%d' % ord(m.group(0)), line + 1, m.start(0) + 1))
        except UnicodeError as e:
            hint = xbrl.Error.create('{exception}', exception=xbrl.Error.Param(str(e), quotes=False))
            error_log.report(xbrl.Error.create('[EFM.5.2.1.1] File {uri} is not a valid ASCII dcoument.', uri=uri, children=[hint]))


def check_valid_html(elem, catalog, baseuri, errors, table=None):
    if elem.local_name == 'a':
        href = elem.find_attribute('href')
        if href:
            href_url = urlparse(href.normalized_value)
            if href_url.scheme != '' and not re_html_href.fullmatch(href.normalized_value):
                errors.append(xbrl.Error.create('[EFM.5.2.2.3] Reference to {href:value} is not allowed in attribute {href} in element {a}.', location='href:value', href=href, a=elem))
    elif elem.local_name == 'img':
        src = elem.find_attribute('src')
        if src and not re_html_src.fullmatch(src.normalized_value):
            errors.append(xbrl.Error.create('[EFM.5.2.2.3] Reference to {src:value} is not allowed in attribute {src} in element {img}.', location='src:value', src=src, img=elem))
        else:
            try:
                imageuri = urljoin(baseuri, src.normalized_value)
                if imghdr.what(imageuri, altova.open(imageuri, catalog=catalog, mode='rb').read()) not in ('gif', 'jpeg'):
                    errors.append(xbrl.Error.create('[EFM.5.2.2.3] Image {src:value} referenced in attribute {src} in element {img} is not a valid GIF or JPEG image.', location='src:value', src=src, img=elem))
            except OSError:
                errors.append(xbrl.Error.create('[EFM.5.2.2.3] Image {src:value} referenced in attribute {src} in element {img} cannot be opened.', location='src:value', src=src, img=elem))
    elif elem.local_name == 'table':
        if table is not None:
            errors.append(xbrl.Error.create('[EFM.5.2.2.3] Element {table} cannot be nested with another table element {table2}.', location='table', table=elem, table2=table))
        else:
            table = elem

    for child in elem.element_children():
        check_valid_html(child, catalog, baseuri, errors, table)


def find_directed_cycle(network, node, path, visited, cycle):
    if node in path:
        return True
    path.add(node)
    visited.append(node)
    for rel in network.relationships_from(node):
        if find_directed_cycle(network, rel.target, path, visited, cycle):
            cycle.append(rel)
            return True
    path.remove(node)
    return False


def detect_directed_cycles(network):
    roots = set([rel.source for rel in network.relationships])
    while len(roots):
        cycle = []
        visited = []
        if find_directed_cycle(network, roots.pop(), set(), visited, cycle):
            return cycle
        for node in visited:
            roots.discard(node)
    return []


def check_undirected_drs_cycles(drs, rel, visited):
    if rel.target in visited:
        return rel.target
    visited.add(rel.target)
    for nextrel in drs.consecutive_relationships(rel):
        node = check_undirected_drs_cycles(drs, nextrel, visited)
        if node is not None:
            return node
    return None


def has_concepts_in_presentation_linkbase(dts, concept1, concept2):
    for link_role in dts.presentation_link_roles():
        network = dts.presentation_base_set(link_role).network_of_relationships()
        if len(list(network.relationships_from(concept1))) or len(list(network.relationships_to(concept1))):
            if len(list(network.relationships_from(concept2))) or len(list(network.relationships_to(concept2))):
                return True
    return False


def parse_edgar_taxonomies(uri_edgar_taxonomies, catalog, error_log):
    (edgar_taxonomies, log) = xml.Instance.create_from_url(uri_edgar_taxonomies, catalog=catalog)
    if not edgar_taxonomies:
        error_log.report(xbrl.Error.create('Failed to load list of allowed standard taxonomies from %s.' % uri_edgar_taxonomies, children=log.errors))
        return None, []

    taxonomies = []
    version = edgar_taxonomies.document_element.find_attribute('version').normalized_value
    for loc in edgar_taxonomies.document_element.element_children():
        entry = {}
        for child in loc.element_children():
            entry[child.local_name] = child.text_content()
        taxonomies.append(entry)
    return version, taxonomies


def parse_edbody_dtd(uri_edbody_dtd, catalog, error_log):
    (edbody_dtd, log) = xml.dtd.DTD.create_from_url(uri_edbody_dtd, catalog=catalog)
    if not edbody_dtd:
        error_log.report(xbrl.Error.create('Failed to load HTML DTD from %s.' % uri_edbody_dtd, children=log.errors))
    return edbody_dtd


def calc_base_to_derived_types(schema):
    base_to_derived_types = {}
    for type in schema.type_definitions:
        derived_types = base_to_derived_types.setdefault(type.base_type_definition, [])
        derived_types.append(type)
    return base_to_derived_types


def get_derived_types(base_to_derived_types, type, derived_types):
    derived_types.add(type)
    for derived_type in base_to_derived_types.get(type, set()):
        get_derived_types(base_to_derived_types, derived_type, derived_types)


def validate_contexts(instance, error_log, CIK, contextrefs, used_concepts, standard_namespace2uris):
    contexts_with_start_date = []
    for context in instance.contexts:
        period = context.period
        if period.is_start_end() and (period.end_date.value - period.start_date.value) > hours24:
            contexts_with_start_date.append((context, period.start_date.value))
    contexts_with_start_date.sort(key=lambda x: x[1])
    start_dates = [x[1] for x in contexts_with_start_date]

    cikValue = None
    unique_contexts = {}
    required_contexts = set()
    for context in instance.contexts:
        identifier = context.entity.identifier

        # 6.5.1 The scheme attribute of the xbrli:identifier element must be http://www.sec.gov/CIK.
        if identifier.scheme != 'http://www.sec.gov/CIK':
            error_log.report(xbrl.Error.create('[EFM.6.5.1] Identifier {scheme} must contain value {CIK}, not {scheme:value}.', location='scheme:value', CIK='http://www.sec.gov/CIK', scheme=identifier.element.find_attribute('scheme')))

        # 6.5.2 An xbrli:identifier element must have the CIK of the registrant as its content.
        # The EFM test suite classify all CIK mismatch errors as EFM.6.5.23 (and not 6.5.2).
        if not re_cik.fullmatch(identifier.value):
            error_log.report(xbrl.Error.create('[EFM.6.5.23] Identifier value {identifier:value} must be a CIK containing exactly ten digits from 0 to 9.', location='identifier:value', identifier=identifier))
        elif CIK is not None and CIK != identifier.value:
            error_log.report(xbrl.Error.create('[EFM.6.5.23] Identifier value {identifier:value} does not match the company\'s CIK {CIK}.', location='identifier:value', identifier=identifier, CIK=CIK))

        # 6.5.3 All xbrli:identifier elements in an instance must have identical content.
        if cikValue is None:
            cikValue = identifier.value
        elif cikValue != identifier.value:
            identifier2 = next(instance.contexts).entity.identifier
            error_log.report(xbrl.Error.create('[EFM.6.5.3] Identifer values {identifier:value} and {identifier2:value} are not equal.', location='identifier:value', identifier=identifier, identifier2=identifier2))

        # 6.5.4 The xbrli:scenario element must not appear in any xbrli:context.
        if context.scenario is not None:
            error_log.report(xbrl.Error.create('[EFM.6.5.4] Scenario element {elem} is not allowed in context {context}.', location=context.scenario.element, elem=context.scenario.element, context=context))

        if context.entity.segment is not None:
            # 6.5.5 If an xbrli:segment element appears in a context, then its children must be one or more xbrldi:explicitMember elements.
            for child in context.entity.segment.non_xdt_child_elements:
                error_log.report(xbrl.Error.create('[EFM.6.5.5] Element {elem} is not allowed in segment of context {context}.', location=child, elem=child, context=context))
            for member in context.entity.segment.explicit_members:
                used_concepts.setdefault(member.value, False)

        # 6.5.7 An instance must not contain duplicate xbrli:context elements.
        cs = xbrl.ConstraintSet(context)
        if unique_contexts.setdefault(cs, context) != context:
            context2 = unique_contexts[cs]
            error_log.report(xbrl.Error.create('[EFM.6.5.7] Context {context} is a duplicate of context {context2}.', location=context.element, context=context, context2=context2))

        # 6.5.8 Every xbrli:context element must appear in at least one contextRef attribute in the same instance.
        if context.id not in contextrefs:
            error_log.report(xbrl.Error.create('[EFM.6.5.8] Context {context} is not referenced by any facts.', location=context.element, context=context))

        period = context.period
        if period.is_start_end():
            if period.start_date.value.time() == midnight and period.end_date.value.time() == midnight and context.entity.segment is None:
                required_contexts.add(context)

            # 6.5.9 If the duration of a context is more than 24 hours, then its endDate datetime value must not be greater than the startDate datetime of any other context by 24 hours or less.
            if period.is_start_end() and (period.end_date.value - period.start_date.value) > hours24:
                i = bisect.bisect(start_dates, period.end_date.value)
                if i > 0:
                    td = period.end_date.value - start_dates[i - 1]
                    if td and td <= hours24:
                        context2 = contexts_with_start_date[i - 1][0]
                        error_log.report(xbrl.Error.create('[EFM.6.5.9] Period of context {context} overlaps with period of context {context2}.', location=context.element, context=context, context2=context2))

        elif period.is_forever():
            # 6.5.38 Do not use element xbrli:forever in contexts.
            error_log.report(xbrl.Error.create('[EFM.6.5.38] Element {forever} is not allowed within a period.', forever=period.forever))

        for dim_value in context.dimension_aspect_values:
            if isinstance(dim_value, xbrl.TypedDimensionAspectValue) and dim_value.dimension.target_namespace not in standard_namespace2uris:
                # 6.5.39 The dimension of xbrli:typedMember must be defined in a standard taxonomy.
                error_log.report(xbrl.Error.create('[EFM.6.5.39] Context {context} references typed dimension {dim} from non standard taxonomy {tns}.', context=context, dim=dim_value.dimension, tns=dim_value.dimension.target_namespace))

    return cikValue, required_contexts


def decimal_comparison(fact1, fact2, cmp):
    """Rounds both numerical facts to the least accurate precision of both facts and calls the given cmp function with the rounded decimal values."""
    decimals = min(fact1.decimals, fact2.decimals)
    if decimals == float('inf'):
        return cmp(fact1.numeric_value, fact2.numeric_value)
    val1 = fact1.numeric_value.scaleb(decimals).quantize(1, decimal.ROUND_HALF_EVEN).scaleb(-decimals)
    val2 = fact2.numeric_value.scaleb(decimals).quantize(1, decimal.ROUND_HALF_EVEN).scaleb(-decimals)
    return cmp(val1, val2, decimals)


def v_equals(fact, fact2):
    if not fact.xsi_nil and fact.concept.is_numeric():
        return decimal_comparison(fact, fact2, lambda x, y, d=None: x == y)
    return fact.normalized_value == fact2.normalized_value


def validate_facts(instance, error_log, catalog, domainItemTypes, textBlockItemTypes, edbody_dtd, is_ixbrl):
    unique_facts = {}
    contextrefs = set()
    used_concepts = {}
    for fact in instance.facts:
        if not used_concepts.setdefault(fact.concept, not fact.xsi_nil) and not fact.xsi_nil:
            used_concepts[fact.concept] = True
        if isinstance(fact, xbrl.Item):
            contextrefs.add(fact.contextRef)

            # 6.5.12 An instance must not have more than one fact having the same element name, equal contextRef attributes, and if they are present, equal unitRef attributes and xml:lang attributes, respectively, unless their fact values are the same.
            key = (fact.qname, fact.contextRef, fact.unitRef, 'en-US' if fact.xml_lang is None else fact.xml_lang)
            if unique_facts.setdefault(key, fact) != fact:
                fact2 = unique_facts[key]
                if not v_equals(fact, fact2):
                    error_log.report(xbrl.Error.create('[EFM.6.5.12] Fact {fact} is a duplicate of fact {fact2} in context {context}.', location=fact.element, fact=fact, fact2=fact2, context=fact.context))

            # 6.5.15 If the un-escaped content of a fact with base type us-types:textBlockItemType or a type equal to or derived by restriction of the type 'escapedItemType' in a standard taxonomy schema namespace contains the '<' character followed by a QName and whitespace, '/>' or '>', then the un-escaped content must contain only a sequence of text and XML nodes.
            # 6.5.16 Facts of type 'text block' whose un-escaped content contains markup must satisfy the content model of the BODY tag as defined in 5.2.2.
            if not is_ixbrl and not fact.xsi_nil and fact.concept.type_definition in textBlockItemTypes:
                if re_html_stag.search(fact.normalized_value):
                    html = ''.join(('<body>', fact.normalized_value, '</body>'))
                    (xsi, log) = xml.Instance.create_from_buffer(html.encode(), dtd=edbody_dtd, catalog=catalog)
                    errors = list(log.errors)
                    if xsi:
                        check_valid_html(xsi.document_element, catalog, instance.uri, errors)
                    if errors:
                        (xsi2, log2) = xml.Instance.create_from_buffer(html.encode())
                        if not xsi2:
                            error_log.report(xbrl.Error.create('[EFM.6.5.15] The un-escaped content of textBlockItem {fact} must be XML well-formed.', fact=fact, children=list(log2.errors)))
                        else:
                            error_log.report(xbrl.Error.create('[EFM.6.5.16] The un-escaped content of textBlockItem {fact} must satisfy the content model of the HTML BODY tag.', fact=fact, children=errors))

            # 6.5.17 The xbrli:xbrl element must not have any facts with the precision attribute.
            if fact.precision is not None:
                error_log.report(xbrl.Error.create('[EFM.6.5.17] Attribute {precision} is not allowed on fact {fact}.', location=fact.element.find_attribute('precision'), precision=fact.element.find_attribute('precision'), fact=fact))

            # 6.5.25 Elements with a type attribute equal to or a restriction of 'domainItemType' in a standard taxonomy schema target namespace must not appear as facts in an instance.
            if fact.concept.type_definition in domainItemTypes:
                error_log.report(xbrl.Error.create('[EFM.6.5.25] Domain item {fact} must not appear as fact in the instance.', fact=fact))

            # 6.5.37 The decimals attribute value must not cause non-zero digits in the fact value to be interpreted as zero.
            if fact.decimals is not None and fact.decimals != float('inf'):
                if fact.numeric_value != fact.effective_numeric_value and (not fact.numeric_value.is_nan() or not fact.effective_numeric_value.is_nan()):
                    error_log.report(xbrl.Error.create('[EFM.6.5.37] Value {fact:value} rounded to {decimals:value} significant figures is {rounded_value} which is not equal to the original value of fact {fact}.',
                                                       location='decimals:value', fact=fact, rounded_value=str(fact.effective_numeric_value), decimals=fact.element.find_attribute('decimals')))

    # 6.5.14 An instance having a fact with non-nil content and the xml:lang attribute not equal to 'en-US' must also contain a fact using the same element and all other attributes with an xml:lang attribute equal to 'en-US'.
    for key in unique_facts:
        if key[3] != 'en-US':
            key2 = list(key)
            key2[3] = 'en-US'
            if tuple(key2) not in unique_facts:
                fact = unique_facts[key]
                error_log.report(xbrl.Error.create('[EFM.6.5.14] Fact {fact} does not have a corresponding en-US fact.', location=fact.element, fact=fact))

    return contextrefs, used_concepts


def validate_required_facts(instance, error_log, taxonomy_per_type, required_contexts, cikValue, cikNames, submissionType):
    main_prefix = 'us-gaap' if 'us-gaap' in taxonomy_per_type else 'ifrs-full' if 'ifrs-full' in taxonomy_per_type else None
    dei_namespace = None if 'dei' not in taxonomy_per_type else taxonomy_per_type['dei'][0].target_namespace
    qname_DocumentType = xml.QName('DocumentType', dei_namespace, 'dei')
    qname_DocumentPeriodEndDate = xml.QName('DocumentPeriodEndDate', dei_namespace, 'dei')
    qname_AmendmentFlag = xml.QName('AmendmentFlag', dei_namespace, 'dei')
    qname_AmendmentDescription = xml.QName('AmendmentDescription', dei_namespace, 'dei')
    qname_EntityRegistrantName = xml.QName('EntityRegistrantName', dei_namespace, 'dei')
    qname_EntityCentralIndexKey = xml.QName('EntityCentralIndexKey', dei_namespace, 'dei')
    qname_EntityCurrentReportingStatus = xml.QName('EntityCurrentReportingStatus', dei_namespace, 'dei')
    qname_EntityVoluntaryFilers = xml.QName('EntityVoluntaryFilers', dei_namespace, 'dei')
    qname_CurrentFiscalYearEndDate = xml.QName('CurrentFiscalYearEndDate', dei_namespace, 'dei')
    qname_EntityFilerCategory = xml.QName('EntityFilerCategory', dei_namespace, 'dei')
    qname_EntityWellKnownSeasonedIssuer = xml.QName('EntityWellKnownSeasonedIssuer', dei_namespace, 'dei')
    qname_EntityPublicFloat = xml.QName('EntityPublicFloat', dei_namespace, 'dei')
    qname_DocumentFiscalYearFocus = xml.QName('DocumentFiscalYearFocus', dei_namespace, 'dei')
    qname_DocumentFiscalPeriodFocus = xml.QName('DocumentFiscalPeriodFocus', dei_namespace, 'dei')
    qname_EntityCommonStockSharesOutstanding = xml.QName('EntityCommonStockSharesOutstanding', dei_namespace, 'dei')
    qname_StatementClassOfStockAxis = xml.QName('StatementClassOfStockAxis', taxonomy_per_type[main_prefix][0].target_namespace, main_prefix) if main_prefix in taxonomy_per_type else None
    qname_ClassesOfShareCapitalAxis = xml.QName('ClassesOfShareCapitalAxis', taxonomy_per_type[main_prefix][0].target_namespace, main_prefix) if main_prefix in taxonomy_per_type else None    

    required_entity_elements = {
        '10-K': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityCurrentReportingStatus, qname_EntityVoluntaryFilers, qname_CurrentFiscalYearEndDate, qname_EntityFilerCategory, qname_EntityWellKnownSeasonedIssuer, qname_EntityPublicFloat, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '10-KT': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityCurrentReportingStatus, qname_EntityVoluntaryFilers, qname_CurrentFiscalYearEndDate, qname_EntityFilerCategory, qname_EntityWellKnownSeasonedIssuer, qname_EntityPublicFloat, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '10-Q': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_EntityFilerCategory, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '10-QT': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_EntityFilerCategory, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '20-F': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityCurrentReportingStatus, qname_CurrentFiscalYearEndDate, qname_EntityFilerCategory, qname_EntityWellKnownSeasonedIssuer, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '40-F': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityCurrentReportingStatus, qname_CurrentFiscalYearEndDate, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '6-K': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        'N-CSR': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        'N-Q': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        'NCSRS': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_CurrentFiscalYearEndDate, qname_DocumentFiscalYearFocus, qname_DocumentFiscalPeriodFocus],
        '10': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        'S-1': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        'S-3': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        'S-4': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        'S-11': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        'POS AM': [qname_EntityRegistrantName, qname_EntityCentralIndexKey, qname_EntityFilerCategory],
        '8-K': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        'F-1': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        'F-3': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        'F-10': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        '497': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        '485BPOS': [qname_EntityRegistrantName, qname_EntityCentralIndexKey],
        'Other': [qname_EntityRegistrantName, qname_EntityCentralIndexKey]
    }

    document_type_value = None
    if not len(required_contexts):
        # 6.5.19 An instance covering a reporting period must contain a Required Context that is an xbrli:context having xbrli:startDate equal to 00:00:00 on the first day of the reporting period and xbrli:endDate equal to 24:00:00 on its last day.
        error_log.report(xbrl.Error.create('[EFM.6.5.19] Instance {xbrl} must contain a required context.', xbrl=instance.document_element))
    else:
        # 6.5.20 For each required Document Information element, an instance must contain a fact with that element and a contextRef attribute referring to its Required Context.
        facts = [fact for fact in instance.facts.filter(qname_DocumentType) if fact.context in required_contexts]
        for fact in facts:
            document_type = fact
            document_type_value = document_type.normalized_value
            required_context = document_type.context

            if document_type_value not in supported_document_types:
                error_log.report(xbrl.Error.create('[EFM.6.5.20] Unknown document type {DocumentType:value} in fact {DocumentType} in required context {context}.', DocumentType=document_type, context=required_context))
            else:
                if 'rr' in taxonomy_per_type and document_type_value not in ['485BPOS', '497']:
                    error_log.report(xbrl.Error.create('[EFM.6.22.3] Taxonomy RR may not be used with document type {DocumentType}.', DocumentType=document_type))
                if 'ifrs-full' in taxonomy_per_type and document_type_value in ['485BPOS', '497', 'K SDR', 'L SDR']:
                    error_log.report(xbrl.Error.create('[EFM.6.22.3] Taxonomy IFRS may not be used with document type {DocumentType}.', DocumentType=document_type))
                if submissionType is not None:
                    if submissionType not in submission_types:
                        error_log.report(xbrl.Error.create('[EFM.6.5.20] Unknown submission type {submissionType}.', severity=xml.ErrorSeverity.WARNING, submissionType=submissionType))
                    elif document_type_value not in submission_types[submissionType]:
                        error_log.report(
                            xbrl.Error.create(
                                '[EFM.6.5.20] Document type {DocumentType:value} in fact {DocumentType} in required context {context} is not allowed for submission type {submissionType}.',
                                DocumentType=document_type,
                                context=required_context,
                                submissionType=submissionType))

        required_context = next(iter(required_contexts)) if len(required_contexts) == 1 else None
        required_context_text = 'required context {context}' if required_context else 'a required context'
        if not facts:
            error_log.report(xbrl.Error.create('[EFM.6.5.20] Instance {xbrl} must contain a {qname} fact in %s.' % required_context_text, xbrl=instance.document_element, qname=qname_DocumentType, context=required_context))

        # 6.5.20 For each required Document Information element, an instance must contain a fact with that element and a contextRef attribute referring to its Required Context.
        facts = [fact for fact in instance.facts.filter(qname_DocumentPeriodEndDate) if fact.context in required_contexts]
        if not facts:
            error_log.report(xbrl.Error.create('[EFM.6.5.20] Instance {xbrl} must contain a {qname} fact in %s.' % required_context_text, xbrl=instance.document_element, qname=qname_DocumentPeriodEndDate, context=required_context))

        # 6.5.20 For each required Document Information element, an instance must contain a fact with that element and a contextRef attribute referring to its Required Context.
        amendment_flag = None
        facts = [fact for fact in instance.facts.filter(qname_AmendmentFlag) if fact.context in required_contexts]
        if not facts:
            error_log.report(xbrl.Error.create('[EFM.6.5.20] Instance {xbrl} must contain a {qname} fact in %s.' % required_context_text, severity=xml.ErrorSeverity.WARNING, xbrl=instance.document_element, qname=qname_AmendmentFlag, context=required_context))
        else:
            amendment_flag = facts[0]
        amendment_flag_value = amendment_flag.element.schema_actual_value if amendment_flag is not None else False

        # 6.5.20 For each required Document Information element, an instance must contain a fact with that element and a contextRef attribute referring to its Required Context.
        facts = [fact for fact in instance.facts.filter(qname_AmendmentDescription) if fact.context in required_contexts]
        if not facts and amendment_flag_value:
            error_log.report(xbrl.Error.create('[EFM.6.5.20] Instance {xbrl} must contain a {qname} fact in the required context {context} when {amendment_flag} was set to {amendment_flag:value}.',
                                               severity=xml.ErrorSeverity.WARNING, xbrl=instance.document_element, qname=qname_AmendmentDescription, context=amendment_flag.context, amendment_flag=amendment_flag))
        elif facts and not amendment_flag_value:
            for fact in facts:
                error_log.report(xbrl.Error.create('[EFM.6.5.20] Fact {fact} must not appear in the required context {context} when {amendment_flag} was set to {amendment_flag:value}.',
                                                   severity=xml.ErrorSeverity.WARNING, location=fact, fact=fact, context=fact.context, amendment_flag=amendment_flag))

        # 6.5.21 An instance must contain one non-empty fact for each required Entity Information element, each with a contextRef attribute referring to a Required Context. The value of an EntityPublicFloat fact in an instance will be 0 for an entity that has only public debt.
        for qname in required_entity_elements.get(document_type_value, []):
            facts = [fact for fact in instance.facts.filter(qname) if fact.context in required_contexts or (fact.context.period.is_instant() and fact.context.entity.segment is None)]
            if not facts or not any(not fact.xsi_nil for fact in facts):
                severity = xml.ErrorSeverity.ERROR if qname in (qname_EntityRegistrantName, qname_EntityCentralIndexKey) else xml.ErrorSeverity.WARNING
                concept = instance.dts.resolve_concept(qname)
                error_log.report(xbrl.Error.create('[EFM.6.5.21] Instance {xbrl} must contain a non-empty {concept} fact in %s.' % required_context_text, severity=severity, xbrl=instance.document_element, concept=concept if concept else qname, context=required_context))
            elif qname == qname_EntityCentralIndexKey:
                # 6.5.23 The contents of the dei:EntityCentralIndexKey fact in the Required Context must equal the content of the xbrli:identifier element in that context.
                for fact in facts:
                    if fact.normalized_value != fact.context.entity.identifier.value:
                        error_log.report(xbrl.Error.create('[EFM.6.5.23] Value {fact:value} in fact {fact} must match the identifier value {identifier:value} in required context {context}.', location='fact:value', fact=fact, identifier=fact.context.entity.identifier, context=fact.context))
            elif qname == qname_EntityRegistrantName and cikValue in cikNames:
                # 6.5.24 The official Registrant Name that corresponds to the CIK of the xbrli:identifier text content must be a case-insensitive prefix of the dei:EntityRegistrantName fact in the Required Context, unless the xbrli:identifier value is 0000000000.
                for fact in facts:
                    if not fact.normalized_value.lower().startswith(cikNames[cikValue].lower()):
                        error_log.report(xbrl.Error.create('[EFM.6.5.24] Official registrant name {name} is not a case-insenstive prefix of value {fact:value} in fact {fact} in required context {context}.', location='fact:value', fact=fact, name=cikNames[cikValue], context=fact.context))

        # 6.5.26 An instance with dei:DocumentType of 10-K, 10-Q, 20-F, 10-KT, 10-QT, or 40-F must have at least one non-empty dei:EntityCommonStockSharesOutstanding fact for each class of stock or other units of ownership outstanding.
        if document_type_value in ('10-K', '10-Q', '20-F', '10-KT', '10-QT', '40-F'):
            required_context_fact = []
            class_of_stock_facts = {}
            facts = instance.facts.filter(qname_EntityCommonStockSharesOutstanding, allow_nil=False)
            for fact in facts:
                if fact.context.entity.segment is None:
                    class_of_stock_facts.setdefault(None, []).append(fact)
                else:
                    explicit_members = list(fact.context.entity.segment.explicit_members)
                    if len(explicit_members) == 1 and explicit_members[0].dimension.qname in (qname_StatementClassOfStockAxis, qname_ClassesOfShareCapitalAxis):
                        class_of_stock_facts.setdefault(explicit_members[0].value, []).append(fact)

            if not len(class_of_stock_facts):
                error_log.report(xbrl.Error.create('[EFM.6.5.26] Missing fact {qname} in %s.' % required_context_text, severity=xml.ErrorSeverity.WARNING, location=instance.document_element, qname=qname_EntityCommonStockSharesOutstanding, context=required_context))
            elif len(class_of_stock_facts) == 1 and None not in class_of_stock_facts:
                for fact in next(iter(class_of_stock_facts.values())):
                    error_log.report(xbrl.Error.create('[EFM.6.5.26] Fact {fact} in context {context} must be reported without a StatementClassOfStockAxis.', severity=xml.ErrorSeverity.WARNING, fact=fact, context=fact.context))
            elif len(class_of_stock_facts) > 1 and None in class_of_stock_facts:
                for fact in class_of_stock_facts[None]:
                    error_log.report(xbrl.Error.create('[EFM.6.5.26] Fact {fact} in context {context} must be reported with a StatementClassOfStockAxis.', severity=xml.ErrorSeverity.WARNING, fact=fact, context=fact.context))


def validate_units(instance, error_log):
    unique_units = {}
    for unit in instance.units:
        # 6.5.11 Element xbrli:xbrl must not have duplicate child xbrli:unit elements.
        if unique_units.setdefault(unit.aspect_value, unit) != unit:
            unit2 = unique_units[unit.aspect_value]
            error_log.report(xbrl.Error.create('[EFM.6.5.11] Unit {unit} is a duplicate of unit {unit2}.', location=unit.element, unit=unit, unit2=unit2))

        # 6.5.36 The local name part of the content of xbrli:measure in UTF-8 must not exceed 200 bytes in length.
        for measure in unit.numerator_measures:
            if len(measure.value.local_name.encode('utf-8')) > 200:
                error_log.report(xbrl.Error.create('[EFM.6.5.36] The local name part {name:value} in {measure} of unit {unit} must not exceed 200 bytes in UTF-8.', location='name:value', name=xbrl.Error.Param(measure.value.local_name, location=measure), measure=measure, unit=unit))
        for measure in unit.denominator_measures:
            if len(measure.value.local_name.encode('utf-8')) > 200:
                error_log.report(xbrl.Error.create('[EFM.6.5.36] The local name part {name:value} in {measure} of unit {unit} must not exceed 200 bytes in UTF-8.', location='name:value', name=xbrl.Error.Param(measure.value.local_name, location=measure), measure=measure, unit=unit))


def validate_labels(instance_uri, dts, error_log):
    label_to_concept = {}
    for label_role in dts.label_link_roles():
        net = dts.label_base_set(label_role).network_of_relationships()
        for rel in net.relationships:
            concept = rel.source
            label = rel.target
            # 6.10.4 The DTS of an instance must have no distinct elements having the same English standard label (xml:lang attribute equal to 'en-US').
            if label.xml_lang == 'en-US' and label.xlink_role == 'http://www.xbrl.org/2003/role/label':
                concept2 = label_to_concept.setdefault(label.text, concept)
                if concept != concept2:
                    # Avoid cluttering of error log when two versions of the same standard taxonomy have been imported
                    if is_extension_document(instance_uri, concept.document) or is_extension_document(instance_uri, label_to_concept[label.text].document):
                        error_log.report(xbrl.Error.create('[EFM.6.10.4] Concepts {concept} and {concept2} must not have the same English standard label text {label:value}.', location=concept, concept=concept, concept2=concept2, label=xbrl.Error.Param(label.text, location=label.element)))

            # 6.10.9 Non-numeric elements must not have labels whose xlink:role value implies they apply to numeric values.
            if label.xlink_role in numeric_roles and isinstance(concept, xbrl.taxonomy.Item) and concept.is_non_numeric():
                error_log.report(xbrl.Error.create('[EFM.6.10.9] Non-numeric concept {concept} must not be linked to a label resource with numeric role {role:value}.',
                                                   location=concept, concept=concept, role=xbrl.Error.Param(label.xlink_role, location=label.element.find_attribute(('role', xlink_namespace)))))

    # for concept in dts.items:
    #   # 6.10.4 The DTS of an instance must have no distinct elements having the same English standard label (xml:lang attribute equal to 'en-US').
    #   for label in concept.labels(label_role='http://www.xbrl.org/2003/role/label',lang='en-US'):
    #       if label.xml_lang == 'en-US':
    #           concept2 = label_to_concept.setdefault(label.text,concept)
    #           if concept != concept2:
    #               # Avoid cluttering of error log when two versions of the same standard taxonomy have been imported
    #               if is_extension_document(instance_uri,concept.document) or is_extension_document(instance_uri,label_to_concept[label.text].document):
    #                   error_log.report(xbrl.Error.create('[EFM.6.10.4] Concepts {concept} and {concept2} must not have the same English standard label text {label:value}.', location=concept, concept=concept, concept2=concept2, label=xbrl.Error.Param(label.text,location=label.element)))
    #
    #   # 6.10.9 Non-numeric elements must not have labels whose xlink:role value implies they apply to numeric values.
    #   if concept.is_non_numeric():
    #       for label in concept.labels():
    #           if label.xlink_role in numeric_roles:
    #               error_log.report(xbrl.Error.create('[EFM.6.10.9] Non-numeric concept {concept} must not be linked to a label resource with numeric role {role:value}.', location=concept, concept=concept, role=xbrl.Error.Param(label.xlink_role,location=label.element.find_attribute(('role',xlink_namespace)))))


def validate(instance_uri, instance, error_log, catalog=xml.Catalog.root_catalog(), **params):

    # instance object will be None if XBRL 2.1 validation was not successful
    if instance is None:
        # 6.4.3 The XBRL instance documents in a submission must be XBRL 2.1 valid.
        xbrl_errors = list(error_log.errors)
        error_log.clear()
        error_log.report(xbrl.Error.create('[EFM.6.4.3] Instance {uri} is not a valid XBRL 2.1 document.', location=instance_uri, children=xbrl_errors, uri=instance_uri))
        return

    CIK = params.get('CIK')
    submissionType = params.get('submissionType')

    cikList = params.get('cikList', '').split(',')
    cikNameList = params.get('cikNameList', '').split('|Edgar|')
    if len(cikList) == len(cikNameList):
        cikNames = dict(zip(cikList, cikNameList))
    else:
        cikNames = {}
        # 6.5.24 The official Registrant Name that corresponds to the CIK of the xbrli:identifier text content must be a case-insensitive prefix of the dei:EntityRegistrantName fact in the Required Context, unless the xbrli:identifier value is 0000000000.
        error_log.report(xbrl.Error.create('''[EFM.6.5.24] The specified 'cikList' and 'cikNameList' parameters must have an equal number of entries.'''))

    uri_edgar_taxonomies = params.get('edgar-taxonomies-url', urljoin('file:', pathname2url(os.path.join(os.path.dirname(__file__), 'edgartaxonomies.xml'))))
    uri_edbody_dtd = params.get('edbody-url', urljoin('file:', pathname2url(os.path.join(os.path.dirname(__file__), 'edbody.dtd'))))

    edgar_version, standard_taxonomies = parse_edgar_taxonomies(uri_edgar_taxonomies, catalog, error_log)
    standard_uris = {entry['Href'] for entry in standard_taxonomies}
    standard_authorities = {re_authority.match(entry['Namespace']).group(1) for entry in standard_taxonomies if entry['AttType'] == 'SCH'}
    standard_mapped_uris = {catalog.resolve_uri(uri): uri for uri in standard_uris}
    re_href = re.compile('(' + '|'.join(list(map('({0})'.format, standard_uris))) + '|([^/:#]*))(#[a-zA-Z_][a-zA-Z0-9_.-]*)?')

    standard_roles = set(xbrl21_roles)
    standard_arcroles = set(xbrl21_arcroles)
    standard_concept_names = {}

    standard_namespace2prefix = get_standard_namespace2prefix(standard_taxonomies)
    standard_namespace2uris = get_standard_namespace2uris(standard_taxonomies)

    taxonomy_per_type = collections.defaultdict(list)
    for taxonomy in instance.dts.taxonomy_schemas:
        prefix = standard_namespace2prefix.get(taxonomy.target_namespace)
        if prefix:
            taxonomy_per_type[prefix].append(taxonomy)

        if taxonomy.document.uri in standard_mapped_uris:
            for role_type in taxonomy.role_types:
                standard_roles.add(role_type.role_uri)
            for arcrole_type in taxonomy.arcrole_types:
                standard_arcroles.add(arcrole_type.arcrole_uri)
            for concept in taxonomy.concepts:
                standard_concept_names[concept.name] = concept

    if 'dei' not in taxonomy_per_type:
        error_log.report(xbrl.Error.create('Instance {xbrl} does not appear to be a SEC filing.', xbrl=instance.document_element))
        return

    # 6.22 Supported Versions of XBRL Standard Taxonomies
    for prefix, taxonomies in taxonomy_per_type.items():
        if prefix == 'us-gaap':
            for taxonomy in taxonomies:
                if 'srt' in taxonomy_per_type and taxonomy.target_namespace[-10:] != taxonomy_per_type['srt'][0].target_namespace[-10:]:
                    error_log.report(
                        xbrl.Error.create(
                            '[EFM.6.22.3] DTS contains the following conflicting taxonomies: {tns1} and {tns2}',
                            location=instance,
                            tns1=xbrl.Error.Param(
                                taxonomy.target_namespace,
                                location=taxonomy.document.uri),
                            tns2=xbrl.Error.Param(
                                taxonomy_per_type['srt'][0].target_namespace,
                                location=taxonomy_per_type['srt'][0].document.uri)))
        elif len(taxonomies) > 1:
            error_log.report(
                xbrl.Error.create(
                    '[EFM.6.22.3] DTS contains the following conflicting taxonomies: {tns1} and {tns2}',
                    location=instance,
                    tns1=xbrl.Error.Param(
                        taxonomy_per_type[prefix][0].target_namespace,
                        location=taxonomy_per_type[prefix][0].document.uri),
                    tns2=xbrl.Error.Param(
                        taxonomy_per_type[prefix][1].target_namespace,
                        location=taxonomy_per_type[prefix][1].document.uri)))

    if not instance_uri.endswith('.htm'):
        # 5.2.1.1 Valid ASCII Characters
        check_valid_ascii(instance.uri, catalog, error_log)

        # 6.3.3 XBRL document names must match {base}-{date}[_{suffix}].{extension}.
        if not re_xml_uri.fullmatch(instance_uri):
            error_log.report(xbrl.Error.create('[EFM.6.3.3] Instance filename {uri} does not match {pattern}.', location='uri', uri=xbrl.Error.Param(instance_uri.rsplit('/', 1)[1], tooltip=instance_uri, location=instance_uri), pattern='{base}-{date}.xml'))

    # 6.3.6 The URI content of the xlink:href attribute, the xsi:schemaLocation attribute and the schemaLocation attribute must be relative and contain no forward slashes, or a recognized external location of a standard taxonomy schema file, or a '#' followed by a shorthand xpointer.
    for schema_location in instance.schema_location_attributes:
        if schema_location.local_name == 'schemaLocation':
            for uri in schema_location.normalized_value.split()[1::2]:
                if not re_href.fullmatch(uri):
                    error_log.report(xbrl.Error.create('[EFM.6.3.6] {uri} in attribute {schemaLocation} on {xbrl} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.',
                                                       location='uri', uri=xml.Error.Param(uri, location=schema_location), schemaLocation=schema_location, xbrl=instance.document_element))
    for schemaref in instance.schema_refs:
        if not re_href.fullmatch(schemaref.xlink_href):
            href = schemaref.element.find_attribute(('href', xlink_namespace))
            error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {schemaRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, schemaRef=schemaref))
    for linkbaseref in instance.linkbase_refs:
        if not re_href.fullmatch(linkbaseref.xlink_href):
            href = linkbaseref.element.find_attribute(('href', xlink_namespace))
            error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {linkbaseRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, linkbaseRef=linkbaseref))
    for roleref in instance.role_refs:
        if not re_href.fullmatch(roleref.xlink_href):
            href = roleref.element.find_attribute(('href', xlink_namespace))
            error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {roleRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, roleRef=roleref))
    for arcroleref in instance.arcrole_refs:
        if not re_href.fullmatch(arcroleref.xlink_href):
            href = arcroleref.element.find_attribute(('href', xlink_namespace))
            error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {arcroleRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, arcroleRef=arcroleref))
    for footnote_link in instance.footnote_links:
        for loc in footnote_link.locators:
            if not re_href.fullmatch(loc.xlink_href):
                href = loc.element.find_attribute(('href', xlink_namespace))
                error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {loc} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, loc=loc))

    # 6.3.11 Attribute xml:base must not appear in any Interactive Data document.
    check_xml_base(instance.document_element, error_log)

    base_to_derived_types = calc_base_to_derived_types(instance.dts.schema)

    domainItemTypes = set()
    textBlockItemTypes = set()
    for ns in standard_namespace2uris:
        domainItemType = instance.dts.schema.resolve_type_definition(('domainItemType', ns))
        if domainItemType is not None:
            get_derived_types(base_to_derived_types, domainItemType, domainItemTypes)
        textBlockItemType = instance.dts.schema.resolve_type_definition(('textBlockItemType', ns))
        if textBlockItemType is not None:
            get_derived_types(base_to_derived_types, textBlockItemType, textBlockItemTypes)
        escapedItemType = instance.dts.schema.resolve_type_definition(('escapedItemType', ns))
        if escapedItemType is not None:
            get_derived_types(base_to_derived_types, escapedItemType, textBlockItemTypes)

    for doc in instance.dts.documents:
        if doc.uri in standard_mapped_uris:
            continue

        # 6.22 Supported Versions of XBRL Standard Taxonomies
        if not is_extension_document(instance_uri, doc):
            hint = xbrl.Error.create('Hint: See {uri} for more information.', uri=xbrl.Error.ExternalLinkParam('https://www.sec.gov/info/edgar/edgartaxonomies.shtml'))
            error_log.report(xbrl.Error.create('[EFM.6.22.2] Document {uri} is not a supported XBRL Standard Taxonomy for EDGAR version {version}.', location='uri', uri=doc.uri, children=[hint], version=edgar_version))
            continue

        # 5.2.1.1 Valid ASCII Characters
        check_valid_ascii(doc.uri, catalog, error_log)

        # 6.3.11 Attribute xml:base must not appear in any Interactive Data document.
        check_xml_base(doc.document_element, error_log)

        if isinstance(doc, xbrl.taxonomy.TaxonomySchemaDocument):
            schema = doc.schema_element

            # 6.3.3 XBRL document names must match {base}-{date}[_{suffix}].{extension}.
            if not re_xsd_uri.fullmatch(doc.uri):
                error_log.report(xbrl.Error.create('[EFM.6.3.3] Taxonomy schema filename {uri} does not match {pattern}.', location='uri', uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}.xsd'))

            # 6.3.6 The URI content of the xlink:href attribute, the xsi:schemaLocation attribute and the schemaLocation attribute must be relative and contain no forward slashes, or a recognized external location of a standard taxonomy schema file, or a '#' followed by a shorthand xpointer.
            for ref in schema.references:
                if not re_href.fullmatch(ref.schema_location):
                    schemalocation = ref.element.find_attribute('schemaLocation')
                    error_log.report(xbrl.Error.create('[EFM.6.3.6] {schemaLocation:value} in attribute {schemaLocation} on {ref} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='schemaLocation:value', schemaLocation=schemalocation, ref=ref))
            for linkbaseref in schema.linkbase_refs:
                if not re_href.fullmatch(linkbaseref.xlink_href):
                    href = linkbaseref.element.find_attribute(('href', xlink_namespace))
                    error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {linkbaseRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, linkbaseRef=linkbaseref))

            for ref in schema.references:
                # 6.7.1 The xsd:schema must not have an xsd:include element.
                if isinstance(ref, xsd.Include):
                    error_log.report(xbrl.Error.create('[EFM.6.7.1] {include} is not allowed in a company extension schema.', include=ref))
                # 6.7.2 If an xsd:import element has a namespace attribute equal to a standard taxonomy schema, then its schemaLocation attribute must be the standard taxonomy assigned to that namespace.
                elif isinstance(ref, xsd.Import):
                    if ref.namespace in standard_namespace2uris and ref.schema_location not in standard_namespace2uris[ref.namespace]:
                        error_log.report(xbrl.Error.create('[EFM.6.7.2] {xsimport} for {namespace} must point to {uri}.', location='xsimport', xsimport=ref, namespace=ref.namespace, uri=standard_namespace2uris[ref.namespace][0]))

            recommended_namespace_prefix = None
            if schema.target_namespace is None:
                    # 6.7.4 The targetNamespace attribute must match http://{authority}/{versionDate}.
                error_log.report(xbrl.Error.create('[EFM.6.7.4] Company extension schema {schema} must have a target namespace that matches {pattern}.', location='schema', schema=schema, pattern='http://{authority}/{versionDate}'))
            else:
                m = re_company_uri.fullmatch(schema.target_namespace)
                tns_attr = schema.element.find_attribute('targetNamespace')
                if m:
                    # 6.7.3 The authority part of an xsd:schema targetNamespace attribute must not equal the authority part of a targetNamespace attribute of any standard taxonomy schema.
                    if m.group(1) in standard_authorities:
                        error_log.report(xbrl.Error.create('[EFM.6.7.3] Target namespace {tns:value} must not use an authority part {authority} of a standard taxonomy schema.', location='tns:value', tns=tns_attr, authority=m.group(1)))

                    # 6.7.4 The targetNamespace attribute must match http://{authority}/{versionDate}.
                    try:
                        if m.group(3) is not None:
                            versionDate = datetime.date(int(m.group(3)), int(m.group(4)), int(m.group(5)))
                        else:
                            versionDate = datetime.date(int(m.group(6)), int(m.group(7)), int(m.group(8)))
                    except ValueError:
                        error_log.report(xbrl.Error.create('[EFM.6.7.4] Target namespace {tns:value} must match {pattern}.', location='tns:value', tns=tns_attr, pattern='http://{authority}/{versionDate}'))
                else:
                    # 6.7.4 The targetNamespace attribute must match http://{authority}/{versionDate}.
                    error_log.report(xbrl.Error.create('[EFM.6.7.4] Target namespace {tns:value} must match {pattern}.', location='tns:value', tns=tns_attr, pattern='http://{authority}/{versionDate}'))

                # 6.7.7 Element xsd:schema must bind a Recommended Namespace Prefix for the targetNamespace attribute that does not contain the underscore character.
                for attr in schema.element.namespace_attributes:
                    if attr.normalized_value == schema.target_namespace:
                        if recommended_namespace_prefix:
                            error_log.report(xbrl.Error.create('[EFM.6.7.7] Prefixes {prefix1} and {prefix2} are bound to target namesapce {tns:value}.', location=schema, prefix1=recommended_namespace_prefix.local_name, prefix2=attr.local_name, tns=tns_attr))
                        else:
                            recommended_namespace_prefix = attr
                if recommended_namespace_prefix is None or not recommended_namespace_prefix.prefix:
                    error_log.report(xbrl.Error.create('[EFM.6.7.7] Recommended namespace prefix for target namespace {tns:value} is missing.', location=schema, tns=tns_attr))
                elif '_' in recommended_namespace_prefix.local_name:
                    prefix = xbrl.Error.Param(recommended_namespace_prefix.local_name, location=recommended_namespace_prefix)
                    error_log.report(xbrl.Error.create('[EFM.6.7.7] Recommended namespace prefix {prefix} for target namespace {tns:value} must not contain an underscore character.', location='prefix', prefix=prefix, tns=tns_attr))

                # 6.7.30 The content of a targetnamespace, roleURI or arcroleURI attribute in UTF-8 must not exceed 255 bytes in length.
                if len(schema.target_namespace.encode('utf-8')) > 255:
                    error_log.report(xbrl.Error.create('[EFM.6.7.30] The target namespace {tns:value} must not exceed 255 bytes in UTF-8.', tns=tns_attr))

            # 6.7.8 Element xsd:schema must not contain any occurrences of 'embedded' linkbases.
            for linkbase in schema.linkbases:
                error_log.report(xbrl.Error.create('[EFM.6.7.8] Embedded linkbase {linkbase} not allowed in company extension schema {schema}.', location=linkbase, linkbase=linkbase, schema=schema))

            for role_type in schema.role_types:
                # 6.7.9 The roleURI attribute of a link:roleType element must begin with the same {scheme} and {authority} as the targetNamespace attribute.
                if m is not None and m.group(1) != re_authority.match(role_type.role_uri).group(1):
                    role_uri_attr = role_type.element.find_attribute('roleURI')
                    tns_attr = schema.element.find_attribute('targetNamespace')
                    error_log.report(xbrl.Error.create('[EFM.6.7.9] roleURI {roleURI:value} on {roleType} must begin with the same schema and authority as the target namespace {tns:value}.', location='roleURI:value', roleURI=role_uri_attr, roleType=role_type, tns=tns_attr))

                # 6.7.11 A link:roleType declaration with link:usedOn containing link:presentationLink, link:definitionLink or link:calculationLink must also have a link:usedOn for the other two.
                usedons = [usedon.value in (qname_presentationLink, qname_calculationLink, qname_definitionLink) for usedon in role_type.used_on]
                if usedons.count(True) > 1 and usedons.count(True) != 3:
                    error_log.report(xbrl.Error.create('[EFM.6.7.11] {roleType} must contain link:usedOn elements for presentation, calculation and definition links.', location=role_type, roleType=role_type))

                # 6.7.12 A link:roleType element must contain a link:definition child
                # element whose content will communicate the title of the section, the
                # level of facts in the instance that a presentation relationship in the
                # base set of that role would display, and sort alphanumerically into the
                # order that sections appear in the official HTML/ASCII document.
                if role_type.definition is None:
                    error_log.report(xbrl.Error.create('[EFM.6.7.12] {roleType} must contain a link:defintion element whose content matches {pattern}.', location=role_type, roleType=role_type, pattern='{SortCode} - {Type} - {Title}'))
                elif not re_definition.fullmatch(role_type.definition.value):
                    error_log.report(xbrl.Error.create('[EFM.6.7.12] The content {definition:value} of element {definition} must match {pattern}.', location='definition:value', definition=role_type.definition, pattern='{SortCode} - {Type} - {Title}'))

                # 6.7.30 The content of a targetnamespace, roleURI or arcroleURI attribute in UTF-8 must not exceed 255 bytes in length.
                if len(role_type.role_uri.encode('utf-8')) > 255:
                    error_log.report(xbrl.Error.create('[EFM.6.7.30] The roleURI {roleURI:value} must not exceed 255 bytes in UTF-8.', roleURI=role_type.element.find_attribute('roleURI')))

            for arcrole_type in schema.arcrole_types:
                # 6.7.13 The arcroleURI attribute of a link:arcroleType element must begin with the same {scheme} and {authority} parts as the targetNamespace attribute.
                if m is not None and m.group(1) != re_authority.match(arcrole_type.arcrole_uri).group(1):
                    arcrole_uri_attr = arcrole_type.element.find_attribute('arcroleURI')
                    tns_attr = schema.element.find_attribute('targetNamespace')
                    error_log.report(xbrl.Error.create('[EFM.6.7.13] arcroleURI {arcroleURI:value} on {arcroleType} must begin with the same schema and authority as the target namespace {tns:value}.', location='arcroleURI:value', arcroleURI=arcrole_uri_attr, arcroleType=arcrole_type, tns=tns_attr))

                # 6.7.15 A link:arcroleType element must have a nonempty link:definition.
                if arcrole_type.definition is None or not len(arcrole_type.definition.value):
                    error_log.report(xbrl.Error.create('[EFM.6.7.15] {arcroleType} must contain a non-empty link:defintion element.', location=arcrole_type, arcroleType=arcrole_type))

                # 6.7.30 The content of a targetnamespace, roleURI or arcroleURI attribute in UTF-8 must not exceed 255 bytes in length.
                if len(arcrole_type.arcrole_uri.encode('utf-8')) > 255:
                    error_log.report(xbrl.Error.create('[EFM.6.7.30] The arcroleURI {arcroleURI:value} must not exceed 255 bytes in UTF-8.', arcroleURI=arcrole_type.element.find_attribute('arcroleURI')))

            for component in schema.components:
                # 6.7.29 The content of an xsd:element, xsd:complexType, or xsd:simpleType name attribute in UTF-8 must not exceed 200 bytes in length.
                if isinstance(component, xsd.ElementDeclaration) or isinstance(component, xsd.TypeDefinition):
                    if len(component.name.encode('utf-8')) > 200:
                        error_log.report(xbrl.Error.create('[EFM.6.7.29] The name {name:value} of schema component {component} must not exceed 200 bytes in UTF-8.', location='name:value', component=component, name=component.element.find_attribute('name')))

            for concept in schema.concepts:
                if isinstance(concept, xbrl.taxonomy.Item):
                    # 6.7.16 The name attribute of an xsd:element must not equal any xsd:element name attribute in a standard taxonomy schema that appears in the same instance DTS.
                    concept2 = standard_concept_names.get(concept.name, None)
                    if concept2 is not None:
                        error_log.report(xbrl.Error.create('[EFM.6.7.16] Concept {concept} has the same local name as concept {concept2} in standard taxonomy schema {uri}.', location=concept, concept=concept, concept2=concept2, uri=standard_mapped_uris[concept2.document.uri]))

                    # 6.7.17 The id attribute of an xsd:element must consist of the Recommended Namespace Prefix of the element namespace, followed by one underscore, followed only by its name attribute.
                    if recommended_namespace_prefix and concept.id != '{prefix}_{name}'.format(prefix=recommended_namespace_prefix.local_name, name=concept.name):
                        name_attr = concept.element.find_attribute('name')
                        prefix = xbrl.Error.Param(recommended_namespace_prefix.local_name, location=recommended_namespace_prefix)
                        id_attr = concept.element.find_attribute('id')
                        if id_attr is not None:
                            error_log.report(
                                xbrl.Error.create(
                                    '[EFM.6.7.17] ID {id:value} of concept {concept} must be the recommended namespace prefix {prefix} followed by one underscore followed by its name {name:value}.',
                                    location='id:value',
                                    concept=concept,
                                    id=id_attr,
                                    name=name_attr,
                                    prefix=prefix))
                        else:
                            error_log.report(
                                xbrl.Error.create(
                                    '[EFM.6.7.17] Concept {concept} must have an ID consisting of the recommended namespace prefix {prefix} followed by one underscore followed by its name {name:value}.',
                                    location=concept,
                                    concept=concept,
                                    id=id_attr,
                                    name=name_attr,
                                    prefix=prefix))

                    # 6.7.18 The nillable attribute value of an xsd:element must equal 'true'.
                    nillable = concept.element.find_attribute('nillable')
                    if nillable.specified and nillable.normalized_value != 'true':
                        error_log.report(xbrl.Error.create('[EFM.6.7.18] Attribute nillable {nillable:value} of concept {concept} must be true.', location='nillable:value', nillable=nillable, concept=concept))

                    # 6.7.20 An xsd:element must not have an xbrldt:typedDomainRef attribute.
                    typedDomainRef = concept.element.find_attribute(('typedDomainRef', xbrldt_namespace))
                    if typedDomainRef is not None:
                        error_log.report(xbrl.Error.create('[EFM.6.7.20] Concept {concept} must not have a {typedDomainRef} attribute.', location=typedDomainRef, concept=concept, typedDomainRef=typedDomainRef))

                    # 6.7.21 If the abstract attribute of xsd:element is 'true', then the xbrli:periodType attribute must be 'duration'.
                    if concept.abstract and concept.period_type != xbrl.taxonomy.PeriodType.DURATION:
                        period_type_attr = concept.element.find_attribute(('periodType', 'http://www.xbrl.org/2003/instance'))
                        error_log.report(xbrl.Error.create('[EFM.6.7.21] Abstract concept {concept} must be of an duration period type.', location='periodType:value', concept=concept, periodType=period_type_attr))

                    # 6.7.23 The xsd:element substitutionGroup attribute must equal 'xbrldt:dimensionItem' if and only if the name attribute ends with 'Axis'.
                    if concept.name.endswith('Axis'):
                        if not isinstance(concept, xbrl.xdt.Dimension):
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.23] Concept {concept} with name {name:value} ending in Axis must be a dimension.', location='name:value', concept=concept, name=name_attr))
                    else:
                        if isinstance(concept, xbrl.xdt.Dimension):
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.23] Concept {concept} with name {name:value} not ending in Axis must not be a dimension.', location='name:value', concept=concept, name=name_attr))

                    # 6.7.24 The xsd:element name attribute must end with 'Table' if and only if substitutionGroup attribute equals 'xbrldt:hypercubeItem'.
                    if concept.name.endswith('Table'):
                        if not isinstance(concept, xbrl.xdt.Hypercube):
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.24] Concept {concept} with name {name:value} ending in Table must be a hypercube.', location='name:value', concept=concept, name=name_attr))
                    else:
                        if isinstance(concept, xbrl.xdt.Hypercube):
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.24] Concept {concept} with name {name:value} not ending in Axis must not be a hypercube.', location='name:value', concept=concept, name=name_attr))

                    # 6.7.25 If the xsd:element substitutionGroup attribute is not equal to 'xbrldt:dimensionItem' or equal to 'xbrldt:hypercubeItem' then it must equal 'xbrli:item'.
                    substitutionGroup = next(iter(concept.substitution_group_affiliations))
                    if substitutionGroup.qname not in (qname_item, qname_hypercubeItem, qname_dimensionItem):
                        error_log.report(xbrl.Error.create('[EFM.6.7.25] Substitution group {substitutionGroup:value} of concept {concept} must be either xbrli:item, xbrldt:hypercubeItem or xbrldt:dimensionItem.',
                                                           location='substitutionGroup:value', concept=concept, substitutionGroup=concept.element.find_attribute('substitutionGroup')))

                    # 6.7.26 If xsd:element name attribute ends with 'LineItems' then the abstract attribute must equal 'true'.
                    if concept.name.endswith('LineItems') and not concept.abstract:
                        name_attr = concept.element.find_attribute('name')
                        error_log.report(xbrl.Error.create('[EFM.6.7.26] Concept {concept} with name {name:value} ending in LineItems must be abstract.', location='name:value', concept=concept, name=name_attr))

                    # 6.7.27 The xsd:element name attribute must end with 'Domain' or 'Member' if and only if the type attribute equals or is derived from 'domainItemType' in a standard taxonomy schema target namespace.
                    if concept.name.endswith('Domain') or concept.name.endswith('Member'):
                        if concept.type_definition not in domainItemTypes:
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.27] Concept {concept} with name {name:value} ending in Domain or Member must be a derived from domainItemType.', location='name:value', concept=concept, name=name_attr))
                    else:
                        if concept.type_definition in domainItemTypes:
                            name_attr = concept.element.find_attribute('name')
                            error_log.report(xbrl.Error.create('[EFM.6.7.27] Concept {concept} with name {name:value} not ending in Domain or Member must not be derived from domainItemType.', location='name:value', concept=concept, name=name_attr))

                    # 6.7.28 If xsd:element type attribute equals or is derived from 'domainItemType' in a standard taxonomy schema target namespace then the xbrli:periodType attribute must equal 'duration'.
                    if concept.period_type != xbrl.taxonomy.PeriodType.DURATION and concept.type_definition in domainItemTypes:
                        period_type_attr = concept.element.find_attribute(('periodType', 'http://www.xbrl.org/2003/instance'))
                        error_log.report(xbrl.Error.create('[EFM.6.7.28] Concept {concept} derived from domainItemType must be of period type duration.', location='periodType:value', concept=concept, periodType=period_type_attr))

                    # 6.7.31 The xsd:element type must not be equal to or derived from xbrli:fractionItemType.
                    if concept.item_type == xbrl.taxonomy.ItemType.FRACTION:
                        type_attr = concept.element.find_attribute('type')
                        error_log.report(xbrl.Error.create('[EFM.6.7.31] Concept {concept} must not have a type equal to or derived from xbrli:fractionItemType.', location='type:value', concept=concept, type=type_attr))

                    # 6.7.32 An element declaration having a non-numeric base type, abstract not 'true', and not derived from domainItemType must have the value 'duration' for xbrli:periodType.
                    if concept.is_non_numeric() and not concept.abstract and concept.period_type != xbrl.taxonomy.PeriodType.DURATION and concept.type_definition not in domainItemTypes:
                        period_type_attr = concept.element.find_attribute(('periodType', 'http://www.xbrl.org/2003/instance'))
                        error_log.report(xbrl.Error.create('[EFM.6.7.32] Non-numeric, abstract concept {concept} not derived from domainItemType must be of period type duration.', location='periodType:value', concept=concept, periodType=period_type_attr))

                    # 6.18.1 An element that has a company specific namespace must not have a reference.
                    if len(list(concept.references())):
                        error_log.report(xbrl.Error.create('[EFM.6.18.1] Concept {concept} in a company specific namespace must not have any references.', location=concept, concept=concept))

                # 6.7.19 The xsd:element substitutionGroup attribute must not be a member of a substitution group with head 'xbrli:tuple'.
                elif isinstance(concept, xbrl.taxonomy.Tuple):
                    error_log.report(xbrl.Error.create('[EFM.6.7.19] Tuple {concept} is not allowed in a company extension taxonomy schema.', location=concept, concept=concept))

        if isinstance(doc, xbrl.taxonomy.LinkbaseDocument):
            linkbase = doc.linkbase

            # 6.3.3 XBRL document names must match {base}-{date}[_{suffix}].{extension}.
            try:
                link = next(linkbase.extended_links)
                if link.qname == qname_labelLink:
                    if not re_lab_uri.fullmatch(doc.uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.3] Label linkbase filename {uri} does not match {pattern}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}_lab.xml'))
                elif link.qname == qname_referenceLink:
                    if not re_ref_uri.fullmatch(doc.uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.3] Reference linkbase filename {uri} does not match {pattern}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}_ref.xml'))
                elif link.qname == qname_presentationLink:
                    if not re_pre_uri.fullmatch(doc.uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.3] Presentation linkbase filename {uri} does not match {pattern}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}_prexml'))
                elif link.qname == qname_calculationLink:
                    if not re_cal_uri.fullmatch(doc.uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.3] Presentation linkbase filename {uri} does not match {pattern}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}_cal.xml'))
                elif link.qname == qname_definitionLink:
                    if not re_def_uri.fullmatch(doc.uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.3] Presentation linkbase filename {uri} does not match {pattern}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri), pattern='{base}-{date}_def.xml'))
                else:
                    error_log.report(xbrl.Error.create('[EFM.6.3.3] Cannot determine linkbase type for linkbase {uri}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri)))
            except StopIteration:
                error_log.report(xbrl.Error.create('[EFM.6.3.3] Cannot determine linkbase type for linkbase {uri}.', location=doc.uri, uri=xbrl.Error.Param(doc.uri.rsplit('/', 1)[1], tooltip=doc.uri, location=doc.uri)))

    # 6.7.14 A DTS must not contain more than one link:arcroleType element with equal values of the arcroleURI attribute.
    arcrole_types = {}
    for arcrole_type in instance.dts.arcrole_types:
        if arcrole_type.arcrole_uri in arcrole_types:
            arcrole_uri_attr = arcrole_type.element.find_attribute('arcroleURI')
            error_log.report(xbrl.Error.create('[EFM.6.7.14] {arcroleType} and {arcroleType2} both have the same arcroleURI value {arcroleURI:value}.', location='arcroleURI:value', arcroleType=arcrole_type, arcroleType2=arcrole_types[arcrole_type.arcrole_uri], arcroleURI=arcrole_uri_attr))
        else:
            arcrole_types[arcrole_type.arcrole_uri] = arcrole_type

    # 6.7.10 A DTS must not contain more than one link:roleType element with equal values of the roleURI attribute.
    role_types = {}
    for role_type in instance.dts.role_types:
        if role_type.role_uri in role_types:
            role_uri_attr = role_type.element.find_attribute('roleURI')
            error_log.report(xbrl.Error.create('[EFM.6.7.10] {roleType} and {roleType2} both have the same roleURI value {roleURI:value}.', location='roleURI:value', roleType=role_type, roleType2=role_types[role_type.role_uri], roleURI=role_uri_attr))
        else:
            role_types[role_type.role_uri] = role_type

    edbody_dtd = parse_edbody_dtd(uri_edbody_dtd, catalog, error_log)

    contextrefs, used_concepts = validate_facts(instance, error_log, catalog, domainItemTypes, textBlockItemTypes, edbody_dtd, instance_uri.endswith('.htm'))

    for link in instance.footnote_links:
        to_labels = set()
        non_empty_footnotes = []

        for elem in link.element.element_children():
            if elem.namespace_name == link_namespace:
                if elem.local_name == 'loc':
                    # 6.5.29 The xlink:role attribute of a link:loc element must be empty, or defined in the XBRL Specification 2.1.
                    role_attr = elem.find_attribute(('role', xlink_namespace))
                    if role_attr is not None and role_attr.normalized_value and role_attr.normalized_value not in xbrl21_roles:
                        error_log.report(xbrl.Error.create('[EFM.6.5.29] Role {role:value} on locator {loc} must be defined in the XBRL 2.1 specification.', location='role:value', role=role_attr, loc=elem))

                    # 6.5.32 A link:footnoteLink link:loc xlink:href attribute must start with the sharp sign '#'.
                    href_attr = elem.find_attribute(('href', xlink_namespace))
                    if not href_attr.normalized_value.startswith('#'):
                        error_log.report(xbrl.Error.create('[EFM.6.5.32] URI {href:value} in attribute {href} on locator {loc} must start with \'#\'.', location='href:value', href=href_attr, loc=elem))

                elif elem.local_name == 'footnote':
                    if elem.children:
                        non_empty_footnotes.append(elem)

                    # 6.5.28 The xlink:role attribute of a link:footnote element must be defined in the XBRL Specification 2.1.
                    role_attr = elem.find_attribute(('role', xlink_namespace))
                    if role_attr is None:
                        error_log.report(xbrl.Error.create('[EFM.6.5.28] Missing attribute {role} on footnote {footnote} must be set to a standard role defined in the XBRL 2.1 specification.', location=elem, role=xml.QName('role', xlink_namespace, 'xlink'), footnote=elem))
                    elif role_attr.normalized_value not in xbrl21_roles:
                        error_log.report(xbrl.Error.create('[EFM.6.5.28] Role {role:value} on footnote {footnote} must be defined in the XBRL 2.1 specification.', location='role:value', role=role_attr, footnote=elem))

                    # 6.5.34 The content of a link:footnote element must satisfy the content model of the BODY tag as defined in 5.2.2.
                    if not instance_uri.endswith('.htm'):
                        contents = elem.serialize(omit_start_tag=True)
                        html = ''.join(('<body>', contents, '</body>')) if contents else '<body/>'
                        (xsi, log) = xml.Instance.create_from_buffer(html.encode(), dtd=edbody_dtd, catalog=catalog)
                        errors = list(log.errors)
                        if xsi:
                            check_valid_html(xsi.document_element, catalog, instance_uri, errors)
                        if errors:
                            error_log.report(xbrl.Error.create('[EFM.6.5.34] The content of footnote {footnote} must satisfy the content model of the HTML BODY tag.', footnote=elem, children=errors))

                elif elem.local_name == 'footnoteArc':
                    to_labels.add(elem.find_attribute(('to', xlink_namespace)).normalized_value)

                    # 6.5.30 The xlink:arcrole attribute of a link:footnoteArc element must be defined in the XBRL Specification 2.1 or a standard taxonomy schema.
                    arcrole_attr = elem.find_attribute(('arcrole', xlink_namespace))
                    if arcrole_attr.normalized_value not in standard_arcroles:
                        arcrole_ref = instance.arcrole_ref(arcrole_attr.normalized_value)
                        if arcrole_ref.xlink_href.partition('#')[0] not in standard_uris:
                            error_log.report(xbrl.Error.create('[EFM.6.5.30] Arcrole {arcrole:value} on footnoteArc {footnoteArc} must be defined in the XBRL 2.1 specification or a standard taxonomy schema.', location='arcrole:value', arcrole=arcrole_attr, footnoteArc=elem))
                else:
                    # 6.5.27 A link:footnoteLink element must have no children other than link:loc, link:footnote, and link:footnoteArc
                    error_log.report(xbrl.Error.create('[EFM.6.5.27] Element {elem} is not allowed under {footnoteLink}.', location=elem, elem=elem, footnoteLink=link))
            else:
                # 6.5.27 A link:footnoteLink element must have no children other than link:loc, link:footnote, and link:footnoteArc
                error_log.report(xbrl.Error.create('[EFM.6.5.27] Element {elem} is not allowed under {footnoteLink}.', location=elem, elem=elem, footnoteLink=link))

        # 6.5.33 Every nonempty link:footnote element must be linked to at least one fact.
        for elem in non_empty_footnotes:
            label_attr = elem.find_attribute(('label', xlink_namespace))
            if label_attr.normalized_value not in to_labels:
                error_log.report(xbrl.Error.create('[EFM.6.5.33] Non-empty footnote {footnote} must be linked to at least one fact.', location=elem, footnote=elem))

    cikValue, required_contexts = validate_contexts(instance, error_log, CIK, contextrefs, used_concepts, standard_namespace2uris)
    validate_units(instance, error_log)

    validate_required_facts(instance, error_log, taxonomy_per_type, required_contexts, cikValue, cikNames, submissionType)

    positive_axes = set()
    negative_axis_rels = []
    drs = instance.dts.dimensional_relationship_set()
    for baseset in instance.dts.base_sets:
        # 6.9.3 A link:linkbase in a submission must have no ineffectual relationships.
        for rel in baseset.relationships:
            if rel.arc.document.uri not in standard_mapped_uris:
                if rel.overriding_relationship is not None:
                    overriding_relationship = rel.overriding_relationship
                    source = xbrl.Error.Param(rel.arc.xlink_from, location=rel.from_locator, deflocation=rel.source)
                    target = xbrl.Error.Param(rel.arc.xlink_to, location=rel.to_locator, deflocation=rel.target)
                    source2 = xbrl.Error.Param(overriding_relationship.arc.xlink_from, location=overriding_relationship.from_locator, deflocation=overriding_relationship.source)
                    target2 = xbrl.Error.Param(overriding_relationship.arc.xlink_to, location=overriding_relationship.to_locator, deflocation=overriding_relationship.target)
                    error_log.report(xbrl.Error.create('[EFM.6.9.3] Relationship {arc} from {source} to {target} is ineffectual because it has been overridden by the relationship {arc2} from {source2} to {target2}.',
                                                       location=rel.arc, arc=rel.arc, source=source, target=target, arc2=overriding_relationship.arc, source2=source2, target2=target2))
                else:
                    overridden_relationships = list(rel.overridden_relationships)
                    if rel.is_prohibited():
                        if not len(overridden_relationships):
                            source = xbrl.Error.Param(rel.arc.xlink_from, location=rel.from_locator, deflocation=rel.source)
                            target = xbrl.Error.Param(rel.arc.xlink_to, location=rel.to_locator, deflocation=rel.target)
                            error_log.report(xbrl.Error.create('[EFM.6.9.3] Prohibiting relationship {arc} from {source} to {target} is ineffectual because it does not override a relationship in a standard taxonomy.', location=rel.arc, arc=rel.arc, source=source, target=target))
                    else:
                        for overridden_rel in overridden_relationships:
                            if not overridden_rel.is_prohibited():
                                source = xbrl.Error.Param(rel.arc.xlink_from, location=rel.from_locator, deflocation=rel.source)
                                target = xbrl.Error.Param(rel.arc.xlink_to, location=rel.to_locator, deflocation=rel.target)
                                source2 = xbrl.Error.Param(overridden_rel.arc.xlink_from, location=overridden_rel.from_locator, deflocation=overridden_rel.source)
                                target2 = xbrl.Error.Param(overridden_rel.arc.xlink_to, location=overridden_rel.to_locator, deflocation=overridden_rel.target)
                                error_log.report(
                                    xbrl.Error.create(
                                        '[EFM.6.9.3] Relationship {arc} from {source} to {target} is ineffectual because it overrides the unprohibited relationship {arc2} from {source2} to {target2}.',
                                        location=rel.arc,
                                        arc=rel.arc,
                                        source=source,
                                        target=target,
                                        arc2=overridden_rel.arc,
                                        source2=source2,
                                        target2=target2))

        if baseset.arcrole == 'http://www.xbrl.org/2003/arcrole/summation-item':
            # 6.14.4 There must be no directed cycles in effective relationships having arc role http://www.xbrl.org/2003/role/summation-item.
            cycle = detect_directed_cycles(baseset.network_of_relationships())
            if len(cycle):
                hints = [xbrl.Error.create('Relationship {arc} from {source} to {target}', severity=xml.ErrorSeverity.INFO, arc=rel.arc, source=rel.source, target=rel.target) for rel in cycle]
                error_log.report(xbrl.Error.create('[EFM.6.14.4] There must be no directed cycles in effective relationships having arc role http://www.xbrl.org/2003/role/summation-item.', location=cycle[0].arc, children=hints))

        elif baseset.arcrole == 'http://xbrl.org/int/dim/arcrole/all':
            network = baseset.network_of_relationships()

            for rel in network.relationships:
                for rel2 in drs.consecutive_relationships(rel):
                    positive_axes.add((rel.role, rel2.target))

            # 6.16.5 The DTS of an instance must contain in each base set, for each source element, at most one effective relationship with an xlink:arcrole attribute equal to 'http://xbrl.org/int/dim/arcrole/all'.
            for root in network.roots:
                rels = list(network.relationships_from(root))
                if len(rels) > 1:
                    error_log.report(xbrl.Error.create('[EFM.6.16.5] Concept {source} has more than one http://xbrl.org/int/dim/arcrole/all relationships {arc2} and {arc}.', location=rels[1].arc, arc=rels[1].arc, arc2=rels[0].arc, source=root))

        elif baseset.arcrole == 'http://xbrl.org/int/dim/arcrole/notAll':
            network = baseset.network_of_relationships()

            for rel in network.relationships:
                for rel2 in drs.consecutive_relationships(rel):
                    negative_axis_rels.append((rel, rel2))

            for rel in network.relationships:
                # 6.16.6 An effective relationship with an xlink:arcrole attribute equal to 'http://xbrl.org/int/dim/arcrole/notAll' must have an xbrldt:closed attribute equal to 'false'.
                if rel.arc.document.uri not in standard_mapped_uris and rel.closed:
                    closed = rel.arc.element.find_attribute(('closed', xbrldt_namespace))
                    if not closed:
                        closed = xbrl.Error.Param('xbrldt:closed', location=rel.arc)
                    error_log.report(xbrl.Error.create('[EFM.6.16.6] http://xbrl.org/int/dim/arcrole/notAll relationship {arc} must have {closed} attribute equal to false.', location='closed:value', arc=rel.arc, closed=closed))

                # 6.16.8 The target of an effective relationship with an xlink:arcrole
                # attribute equal to 'http://xbrl.org/int/dim/arcrole/notAll' must not be
                # the target of an effective arc with an xlink:arcrole attribute equal to
                # 'http://xbrl.org/int/dim/arcrole/all' in link:definitionLink elements
                # having equal values of xlink:role.
                all_network = instance.dts.definition_base_set(baseset.role, 'http://xbrl.org/int/dim/arcrole/all').network_of_relationships()
                all_relationships = list(all_network.relationships_to(rel.target))
                if len(all_relationships):
                    error_log.report(
                        xbrl.Error.create(
                            '[EFM.6.16.8] Hypercube {hypercube} must not be a target of all relationship {all} and notAll relationship {notAll} within the same link role {role}.',
                            location=rel.arc,
                            notAll=rel.arc,
                            all=all_relationships[0].arc,
                            hypercube=rel.target,
                            role=baseset.role))

        elif baseset.arcrole == 'http://xbrl.org/int/dim/arcrole/dimension-domain':
            network = baseset.network_of_relationships()

            # 6.16.3 The target of an effective relationship with an xlink:arcrole attribute equal to 'http://xbrl.org/int/dim/arcrole/dimension-domain' or 'http://xbrl.org/int/dim/arcrole/dimension-default' must be a domain member.
            for rel in network.relationships:
                if rel.arc.document.uri not in standard_mapped_uris and rel.target.type_definition not in domainItemTypes:
                    error_log.report(xbrl.Error.create('[EFM.6.16.3] Target {target} of dimension-domain relationship {arc} must be a domain member.', location=rel.arc, arc=rel.arc, target=rel.target))

            # 6.16.4 The xlink:arcrole attributes 'http://xbrl.org/int/dim/arcrole/domain-member' and 'http://xbrl.org/int/dim/arcrole/dimension-domain' must have no undirected cycles in any Directed Relationship Set as defined in XBRL Dimensions 1.0.
            for dim in network.roots:
                visited = set()
                for rel in network.relationships_from(dim):
                    cycle_member = check_undirected_drs_cycles(drs, rel, visited)
                    if cycle_member is not None:
                        error_log.report(xbrl.Error.create('[EFM.6.16.4] DRS has an undirected cycle in domain member network with role {role} between {dim} and {member} starting from relationship {arc}.', location=rel.arc, dim=dim, member=cycle_member, arc=rel.arc, role=xbrl.Error.Param(rel.role)))
                        break

        elif baseset.arcrole == 'http://xbrl.org/int/dim/arcrole/dimension-default':
            # 6.16.3 The target of an effective relationship with an xlink:arcrole attribute equal to 'http://xbrl.org/int/dim/arcrole/dimension-domain' or 'http://xbrl.org/int/dim/arcrole/dimension-default' must be a domain member.
            for rel in baseset.network_of_relationships().relationships:
                if rel.arc.document.uri not in standard_mapped_uris and rel.target.type_definition not in domainItemTypes:
                    error_log.report(xbrl.Error.create('[EFM.6.16.3] Target {target} of dimension-default relationship {arc} must be a domain member.', location=rel.arc, arc=rel.arc, target=rel.target))

        elif baseset.arcrole == 'http://xbrl.org/int/dim/arcrole/domain-member':
            network = baseset.network_of_relationships()

            primary_items = set(drs.primary_items(baseset.role))
            # 6.16.4 The xlink:arcrole attributes 'http://xbrl.org/int/dim/arcrole/domain-member' and 'http://xbrl.org/int/dim/arcrole/dimension-domain' must have no undirected cycles in any Directed Relationship Set as defined in XBRL Dimensions 1.0.
            for item in network.roots:
                if item in primary_items and item.type_definition not in domainItemTypes:
                    visited = set()
                    for rel in network.relationships_from(item):
                        cycle_item = check_undirected_drs_cycles(drs, rel, visited)
                        if cycle_item:
                            error_log.report(xbrl.Error.create('[EFM.6.16.4] DRS has an undirected cycle in domain member network with role {role} between {primary_item} and {item} starting from relationship {arc}.', location=rel.arc, primary_item=item, item=cycle_item, arc=rel.arc, role=xbrl.Error.Param(rel.role)))
                            break

        if baseset.extended_link_qname == qname_definitionLink:
            # 6.16.9 If the value of attribute xbrldt:targetRole on an effective definition relationship is not empty, then that relationship must have at least one effective consecutive relationship (as defined by the XBRL Dimensions specification).
            network = baseset.network_of_relationships()
            for rel in network.relationships:
                if rel.arc.target_role and not len(list(drs.consecutive_relationships(rel))):
                    target_role_attr = rel.arc.element.find_attribute(('targetRole', xbrldt_namespace))
                    error_log.report(xbrl.Error.create('[EFM.6.16.9] Relationship {arc} has non-empty {targetRole} attribute but no consecutive relationships.', location='targetRole:value', arc=rel.arc, targetRole=target_role_attr))

    # 6.16.7 An axis of a negative table must appear in a positive table in a definitionLink having an equal value of xlink:role.
    for (rel, rel2) in negative_axis_rels:
        if not (rel.role, rel2.target) in positive_axes:
            error_log.report(xbrl.Error.create('[EFM.6.16.7] Axis {axis} of negative table {table} must appear in a positive table.', location=rel.arc, table=rel.target, axis=rel2.target))

    for doc in instance.dts.documents:
        if not is_extension_document(instance_uri, doc):
            continue

        # 6.3.6 The URI content of the xlink:href attribute, the xsi:schemaLocation attribute and the schemaLocation attribute must be relative and contain no forward slashes, or a recognized external location of a standard taxonomy schema file, or a '#' followed by a shorthand xpointer.
        for schema_location in doc.schema_location_attributes:
            if schema_location.local_name == 'schemaLocation':
                for uri in schema_location.normalized_value.split()[1::2]:
                    if not re_href.fullmatch(uri):
                        error_log.report(xbrl.Error.create('[EFM.6.3.6] {uri} in attribute {schemaLocation} on {elem} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.',
                                                           location='uri', uri=xml.Error.Param(uri, location=schema_location), schemaLocation=schema_location, elem=doc.document_element))

        if isinstance(doc, xbrl.taxonomy.LinkbaseDocument):
            linkbase = doc.linkbase

            # 6.3.6 The URI content of the xlink:href attribute, the xsi:schemaLocation attribute and the schemaLocation attribute must be relative and contain no forward slashes, or a recognized external location of a standard taxonomy schema file, or a '#' followed by a shorthand xpointer.
            for roleref in linkbase.role_refs:
                if not re_href.fullmatch(roleref.xlink_href):
                    href = roleref.element.find_attribute(('href', xlink_namespace))
                    error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {roleRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, roleRef=roleref))
            for arcroleref in linkbase.arcrole_refs:
                if not re_href.fullmatch(arcroleref.xlink_href):
                    href = arcroleref.element.find_attribute(('href', xlink_namespace))
                    error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on {arcroleRef} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, arcroleRef=arcroleref))
            for link in linkbase.extended_links:
                for loc in link.locators:
                    if not re_href.fullmatch(loc.xlink_href):
                        href = loc.element.find_attribute(('href', xlink_namespace))
                        error_log.report(xbrl.Error.create('[EFM.6.3.6] {href:value} in attribute {href} on locator {loc} must be a standard schema or a relative filename optionally followed by a shorthard xpointer.', location='href:value', href=href, loc=loc))

            for arcrole_ref in linkbase.arcrole_refs:
                # 6.9.6 The text preceding a sharp sign '#' in an xlink:href attribute of link:arcroleRef must be a standard taxonomy.
                if arcrole_ref.xlink_href.partition('#')[0] not in standard_uris:
                    error_log.report(xbrl.Error.create('[EFM.6.9.6] Arcrole URI {arcroleURI:value} on {arcroleRef} must be defined in the XBRL 2.1 specification or a standard taxonomy schema.',
                                                       location='arcroleURI:value', arcroleRef=arcrole_ref.element, arcroleURI=arcrole_ref.element.find_attribute('arcroleURI')))

            extended_link_qname = None
            for link in linkbase.extended_links:

                # 6.9.7 All extended link elements in a single linkbase must have the same namespace and local name.
                if extended_link_qname is None:
                    extended_link_qname = link.qname
                elif link.qname != extended_link_qname:
                    link2 = next(iter(linkbase.extended_links))
                    error_log.report(xbrl.Error.create('[EFM.6.9.7] Linkbase {linkbase} must not contain different extended links {link2} and {link}.', location=link.element, link=link.element, link2=link2.element, linkbase=linkbase.element))
                # 6.9.4 The xlink:role attribute of an element with a type='extended' attribute or a type='resource' attribute must be present and must not be empty.
                if not link.xlink_role:
                    error_log.report(xbrl.Error.create('[EFM.6.9.4] Extended link {link} must have a non-empty {role} attribute.', location=link.element, link=link.element, role=xml.QName('role', xlink_namespace, 'xlink')))

                for resource in link.resources:
                    # 6.9.4 The xlink:role attribute of an element with a type='extended' attribute or a type='resource' attribute must be present and must not be empty.
                    if not resource.xlink_role:
                        error_log.report(xbrl.Error.create('[EFM.6.9.4] Resource {resource} must have a non-empty {role} attribute.', location=resource.element, resource=resource.element, role=xml.QName('role', xlink_namespace, 'xlink')))
                    elif resource.xlink_role not in standard_roles:
                        # 6.9.5 The xlink:role attribute of an element with an xlink:type attribute of 'resource' must be present and must be defined in XBRL 2.1 or a standard taxonomy.
                        error_log.report(xbrl.Error.create('[EFM.6.9.5] Role {role:value} on resource {resource} must be defined in the XBRL 2.1 specification or a standard taxonomy.', location='role:value', role=resource.element.find_attribute(('role', xlink_namespace)), resource=resource.element))

                for arc in link.arcs:
                    # 6.9.9 The value of the priority attribute must be strictly less than 10.
                    if arc.priority >= 10:
                        priority_attr = arc.element.find_attribute('priority')
                        error_log.report(xbrl.Error.create('[EFM.6.9.9] Priority {priority:value} on arc {arc} must be less than 10.', location='priority:value', priority=priority_attr, arc=arc))

                if link.qname == qname_labelLink:
                    for arc in link.arcs:
                        for rel in arc.relationships:
                            # 6.10.5 A label linkbase must not have a definition for an element defined in a standard taxonomy.
                            if rel.source.document.uri in standard_mapped_uris and rel.target.xlink_role == 'http://www.xbrl.org/2003/role/documentation':
                                if rel.is_prohibited():
                                    error_log.report(xbrl.Error.create('[EFM.6.10.5] Label {label} must not be removed from standard concept {concept}.', location=arc, label=resource, concept=rel.source))
                                else:
                                    error_log.report(xbrl.Error.create('[EFM.6.10.5] Label {label} must not be added to standard concept {concept}.', location=arc, label=resource, concept=rel.source))

                    for resource in link.resources:
                        text = []
                        contains_markup = False
                        for child in resource.element.children:
                            if isinstance(child, xml.CharDataInformationItem):
                                text.append(child.value)
                            else:
                                contains_markup = True
                        text = ''.join(text)

                        # 6.10.6 The ASCII text of link:label must be a string of fewer than 511 characters with no consecutive XML whitespace characters and no occurrences of '<' unless its xlink:role attribute is 'http://www.xbrl.org/2003/label/documentation'.
                        if resource.xlink_role != 'http://www.xbrl.org/2003/role/documentation':
                            if contains_markup or '<' in text:
                                error_log.report(xbrl.Error.create('[EFM.6.10.6] Non-documentation label {label} must not contain any \'<\' characters.', location='label:value', label=resource))
                            if len(text) >= 511:
                                error_log.report(xbrl.Error.create('[EFM.6.10.6] Non-documentation label {label} must contain fewer than 511 characters.', location='label:value', label=resource))
                            if re_consecutive_xml_whitespace.search(text):
                                error_log.report(xbrl.Error.create('[EFM.6.10.6] Non-documentation label {label} must not contain consecutive XML whitespace characters.', location='label:value', label=resource))

                        # 6.10.8 The text of link:label must not have leading or trailing XML whitespace.
                        if len(text) and (text[0] in ' \t\n\r' or text[-1] in ' \t\n\r'):
                            error_log.report(xbrl.Error.create('[EFM.6.10.8] Label {label} must not have leading or trailing XML whitespace characters.', location='label:value', label=resource))

                if link.qname == qname_referenceLink:
                    for arc in link.arcs:
                        for rel in arc.relationships:
                            # 6.18.2 A company extension reference linkbase must not add, remove, or change references for any element declared in a standard taxonomy schema.
                            if rel.source.document.uri in standard_mapped_uris:
                                if rel.is_prohibited():
                                    error_log.report(xbrl.Error.create('[EFM.6.18.2] Reference {ref} must not be removed for standard concept {concept} by prohibiting relationship {rel}.', location='rel', rel=rel.arc, ref=resource.element, concept=rel.source))
                                else:
                                    error_log.report(xbrl.Error.create('[EFM.6.18.2] Reference {ref} must not be added to standard concept {concept} by relationship {rel}.', location='rel', rel=rel.arc, ref=resource.element, concept=rel.source))

                elif link.qname == qname_presentationLink:
                    for arc in link.arcs:
                        # 6.12.1 The link:presentationArc element requires an order attribute.
                        order = arc.element.find_attribute('order')
                        if order is None or not order.specified:
                            error_log.report(xbrl.Error.create('[EFM.6.12.1] Presentation arc {arc} must have an order attribute.', arc=arc))

                elif link.qname == qname_calculationLink:
                    for arc in link.arcs:
                        # 6.14.1 Element link:calculationArc requires an order attribute.
                        order = arc.element.find_attribute('order')
                        if order is None or not order.specified:
                            error_log.report(xbrl.Error.create('[EFM.6.14.1] Calculation arc {arc} must have an order attribute.', arc=arc))

                        # 6.14.2 Element link:calculationArc requires a weight attribute value equal to 1 or -1.
                        if abs(arc.weight) != 1:
                            error_log.report(xbrl.Error.create('[EFM.6.14.2] Calculation arc {arc} must have a weight attribute equal to 1 or -1.', location='weight:value', arc=arc, weight=arc.element.find_attribute('weight')))

                elif link.qname == qname_definitionLink:
                    for arc in link.arcs:
                        # 6.16.1 Element link:definitionArc requires an order attribute.
                        order = arc.element.find_attribute('order')
                        if order is None or not order.specified:
                            error_log.report(xbrl.Error.create('[EFM.6.16.1] Definition arc {arc} must have an order attribute.', arc=arc))

    presentation_networks = []
    for presentation_role in instance.dts.presentation_link_roles():
        baseset = instance.dts.presentation_base_set(presentation_role)
        network = baseset.network_of_relationships()
        presentation_networks.append(network)

        # 6.12.2 All effective presentation relationships in the same base set with the same source element must have distinct values of the order attribute.
        source_to_relationship = {}
        for rel in network.relationships:

            if (rel.source, rel.order) in source_to_relationship:
                rel2 = source_to_relationship[(rel.source, rel.order)]
                if rel.arc.document.uri not in standard_mapped_uris or rel2.arc.document.uri not in standard_mapped_uris:
                    error_log.report(xbrl.Error.create('[EFM.6.12.2] Presentation arcs {arc} and {arc2} within the same base set starting from same source element {source} must have distinct values of the order attribute.', arc=rel.arc, arc2=rel2.arc, source=rel.source))
            else:
                source_to_relationship[(rel.source, rel.order)] = rel

            # 6.12.7 An effective presentation relationship whose target is an xsd:element with an xbrli:periodType attribute equal to 'duration' should not have a preferredLabel attribute value that is a role for elements with xbrli:periodType attribute equal to 'instant'.
            if rel.preferred_label:
                if rel.target.period_type == xbrl.taxonomy.PeriodType.DURATION and re_period_start_or_end.search(rel.preferred_label):
                    error_log.report(xbrl.Error.create('[EFM.6.12.7] Presentation arc {arc} with a duration target concept {concept} must not have a {preferredLabel} attribute with role {role}.',
                                                       severity=xml.ErrorSeverity.WARNING, arc=rel.arc, concept=rel.target, preferredLabel=rel.arc.element.find_attribute('preferredLabel'), role=rel.preferred_label))

        # 6.12.6 Each effective presentation relationship base set should have only one root element.
        if len(list(network.roots)) > 1:
            child_errors = []
            for root in network.roots:
                for rel in network.relationships_from(root):
                    child_errors.append(xbrl.Error.create('Concept {concept} is the source of presentation arc {arc}.', location=rel.arc, concept=root, arc=rel.arc))
            error_log.report(xbrl.Error.create('[EFM.6.12.6] Presentation relationship base set with linkrole {linkrole} contains multiple root elements.', severity=xml.ErrorSeverity.WARNING, location=rel.arc, linkrole=baseset.role, children=child_errors))

        # 6.12.8 Each axis element in an effective presentation relationship base set should be the source of at least one effective presentation relationship in the same base set whose target is a domainItemType element.
        axes = set()
        for rel in network.relationships:
            if isinstance(rel.source, xbrl.xdt.Dimension) and rel.source.name.endswith('Axis'):
                axes.add(rel.source)
            if isinstance(rel.target, xbrl.xdt.Dimension) and rel.target.name.endswith('Axis'):
                axes.add(rel.target)
        for axis in axes:
            domain_members = [rel.target for rel in network.relationships_from(axis) if rel.target.type_definition in domainItemTypes]
            if len(domain_members) == 0:
                error_log.report(xbrl.Error.create('[EFM.6.12.8] Axis {axis} in presentation relationship base set {linkrole} must be the source of at least one relationship to a domain member item.', severity=xml.ErrorSeverity.WARNING, axis=axis, linkrole=baseset.role))

        # 6.12.9 A base set having one effective presentation relationship whose target has the same local name as the unitRef attribute value of a fact of a source or target element in the same base set should provide an ordering for all such unitRef attribute values.
        unitRefs = set()
        localNames = set()
        for rel in network.relationships:
            if isinstance(rel.source, xbrl.taxonomy.Item) and not rel.source.abstract and rel.source.is_numeric():
                for fact in instance.facts.filter(rel.source):
                    unitRefs.add(fact.unitRef)
            localNames.add(rel.source.name)
            if isinstance(rel.target, xbrl.taxonomy.Item) and not rel.target.abstract and rel.target.is_numeric():
                for fact in instance.facts.filter(rel.target):
                    unitRefs.add(fact.unitRef)
            localNames.add(rel.target.name)
        if unitRefs and not unitRefs.isdisjoint(localNames):
            unitRefs -= localNames
            for unitRef in unitRefs:
                unit = instance.unit(unitRef)
                error_log.report(xbrl.Error.create('[EFM.6.12.9] Presentation relationship base set with linkrole {linkrole} should contain an ordering for unit {unit}.', severity=xml.ErrorSeverity.WARNING, location=unit, linkrole=baseset.role, unit=unit))

    for calculation_role in instance.dts.calculation_link_roles():
        network = instance.dts.calculation_base_set(calculation_role).network_of_relationships()

        for rel in network.relationships:
            # 6.14.3 The source and target of an effective calculation relationship must have equal values of the xbrli:periodType attribute.
            if rel.source.period_type != rel.target.period_type:
                error_log.report(xbrl.Error.create('[EFM.6.14.3] The source {source} and target {target} of relationship {arc} must have equal values of xbrli:periodType attribute.', location=rel.arc, arc=rel.arc, source=rel.source, target=rel.target))

            # 6.14.5 If an instance contains non-empty facts for the source and target
            # of an effective calculation relationship, then at least one effective
            # presentation relationship that the source and target appear in (because
            # of 6.12.3) must be either (a) a relationship with each other or (b) two
            # relationships with any other elements that share a single extended link
            # role.
            if used_concepts.get(rel.source, False) and used_concepts.get(rel.target, False) and not has_concepts_in_presentation_linkbase(instance.dts, rel.source, rel.target):
                error_log.report(xbrl.Error.create('[EFM.6.14.5] The source {source} and target {target} of calculation relationship {arc} must also have effective presentation relationships with the same extended link role.', location=rel.arc, arc=rel.arc, source=rel.source, target=rel.target))

    for concept in used_concepts:
        labels = {}
        translated_roles = {}
        for label in concept.labels():
            if label.xml_lang != 'en-US':
                translated_roles[label.xlink_role] = label
            key = (label.xlink_role, label.xml_lang)
            # 6.10.2 An element used in a fact or xbrldi:explicitMember in an instance must have at most one label for any combination of the xlink:role attribute and the xml:lang attribute in the DTS of that instance.
            if key in labels:
                error_log.report(xbrl.Error.create('[EFM.6.10.2] Concept {concept} must not be linked to more than one label resource with the same role {role} and language {lang}.', location=concept, concept=concept, role=label.xlink_role, lang=label.xml_lang))
            labels[key] = label
        # 6.10.1 An element used in a fact or xbrldi:explicitMember in an instance must have an English standard label in the DTS of that instance.
        if ('http://www.xbrl.org/2003/role/label', 'en-US') not in labels:
            error_log.report(xbrl.Error.create('[EFM.6.10.1] Concept {concept} must be linked to a standard \'en-US\' label resource.', location=concept, concept=concept))
        # 6.10.3 If an element used in an instance is assigned a label in the DTS whose xml:lang attribute is not 'en-US', then the DTS must also contain a link:label for the same element and all other attributes with an xml:lang attribute equal to 'en-US'.
        for role, label in translated_roles.items():
            if (role, 'en-US') not in labels:
                lang = xbrl.Error.Param(label.xml_lang, location=label.element.find_attribute(('lang', xml_namespace)))
                role = xbrl.Error.Param(label.xlink_role, location=label.element.find_attribute(('role', xlink_namespace)))
                error_log.report(xbrl.Error.create('[EFM.6.10.3] Concept {concept} having label {label} with language {lang:value} and role {role:value} must be also linked to an \'en-US\' label resource with the same role.', location=concept, concept=concept, lang=lang, role=role, label=label))

        # 6.12.3 An element used in an instance must participate in at least one effective presentation relationship in the DTS of that instance.
        participates_in_presentation_relationship = False
        for network in presentation_networks:
            if len(list(network.relationships_from(concept))) or len(list(network.relationships_to(concept))):
                participates_in_presentation_relationship = True
                break
        if not participates_in_presentation_relationship:
            facts = instance.facts.filter(concept)
            if len(facts):
                error_log.report(xbrl.Error.create('[EFM.6.12.3] Concept {concept} reported as fact {fact} must participate in at least one effective presentation relationship.', location=concept, concept=concept, fact=facts[0]))
            else:
                try:
                    for context in instance.contexts:
                        if context.entity.segment:
                            for member in context.entity.segment.explicit_members:
                                if member.value == concept:
                                    raise StopIteration
                except StopIteration:
                    error_log.report(xbrl.Error.create('[EFM.6.12.3] Concept {concept} referred to by context {context} in {explicitMember} must participate in at least one effective presentation relationship.', location=concept, concept=concept, context=context, explicitMember=member))

        # 6.12.5 If an element used in an instance is the target in the instance DTS of more than one effective presentation relationship in a base set with the same source element, then the presentation relationships must have distinct values of the preferredLabel attribute.
        source_to_relationship = {}
        for rel in network.relationships_to(concept):
            if (rel.source, rel.preferred_label) in source_to_relationship:
                rel2 = source_to_relationship[(rel.source, rel.preferred_label)]
                error_log.report(xbrl.Error.create('[EFM.6.12.5] Presentation arcs {arc} and {arc2} in the same base set with the same source and target must have distinct values of the preferredLabel attribute.', arc=rel.arc, arc2=rel2.arc))
            else:
                source_to_relationship[(rel.source, rel.preferred_label)] = rel

    validate_labels(instance_uri, instance.dts, error_log)

    if params.get('enableDqcValidation', 'false') == 'true':
        dqc_validation.validate(instance, error_log, **params)

# Main entry point, will be called by RaptorXML after the DTS discovery from XBRL instance has finished

def check_for_UTR_concept(dts, standard_namespace2uris):

    for ns in standard_namespace2uris:
        if dts.resolve_concept(('UTR', ns)):
            return True
    return False

def on_xbrl_finished_dts(job, dts):
    if dts is not None:
        # 6.5.35 If element 'UTR' in a standard namespace is declared in the DTS
        # of an instance, then the value of each 'unitRef' attribute on each fact
        # of a type in that registry must refer to a unit declaration consistent
        # with the data type of that fact, where consistency is defined by that
        # registry.
        if job.script_params.get('forceUtrValidation', 'false') == 'true':
            bEnableUTR = True
        else:
            uri_edgar_taxonomies = job.script_params.get('edgar-taxonomies-url', urljoin('file:', pathname2url(os.path.join(os.path.dirname(__file__), 'edgartaxonomies.xml'))))

            edgar_version, standard_taxonomies = parse_edgar_taxonomies(uri_edgar_taxonomies, job.catalog, job.error_log)
            standard_namespace2uris = get_standard_namespace2uris(standard_taxonomies)

            bEnableUTR = check_for_UTR_concept(dts, standard_namespace2uris)
            
        job.options['utr'] = bEnableUTR

# Main entry point, will be called by RaptorXML after the XBRL instance validation job has finished


def on_xbrl_finished(job, instance):
    validate(job.input_filenames[0], instance, job.error_log, catalog=job.catalog, **job.script_params)


# 5.2.5 Inline XBRL Documents

allowed_inlinexbrl_html_tags = set([
    'a',
    'address',
    'b',
    'big',
    'blockquote',
    'body',
    'br',
    'caption',
    'center',
    'code',
    'dfn',
    'i',
    'div',
    'dl',
    'dt',
    'em',
    'h1',
    'h2',
    'h3',
    'h4',
    'h5',
    'h6',
    'head',
    'hr',
    'html',
    'i',
    'img',
    'kbd',
    'li',
    'meta',
    'ol',
    'p',
    'pre',
    'samp',
    'small',
    'span',
    'strong',
    'sub',
    'sup',
    'table',
    'tbody',
    'td',
    'tfoot',
    'th',
    'thead',
    'title',
    'tr',
    'tt',
    'ul',
    'var'
])
allowed_inlinexbrl_html_attributes = set([
    xml.QName('align'),
    xml.QName('alink'),
    xml.QName('alt'),
    xml.QName('bgcolor'),
    xml.QName('border'),
    xml.QName('cellpadding'),
    xml.QName('cellspacing'),
    xml.QName('class'),
    xml.QName('clear'),
    xml.QName('color'),
    xml.QName('colspan'),
    xml.QName('compact'),
    xml.QName('content'),
    xml.QName('dir'),
    xml.QName('height'),
    xml.QName('href'),
    xml.QName('http-equiv'),  # Used in the efm testsuite but not mentioned in the actual EDGAR filer manual!
    xml.QName('id'),
    xml.QName('lang'),
    xml.QName('link'),
    xml.QName('longdesc'),  # Used in the efm testsuite but not mentioned in the actual EDGAR filer manual!
    xml.QName('name'),
    xml.QName('noshade'),
    xml.QName('nowrap'),
    xml.QName('prompt'),
    xml.QName('rel'),
    xml.QName('rev'),
    xml.QName('rowspan'),
    xml.QName('size'),
    xml.QName('src'),
    xml.QName('start'),
    xml.QName('style'),
    xml.QName('summary'),  # Used in the efm testsuite but not mentioned in the actual EDGAR filer manual!
    xml.QName('text'),
    xml.QName('title'),
    xml.QName('type'),
    xml.QName('valign'),
    xml.QName('vlink'),
    xml.QName('width'),
    xml.QName('lang', xml_namespace),
    xml.QName('schemaLocation', xsi_namespace),
    xml.QName('noNamespaceSchemaLocation', xsi_namespace)
])

# list of allowed prefixes for some standard namespaces
allowed_namespace_prefixes = {
    xhtml_namespace: '',
    ix_namespace: 'ix',
    ixt_namespace: 'ixt',
    ixtsec_namespace: 'ixt-sec'
}

# returns the value of the -sec-ix-hidden property, or None if absent.


def get_sec_ix_hidden(value):
    for part in value.split(';'):
        key_value = part.split(':')
        if len(key_value) == 2:
            if key_value[0].strip() == "-sec-ix-hidden":
                return key_value[1].strip()
    return None


# check namespace bindings of standard namespaces
def check_ixbrl_namespaces(elem, error_log):
    for nsattr in elem.namespace_attributes:
        namespace = nsattr.normalized_value
        if namespace in allowed_namespace_prefixes:
            prefix = '' if nsattr.prefix is None else nsattr.local_name
            recommended_prefix = allowed_namespace_prefixes[namespace]
            if prefix != recommended_prefix:
                # 5.2.5 standard namespace prefixes
                error_log.report(xbrl.Error.create('[EFM.5.2.5] At element {elem} the prefix {prefix} of namespace declaration {namespace} must be replaced by {recommended_prefix}.', elem=elem, prefix=prefix, namespace=namespace, recommended_prefix=recommended_prefix))


def check_valid_ixbrl(elem, catalog, error_log, ix_hidden_data, table=None):
    if elem.find_attribute(xml.QName('schemaLocation', xsi_namespace)):
        # 5.2.5.13 Other Inline XBRL restrictions
        # Attribute xsi:schemaLocation should not be used on an Inline XBRL document.
        error_log.report(xbrl.Error.create('[EFM.5.2.5.13] Attribute {schemaLocation} should not be used.', severity=xml.ErrorSeverity.WARNING, schemaLocation=elem.find_attribute(xml.QName('schemaLocation', xsi_namespace))))

    if elem.namespace_name == ix_namespace:
        if elem.local_name in ('tuple', 'fraction'):
            # 5.2.5.11 Inline XBRL 1.1 features that are not supported by EDGAR
            # The ix:tuple element is not allowed.
            # The ix:fraction element is not allowed.
            error_log.report(xbrl.Error.create('[EFM.5.2.5.11] Inline XBRL element {elem} is not allowed.', elem=elem))

        elif elem.local_name == 'header':
            style = elem.parent.find_attribute('style')
            if elem.parent.qname != xml.QName('div', xhtml_namespace) or style is None or not re_display_none.fullmatch(style.normalized_value):
                # 5.2.5.13 Other Inline XBRL restrictions
                # Element ix:heading should appear as the child of a <div> element with style attribute display:none.
                error_log.report(xbrl.Error.create('[EFM.5.2.5.13] Inline XBRL element {elem} must be a child of a <div> element with style attribute display:none.', severity=xml.ErrorSeverity.WARNING, elem=elem))

        id = None
        for attr in elem.attributes:
            if attr.qname == xml.QName('format'):
                if attr.schema_actual_value is not None and attr.schema_actual_value.namespace_name not in ('http://www.xbrl.org/inlineXBRL/transformation/2015-02-26', 'http://xbrl.sec.gov/inlineXBRL/transformation/2015-08-31', 'http://www.sec.gov/inlineXBRL/transformation/2015-08-31'):
                    # 5.2.5.12 Inline XBRL Transformation Registries supported by EDGAR
                    error_log.report(xbrl.Error.create('[EFM.5.2.5.12] Inline XBRL Transformation Registry {url} is not supported.', location='attr:value', attr=attr, url=attr.schema_actual_value.namespace_name))
            elif attr.qname in (xml.QName('target'), xml.QName('base', xml_namespace)):
                # 5.2.5.11 Inline XBRL 1.1 features that are not supported by EDGAR
                # The target attribute is not allowed on any Inline XBRL element.
                # The xml:base attribute is not allowed on any Inline XBRL element.
                error_log.report(xbrl.Error.create('[EFM.5.2.5.11] Attribute {attr} is not allowed on any Inline XBRL elements.', attr=attr))
            elif attr.qname == xml.QName('id'):
                id = attr.normalized_value

        if elem.local_name in ('fraction', 'nonFraction', 'nonNumeric') and elem.parent.namespace_name == ix_namespace and elem.parent.local_name == 'hidden':
            ix_hidden_data["facts"].setdefault(id, []).append(elem)

    elif elem.namespace_name == xhtml_namespace:
        if elem.local_name not in allowed_inlinexbrl_html_tags:
            # 5.2.5.6 HTML tags that are not allowed in Inline XBRL Documents
            error_log.report(xbrl.Error.create('[EFM.5.2.5.6] HTML element {elem} is not allowed in Inline XBRL documents', elem=elem))

        for attr in elem.attributes:
            if attr.specified and attr.qname not in allowed_inlinexbrl_html_attributes:
                # 5.2.5.9 HTML attributes allowed in Inline XBRL Documents
                error_log.report(xbrl.Error.create('[EFM.5.2.5.9] HTML attribute {attr} is not allowed in Inline XBRL documents', attr=attr))
            if attr.local_name == "style" and attr.namespace_name == "":
                sec_ix_hidden_id = get_sec_ix_hidden(attr.normalized_value)
                if sec_ix_hidden_id is not None:
                    ix_hidden_data["refs"][elem] = sec_ix_hidden_id

        if elem.local_name == 'a':
            href = elem.find_attribute('href')
            if href:
                href_url = urlparse(href.normalized_value)
                if href_url.scheme != '' and not re_html_href.fullmatch(href.normalized_value):
                    # 5.2.5.10 HTML attribute values that are not allowed in Inline XBRL Documents
                    # Attribute href (on the <a> tag) may only reference other HTML, ASCII and
                    # Inline XBRL documents that are local or are located on the SEC web site
                    # as attachments to previously accepted submissions. This precludes active
                    # content such as javascript from appearing in the href attribute.
                    error_log.report(xbrl.Error.create('[EFM.5.2.5.10] Reference to {href:value} is not allowed in attribute {href} in element {a}.', location='href:value', href=href, a=elem))
            else:
                parent = elem.parent
                while isinstance(parent, xml.ElementInformationItem):
                    if parent.namespace_name == xhtml_namespace and parent.local_name not in ('html', 'body', 'div'):
                        # 5.2.5.8 Restrictions on HTML bookmark positions
                        error_log.report(xbrl.Error.create('[EFM.5.2.5.8] HTML bookmark {elem} must not have ancestor {parent}.', severity=xml.ErrorSeverity.WARNING, location=elem, elem=elem, parent=parent))
                        break
                    parent = parent.parent

        elif elem.local_name == 'img':
            src = elem.find_attribute('src')
            if src and not re_html_src.fullmatch(src.normalized_value):
                # 5.2.5.10 HTML attribute values that are not allowed in Inline XBRL Documents
                # Attribute src on the <img> tag may only locally reference jpeg and gif graphics.
                error_log.report(xbrl.Error.create('[EFM.5.2.5.10] Reference to {src:value} is not allowed in attribute {src} in element {img}.', location='src:value', src=src, img=elem))
            else:
                try:
                    imageuri = urljoin(elem.base_uri, src.normalized_value)
                    if imghdr.what(imageuri, altova.open(imageuri, catalog=catalog, mode='rb').read()) not in ('gif', 'jpeg'):
                        # 5.2.5.10 HTML attribute values that are not allowed in Inline XBRL Documents
                        # Attribute src on the <img> tag may only locally reference jpeg and gif graphics.
                        error_log.report(xbrl.Error.create('[EFM.5.2.5.10] Image {src:value} referenced in attribute {src} in element {img} is not a valid GIF or JPEG image.', location='src:value', src=src, img=elem))
                except OSError:
                    # 5.2.5.10 HTML attribute values that are not allowed in Inline XBRL Documents
                    # Attribute src on the <img> tag may only locally reference jpeg and gif graphics.
                    error_log.report(xbrl.Error.create('[EFM.5.2.5.10] Image {src:value} referenced in attribute {src} in element {img} cannot be opened.', location='src:value', src=src, img=elem))
        elif elem.local_name == 'table':
            if table is not None:
                # 5.2.5.7 Nested HTML table elements are not allowed
                error_log.report(xbrl.Error.create('[EFM.5.2.5.7] Element {table} cannot be nested with another table element {table2}.', location='table', table=elem, table2=table))
            else:
                table = elem

    elif elem.namespace_name == link_namespace:
        if elem.local_name == "schemaRef":
            href_attr = elem.find_attribute(xml.QName("href", xlink_namespace))
            if href_attr is not None:
                ix_hidden_data["schemaRef"] = urljoin(elem.base_uri, href_attr.normalized_value)

    for child in elem.element_children():
        check_valid_ixbrl(child, catalog, error_log, ix_hidden_data, table)


# The XML Schema primitive types not eligible for transformation are anyURI, base64Binary, hexBinary, NOTATION, QName, and time.
# XML derived types token and language are not eligible for transformation. All other primitive and derived types are eligible for transformation.
def is_eligible_for_transformation(ix_fact, dts):
    name_attr = ix_fact.find_attribute('name')
    if name_attr is not None and isinstance(name_attr.schema_actual_value, xsd.QName):
        concept = dts.resolve_concept(xml.QName(name_attr.schema_actual_value.local_part, name_attr.schema_actual_value.namespace_name))
        return (isinstance(concept, xbrl.taxonomy.Item)
                and not concept.is_derived_from(qname_xs_anyURI)
                and not concept.is_derived_from(qname_xs_base64Binary)
                and not concept.is_derived_from(qname_xs_hexBinary)
                and not concept.is_derived_from(qname_xs_NOTATION)
                and not concept.is_derived_from(qname_xs_QName)
                and not concept.is_derived_from(qname_xs_time)
                and not concept.is_derived_from(qname_xs_token)
                and not concept.is_derived_from(qname_xs_language))
    return False


def is_ix_dei_fact(ix_fact):
    name_attr = ix_fact.find_attribute('name')
    if name_attr is not None and isinstance(name_attr.schema_actual_value, xsd.QName):
        return re_dei.match(name_attr.schema_actual_value.namespace_name)
    return False


def is_ix_nil(ix_fact):
    nil_attr = ix_fact.find_attribute(xml.QName('nil', xsi_namespace))
    return nil_attr is not None and bool(nil_attr.schema_actual_value)


def check_efm_5_2_5_14(dts, error_log, ix_hidden_data):
    id_to_ref = {}
    for ref, id in ix_hidden_data["refs"].items():
        # 5.2.5.14 The value of an -sec-ix-hidden style property must resolve to the @id of a fact in ix:hidden.
        if not id in ix_hidden_data["facts"]:
            error_log.report(xbrl.Error.create("[EFM.5.2.5.14] Value {value} of -sec-ix-hidden property doesn't resolve to a hidden fact.", location='attr:value', value=id, attr=ref))
        else:
            # The @id of a fact in ix:hidden should not appear as the value of more than one -sec-ix-hidden style property.
            if id in id_to_ref:
                for fact in ix_hidden_data["facts"][id]:
                    error_log.report(xbrl.Error.create("[EFM.5.2.5.14] Id {id} of hidden fact {fact} is referenced from {ref1} and {ref2}.", severity=xml.ErrorSeverity.WARNING, location='ref2', id=id, fact=fact, ref1=id_to_ref[id], ref2=ref))
            else:
                id_to_ref[id] = ref

    for id, hidden_facts in ix_hidden_data["facts"].items():
        for fact in hidden_facts:
            # Facts in ix:hidden whose @name attributes resolve to an element in the "dei" namespace are "dei facts" that may always
            # appear in ix:hidden and may (but need not) be displayed using -sec-ix-hidden.
            if not is_ix_dei_fact(fact):
                if is_ix_nil(fact):
                    # Facts in ix:hidden that are not dei facts, with an @xsi:nil attribute of "true", should be displayed using -sec-ix-hidden.
                    # (Note that the inline xbrl transformation “ixt:nocontent" produces an non-nil fact, which differs from a nil fact).
                    if id not in id_to_ref:
                        error_log.report(xbrl.Error.create("[EFM.5.2.5.14] Hidden nil-fact {fact} should be displayed using -sec-ix-hidden.", severity=xml.ErrorSeverity.WARNING, location='fact', fact=fact))
                elif is_eligible_for_transformation(fact, dts):
                    # Facts with a @name attribute that resolves to an element whose XML value space is a subset of available transformation
                    # outputs are "eligible for transformation". A non-dei fact eligible for transformation should not be in ix:hidden.
                    error_log.report(xbrl.Error.create("[EFM.5.2.5.14] Non dei-fact {fact} is eligible for transformation and should therefore not be in ix:hidden.", severity=xml.ErrorSeverity.WARNING, location='fact', fact=fact))
                else:
                    # Facts in ix:hidden that are not dei facts, not having @xsi:nil value "true" and not eligible for transformation should be
                    # displayed using -sec-ix-hidden.
                    if id not in id_to_ref:
                        error_log.report(xbrl.Error.create("[EFM.5.2.5.14] Hidden non-nil fact {fact} is not eligible for transformation and should therefore be displayed using -sec-ix-hidden.", severity=xml.ErrorSeverity.WARNING, location='fact', fact=fact))


def validate_ixbrl(instance, error_log, catalog=xml.Catalog.root_catalog()):
    if instance is None:
        return

    # 5.2.5.1 The <DOCTYPE> declaration not supported
    if instance.dtd is not None:
        error_log.report(xbrl.Error.create('[EFM.5.2.5.1] Inline XBRL document {uri} must not contain a <DOCTYPE> declaration.', uri=instance.uri))

    # 5.2.5.3 Element <head> content
    head = instance.document_element.find_child_element(('head', xhtml_namespace))
    if head:
        bHasMeta = False
        for child in head.element_children():
            if child.local_name == 'meta' and child.namespace_name == xhtml_namespace:
                http_equiv = child.find_attribute('http-equiv')
                if http_equiv and http_equiv.normalized_value == 'Content-Type':
                    content = child.find_attribute('content')
                    if content and (content.normalized_value == 'text/html' or content.normalized_value.startswith('text/html;')):
                        bHasMeta = True
                        break
        if not bHasMeta:
            error_log.report(xbrl.Error.create('[EFM.5.2.5.3] Element {head} must contain a <meta http-equiv="Content-Type" content="text/html"> child element.', head=head))

    ix_hidden_data = {"facts": {}, "refs": {}, "schemaRef": None}

    # only check namespace bindings on document element, otherwise some testcases FAIL
    check_ixbrl_namespaces(instance.document_element, error_log)

    check_valid_ixbrl(instance.document_element, catalog, error_log, ix_hidden_data)
    if ix_hidden_data["schemaRef"] is not None:
        # no xbrl instance/dts in on_ixbrl_finished, so it must be loaded here.
        dts, xbrl_error_log = xbrl.taxonomy.DTS.create_from_url(ix_hidden_data["schemaRef"], catalog=catalog)
        if dts is not None:
            check_efm_5_2_5_14(dts, error_log, ix_hidden_data)  # check hidden element restrictions (ix:hidden), DTS needed

# Main entry point, will be called by RaptorXML after the Inline XBRL transformation is finished


def on_ixbrl_finished(job, document_set, target_documents):
    # 5.2.5.2 Inline XBRL validation
    if len(document_set) == 1:
        validate_ixbrl(document_set[0], job.error_log, job.catalog)
    else:
        # 5.2.5.11 Inline XBRL 1.1 features that are not supported by EDGAR
        # Inline XBRL Document Sets as defined by section 3.1 of the Inline XBRL 1.1 Specification can contain only one input document.
        error_log.report(xbrl.Error.create('[EFM.5.2.5.11] Inline XBRL Document Set must contain only one input document.'))
