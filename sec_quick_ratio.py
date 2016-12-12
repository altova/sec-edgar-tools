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
__copyright__ = "Copyright 2015, 2016 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# This script uses RaptorXML Python API v2 to demonstrate one way how to calculate financial ratios from quarterly and yearly SEC filings.
# This script uses RaptorXML Python API v2 to demonstrate how to report additional information calculated during validation in XMLSpy.
#
# Example invocation:
#   raptorxmlxbrl valxbrl --script=sec_quick_ratio.py nanonull.xbrl


import re
from altova import *

def concept_label(concept, label_role=None):
    if not label_role:
        label_role = xbrl.taxonomy.ROLE_LABEL
    # Find all labels matching the given criteria
    labels = list(concept.labels(label_role=label_role, lang='en'))
    if not labels:
        # If not labels are found fallback to concept QName
        return str(concept.qname)
    # Return text of first label found
    return labels[0].text

def find_namespaces(dts):
    # Determine dei and us-gaap namespaces (the namespaces will vary depending on the version of the US-GAAP taxonomy used)
    dei_ns = None
    gaap_ns = None
    re_dei = re.compile('^http://xbrl.us/dei/|^http://xbrl.sec.gov/dei/')
    re_gaap = re.compile('^http://[^/]+/us-gaap/')
    for taxonomy in dts.taxonomy_schemas:
        if re_dei.match(taxonomy.target_namespace):
            dei_ns = taxonomy.target_namespace
        elif re_gaap.match(taxonomy.target_namespace):
            gaap_ns = taxonomy.target_namespace
    return (dei_ns, gaap_ns)

def find_fact_value(instance, context, concepts):
    # Return the first reported value found for the given list of concepts
    for concept in concepts:
        facts = instance.facts.filter(concept, context)
        if facts:
            return (facts[0], facts[0].effective_numeric_value)
    return (None, 0)

def calc_quick_ratio(instance, error_log):
    # Determine dei and us-gaap namespaces (the namespaces will vary depending on the version of the US-GAAP taxonomy used)
    dei_ns, gaap_ns = find_namespaces(instance.dts)

    # Check if instance is an quarterly or yearly SEC filing
    documentTypes = instance.facts.filter(xml.QName('DocumentType', dei_ns))
    if not len(documentTypes) or documentTypes[0].normalized_value not in ('10-K','10-Q'):
        error_log.report(xbrl.Error.create('Quick ratio cannot be computed because instance does not appear to be a 10-K or 10-Q SEC filing.', severity = xml.ErrorSeverity.WARNING, location=instance))
        return

    # Prepare lists of QNames with different alias concept names for cash, securities, receivables and liabilities
    cash_concepts           = [xml.QName(name, gaap_ns) for name in ['Cash','CashAndCashEquivalentsAtCarryingValue','CashCashEquivalentsAndShortTermInvestments']]
    securities_concepts     = [xml.QName(name, gaap_ns) for name in ['MarketableSecuritiesCurrent','AvailableForSaleSecuritiesCurrent','ShortTermInvestments','OtherShortTermInvestments']]
    receivables_concepts    = [xml.QName(name, gaap_ns) for name in ['AccountsReceivableNetCurrent','ReceivablesNetCurrent']]
    liabilities_concepts    = [xml.QName(name, gaap_ns) for name in ['LiabilitiesCurrent']]

    for context in instance.contexts:
        if context.period.is_instant() and context.period.aspect_value.instant == documentTypes[0].period_aspect_value.end and not context.entity.segment:
            # For each 'required' context (for more information please consult the EDGAR Filer Manual at http://www.sec.gov/info/edgar/edmanuals.htm)

            # ... find a fact value for cash, securities, receivables and liabilities
            cash_fact, cash_val                 = find_fact_value(instance, context, cash_concepts)
            securities_fact, securities_val     = find_fact_value(instance, context, securities_concepts)
            receivables_fact, receivables_val   = find_fact_value(instance, context, receivables_concepts)
            liabilities_fact, liabilities_val   = find_fact_value(instance, context, liabilities_concepts)

            if liabilities_fact:
                # Calculate the quick ratio (http://en.wikipedia.org/wiki/Quick_ratio)
                quick_ratio = (cash_val + securities_val + receivables_val) / liabilities_val

                # Report the quick ratio together with the underlying values used in the computation. In XMLSpy clicking on the values will also navigate to the corresponding fact in the instance document.
                # An example is shown below:
                # Quick ratio for instant '2014-05-31' in context 'FI2014Q2' is 0.0853
                #   cash = 343000000
                #   securities = no reported value found
                #   receivables = 288000000
                #   liabilities = 7401000000

                # Report each contributing value on a separate sub-line
                child_lines = []
                for label, fact, val in [('cash', cash_fact, cash_val), ('securities', securities_fact, securities_val), ('receivables', receivables_fact, receivables_val), ('liabilities', liabilities_fact, liabilities_val)]:
                    if fact:
                        fact_param = xbrl.Error.Param(str(val), location=fact, deflocation=fact.concept, tooltip=concept_label(fact.concept), quotes=False)
                        child_lines.append(xbrl.Error.create('{label} = {fact:value}', severity=xml.ErrorSeverity.OTHER, label=xbrl.Error.Param(label, quotes=False), fact=fact_param))
                    else:
                        child_lines.append(xbrl.Error.create('{label} = no reported value found', severity=xml.ErrorSeverity.OTHER, label=xbrl.Error.Param(label, quotes=False)))

                # Report the actual quick ratio as the main line
                quick_ratio_param = xbrl.Error.Param('%.4f' % quick_ratio, quotes=False)
                main_line = xbrl.Error.create('Quick ratio for instant {instant:value} in context {context} is {quick_ratio}', severity=xml.ErrorSeverity.INFO, instant=context.period.instant, context=context, quick_ratio=quick_ratio_param, children=child_lines)
                error_log.report(main_line)
            else:
                error_log.report(xbrl.Error.create('Quick ratio for context {context} cannot be computed because no reported values for liabilities were found.', severity = xml.ErrorSeverity.WARNING, context=context))

# Main entry point, will be called by RaptorXML after the XBRL instance validation job has finished
def on_xbrl_finished(job, instance):
    # instance object will be None if XBRL 2.1 validation was not successful
    if instance:
        calc_quick_ratio(instance, job.error_log)