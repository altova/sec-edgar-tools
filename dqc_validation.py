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
__copyright__ = "Copyright 2015-2017 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'
__version__ = '4.0.0'

# This script implements additional data quality validation rules as specified by the XBRL US Data Quality Committee (http://xbrl.us/data-quality/rules-guidance/).
# This script is designed to be used standalone or in conjunction with the EDGAR Filer Manual (EFM) rules implemented in script efm_validation.py. When using the efm_validation.py script, the DQC validation rules can be enabled with the enableDqcValidation option.
#
# The following script parameters can be additionally specified:
#
#   suppressErrors                  A list of DQC.US.nnnn.mmm error codes separated by | characters.
#
# Example invocations
#
# Validate a single filing
#   raptorxmlxbrl valxbrl --script=dqc_validation.py instance.xml
# Suppress a specific error
#   raptorxmlxbrl valxbrl --script=dqc_validation.py --script-param=suppressErrors:DQC.US.0004.16 instance.xml
# Validate a single filing using EFM and DQC rules
#   raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=enableDqcValidation:true instance.xml
#
# Using Altova RaptorXML+XBRL Server with XMLSpy client:
#
# 1a.   Copy efm_validation.py and all dqc_* files to the Altova RaptorXML Server script directory /etc/scripts/sec-edgar-tools/ (default C:\Program Files\Altova\RaptorXMLXBRLServer2016\etc\scripts\sec-edgar-tools\) or
# 1b.   Edit the <server.script-root-dir> tag in /etc/server_config.xml
# 2.    Start Altova RaptorXML+XBRL server.
# 3.    Start Altova XMLSpy, open Tools|Manage Raptor Servers... and connect to the running server
# 4.    Create a new configuration and rename it to e.g. "DQC CHECKS"
# 5.    Select the XBRL Instance property page and then set the script property to sec-edgar-tools/dqc_validation.py
# 6.    Select the new "DQC CHECKS" configuration in Tools|Raptor Servers and Configurations
# 7.    Open a SEC instance file
# 8.    Validate instance file with XML|Validate XML on Server (Ctrl+F8)


import collections,datetime,decimal,json,operator,os,re,sys
import altova_api.v2.xml as xml
import altova_api.v2.xsd as xsd
import altova_api.v2.xbrl as xbrl

RuleInfo = collections.namedtuple('ruleInfo',['ruleVersion','releaseDate','url'])

re_namespaces = {
    'country':  re.compile(r'http://xbrl\.(us|sec\.gov)/country/[0-9-]{10}'),
    'currency': re.compile(r'http://xbrl\.(us|sec\.gov)/currency/[0-9-]{10}'),
    'dei':      re.compile(r'http://xbrl\.(us|sec\.gov)/dei/[0-9-]{10}'),
    'exch':     re.compile(r'http://xbrl\.(us|sec\.gov)/exch/[0-9-]{10}'),
    'invest':   re.compile(r'http://xbrl\.(us|sec\.gov)/invest/[0-9-]{10}'),
    'naics':    re.compile(r'http://xbrl\.(us|sec\.gov)/naics/[0-9-]{10}'),
    'sic':      re.compile(r'http://xbrl\.(us|sec\.gov)/sic/[0-9-]{10}'),
    'stpr':     re.compile(r'http://xbrl\.(us|sec\.gov)/stpr/[0-9-]{10}'),
    'us-gaap':  re.compile(r'http://(xbrl\.us|fasb\.org)/us-gaap/[0-9-]{10}'),
}
re_standard_ns = re.compile(r'http://(xbrl\.(us|sec\.gov)|fasb\.org)/')

msg_template_properties = [
    'The properties of this ${fact1.name} fact are:',
    'Period: ${fact1.period}',
    'Dimensions: ${fact1.dimensions}',
    'Unit: ${fact1.unit}',
    'Rule version: ${ruleVersion}',
]
msg_templates = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_msg_templates.json')))

dqc_0001_axis_members = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0001_axis_members.json')))
dqc_0006_period_focus_durations = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0006_period_focus_durations.json')))
dqc_0008_calculations = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0008_calculations.json')))
dqc_0009_facts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0009_facts.json')))
dqc_0011_facts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0011_facts.json')))
dqc_0013_facts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0013_facts.json')))
dqc_0013_preconditions = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0013_preconditions.json')))
dqc_0014_facts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0014_facts.json')))
dqc_0015_facts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0015_facts.json')))
dqc_0015_member_exclusions = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0015_member_exclusions.json')))
dqc_0018_concepts = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0018_concepts.json')))
dqc_0041_default_members = json.load(open(os.path.join(os.path.dirname(__file__),'dqc_0041_default_members.json')))

arcrole_summation_item = 'http://www.xbrl.org/2003/arcrole/summation-item'

def prefixed_name(x):
    """Give a fact of concept returns the name formatted as [prefix:]name."""
    qname = x.qname
    return '%s:%s' % (qname.prefix, qname.local_name) if qname.prefix else qname.local_name

def label(x):
    """Give a fact of concept returns the text of the first English standard label."""
    if isinstance(x,xbrl.taxonomy.Concept):
        concept = x
    else:
        concept = x.concept
    labels = list(concept.labels(lang='en',label_role=xbrl.taxonomy.ROLE_LABEL))
    return labels[0].text if labels else prefixed_name(x)

def period_end(fact):
    """Given a fact returns either the end date of the duration period or instant date of the instant period."""
    period = fact.period_aspect_value
    if period.period_type == xbrl.PeriodType.START_END:
        return period.end
    elif period.period_type == xbrl.PeriodType.INSTANT:
        return period.instant
    else:
        return datetime.datetime.max

def period_duration(fact):
    """Given a fact returns the duration of the period in days."""
    period = fact.period_aspect_value
    if period.period_type == xbrl.PeriodType.START_END:
        return (period.end-period.start).days
    elif period.period_type == xbrl.PeriodType.INSTANT:
        return 0
    else:
        return sys.maxsize

def format_date(val,is_end=False):
    """Given a date or datetime object, return the date part as a string. If the is_end flag is set, the date represents the end of the day which is according to XBRL 2.1 midnight of the next day. In this case, a day is subtracted first before formatting."""
    if val.time() != datetime.time.min:
        return val.strftime('%Y-%m-%d %H:%M:%S')
    if is_end:
        val -= datetime.timedelta(days=1)
    return val.strftime('%Y-%m-%d')

def create_error(msg,location,severity,children,**kargs):
    """Creates a xbrl.Error object from a message template msg and other arguments depending on the template."""
    msg_parts = []
    msg_params = {}

    text_start = 0
    while True:
        param_start = msg.find('${',text_start)
        if param_start == -1:
            msg_parts.append(msg[text_start:])
            break
        if text_start < param_start:
            msg_parts.append(msg[text_start:param_start])

        param_start += 2
        param_end = msg.find('}',param_start)
        param = msg[param_start:param_end]
        param_parts = param.split('.')
        param = param.replace(':','_')
        param_values = kargs
        
        if param_parts[0] not in param_values:
            raise KeyError('Missing value for parameter '+param_parts[0])

        if isinstance(param_values[param_parts[0]],dict):
            param_values = param_values[param_parts[0]]
            if param_parts[1] not in param_values:
                raise KeyError('Missing value for parameter '+'.'.join(param_parts[:2]))
            param_parts = param_parts[1:]

        if isinstance(param_values[param_parts[0]],xbrl.Fact):
            fact = param_values[param_parts[0]]

            if param_parts[1] == 'fact':
                del param_parts[1]
            if param_parts[1] == 'name':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(prefixed_name(fact),tooltip=str(fact.qname),location=fact,quotes=False)
            elif param_parts[1] == 'localName':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(fact.local_name,tooltip=str(fact.qname),location=fact,quotes=False)
            elif param_parts[1] == 'label':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(label(fact),tooltip=str(fact.qname),location=fact,deflocation=fact.concept,quotes=False)
            elif param_parts[1] == 'value':
                msg_parts.append('{%s:value}'%param)
                if fact.xsi_nil:
                    msg_params[param] = xbrl.Error.Param('nil',location=fact.element.find_attribute(('nil',xsd.NAMESPACE_XSI)),quotes=False)
                elif fact.concept.is_numeric():
                    msg_params[param] = xbrl.Error.Param('{:,}'.format(fact.numeric_value),location=fact,quotes=False)
                else:
                    msg_params[param] = xbrl.Error.Param(fact.normalized_value,location=fact,quotes=False)
            elif param_parts[1] == 'period':
                period = fact.context.period
                if len(param_parts) > 2:
                    if param_parts[2] == 'startDate':
                        msg_parts.append('{%s:value}'%param)
                        msg_params[param] = xbrl.Error.Param(format_date(period.start_date.value),location=period.start_date,quotes=False)
                    elif param_parts[2] == 'endDate':
                        end_date = period.instant if period.type == xbrl.PeriodType.INSTANT else period.end_date
                        msg_parts.append('{%s:value}'%param)
                        msg_params[param] = xbrl.Error.Param(format_date(end_date.value,is_end=True),location=end_date,quotes=False)
                    elif param_parts[2] == 'instant':
                        msg_parts.append('{%s:value}'%param)
                        msg_params[param] = xbrl.Error.Param(format_date(period.instant.value,is_end=True),location=period.instant,quotes=False)
                    elif param_parts[2] == 'durationDays':
                        msg_parts.append('{%s}'%param)
                        msg_params[param] = xbrl.Error.Param(str(period_duration(fact)),quotes=False)
                    else:
                        raise KeyError('Unknown period property '+param_parts[2])
                else:
                    if period.type == xbrl.PeriodType.INSTANT:
                        msg_parts.append('{%s.instant:value}'%param)
                        msg_params[param+'.instant'] = xbrl.Error.Param(format_date(period.instant.value,is_end=True),location=period.instant,quotes=False)
                    elif period.type == xbrl.PeriodType.START_END:
                        msg_parts.append('{%s.startDate:value} - {%s.endDate:value}'%(param,param))
                        msg_params[param+'.startDate'] = xbrl.Error.Param(format_date(period.start_date.value),location=period.start_date,quotes=False)
                        msg_params[param+'.endDate'] = xbrl.Error.Param(format_date(period.end_date.value,is_end=True),location=period.end_date,quotes=False)
                    else:
                        msg_parts.append('forever')
            elif param_parts[1] == 'dimensions':
                dimension_aspects = list(fact.context.dimension_aspect_values)
                if dimension_aspects:
                    msg_parts.append(', '.join('{%s.dim%d} = {%s.member%d}'%(param,i,param,i) for i, aspect in enumerate(dimension_aspects)))
                    for i, aspect in enumerate(dimension_aspects):
                        msg_params['%s.dim%d'%(param,i)] = xbrl.Error.Param(prefixed_name(aspect.dimension),tooltip=str(aspect.dimension.qname),deflocation=aspect.dimension,quotes=False)
                        msg_params['%s.member%d'%(param,i)] = xbrl.Error.Param(prefixed_name(aspect.value),tooltip=str(aspect.value.qname),deflocation=aspect.value,quotes=False)
                else:
                    msg_parts.append('none')
            elif param_parts[1] == 'unit':
                if fact.unit:
                    numerator = list(fact.unit.numerator_measures)
                    denominator = list(fact.unit.denominator_measures)
                    msg_parts.append(' '.join('{%s.num%d:value}'%(param,i) for i, measure in enumerate(numerator)))
                    for i, measure in enumerate(numerator):
                        msg_params['%s.num%d'%(param,i)] = xbrl.Error.Param(measure.value.local_name,tooltip=str(measure.value),location=measure,quotes=False)
                    if len(denominator):
                        msg_parts.append(' / ')
                        msg_parts.append(' '.join('{%s.denom%d:value}'%(param,i) for i, measure in enumerate(denominator)))
                        for i, measure in enumerate(denominator):
                            msg_params['%s.denom%d'%(param,i)] = xbrl.Error.Param(measure.value.local_name,tooltip=str(measure.value),location=measure,quotes=False)
                else:
                    msg_parts.append('none')
            elif param_parts[1] == 'decimals':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(str(fact.decimals),location=fact.element.find_attribute('decimals'),quotes=False)
            else:
                raise KeyError('Unknown fact property '+param_parts[1])

        elif isinstance(param_values[param_parts[0]],xbrl.taxonomy.Concept):
            concept = param_values[param_parts[0]]

            if param_parts[1] == 'name':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(prefixed_name(concept),tooltip=str(concept.qname),deflocation=concept,quotes=False)
            elif param_parts[1] == 'localName':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(concept.name,tooltip=str(concept.qname),deflocation=concept,quotes=False)
            elif param_parts[1] == 'label':
                msg_parts.append('{%s}'%param)
                msg_params[param] = xbrl.Error.Param(label(concept),tooltip=str(concept.qname),deflocation=concept,quotes=False)

        elif isinstance(param_values[param_parts[0]],RuleInfo):
            ruleVersion = param_values[param_parts[0]]
            msg_parts.append('{%s}'%param)
            msg_params[param] = xbrl.Error.ExternalLinkParam(ruleVersion.url,title=ruleVersion.ruleVersion,tooltip=ruleVersion.releaseDate,quotes=False)

        elif isinstance(param_values[param_parts[0]],xbrl.Error.Param):
            msg_parts.append('{%s}'%param)
            msg_params[param] = param_values[param_parts[0]]
            
        else:
            msg_parts.append('{%s}'%param)
            msg_params[param] = xbrl.Error.Param(str(param_values[param_parts[0]]),quotes=False)

        text_start = param_end+1

    return xbrl.Error.create(''.join(msg_parts), location=location, severity=severity, children=children, **msg_params )

def report_error(error_log,suppress_errors,rule_id,location=None,variation=None,**kargs):
    """Constructs and reports an error given an error code and additional arguments. This function creates xbrl.Error objects according to the associated message template and adds it to the error log."""
    if rule_id in suppress_errors or rule_id.rsplit('.',1)[0] in suppress_errors:
        return
    if rule_id in msg_templates:
        msg = msg_templates[rule_id]
    else:
        # Remove test case number
        msg = msg_templates[rule_id.rsplit('.',1)[0]]
    kargs['ruleVersion'] = RuleInfo(*msg['version'])
    if variation is not None:
        msg = msg['variations'][variation]

    property_lines = []
    for line in msg_template_properties[1:]:
        if 'fact1' not in line or 'fact1' in kargs:
            property_lines.append(create_error(line,None,xml.ErrorSeverity.OTHER,None,**kargs))

    child_lines = []
    hints = msg.get('hint',[])
    if not isinstance(hints,list):
        hints = [hints]
    for hint in hints:
        child_lines.append(create_error(hint,None,xml.ErrorSeverity.INFO,None,**kargs))
    if 'fact1' in kargs:
        location = kargs['fact1']
        child_lines.append(create_error(msg_template_properties[0],None,xml.ErrorSeverity.OTHER,property_lines,**kargs))

    msg_text = '[%s] %s' % (rule_id,msg['msg'])
    error_log.report(create_error(msg_text,location,xml.ErrorSeverity.ERROR,child_lines,**kargs))

def decimal_comparison(fact1,fact2,cmp):
    """Rounds both numerical facts to the least accurate precision of both facts and calls the given cmp function with the rounded decimal values."""
    # When comparing two numeric fact values in a rule, the comparison needs to take into account different decimals. Numbers are compared based on the lowest decimal value rounded per XBRL specification. For example, the number 532,000,000 with decimals of -6 is considered to be equivalent to 532,300,000 with a decimals value of -5. In this case the 532,300,000 is rounded to a million and then compared to the value of 532,000,000. (Note that XBRL specifies "round half to nearest even" so 532,500,000 with decimals -6 rounds to 532,000,000, and 532,500,001 rounds to 533,000,000.)
    decimals = min(fact1.inferred_decimals,fact2.inferred_decimals)
    if decimals == float('inf'):
        return cmp(fact1.numeric_value,fact2.numeric_value)
    return cmp(fact1.round_numeric_value(decimals),fact2.round_numeric_value(decimals),decimals)

def equal_within_tolerance(val1,val2,decimals=None):
    """Returns true if va1 is equal to val2 within given tolerance."""
    # The rule allows a tolerance for rounding between the values tested of 2 based on the scale of the values. For example, if the values are reported in millions, the rounding tolerance would be $2 million.
    if decimals is None:
        return val1 == val2
    return abs(val1-val2) <= decimal.Decimal(2).scaleb(-decimals)

def less_or_equal(val1,val2,decimals=None):
    """Returns true if va1 is less or equal than val2."""
    return val1 <= val2

def dimension_value(fact,dim):
    """Returns the domain member for the given dimension aspect or None if fact does not have this dimension aspect."""
    aspect_value = fact.dimension_aspect_value(dim)
    return aspect_value.value if aspect_value else None

def reporting_period_ends(instance,dei_namespace):
    """Returns a dict of DocumentPeriodEndDate fact and end date tuples keyed by the legal entity domain member."""

    reporting_period_end_for_legal_entity = {}

    dim_LegalEntityAxis = instance.dts.resolve_concept(xml.QName('LegalEntityAxis',dei_namespace))
    concept_DocumentPeriodEndDate = instance.dts.resolve_concept(xml.QName('DocumentPeriodEndDate',dei_namespace))
    for fact in instance.facts.filter(concept_DocumentPeriodEndDate):
        # Amendment: Use the period end date of the context and not the DocumentPeriodEndDate value! 
        end_date = fact.period_aspect_value.end

        legal_entity = dimension_value(fact,dim_LegalEntityAxis)
        if legal_entity not in reporting_period_end_for_legal_entity or reporting_period_end_for_legal_entity[legal_entity][1] < end_date:
            reporting_period_end_for_legal_entity[legal_entity] = (fact,end_date)

    return reporting_period_end_for_legal_entity

def textblock_facts(instance):
    """Returns an xbrl.FactSet object with facts whose concept's item type is or is derived from textBlockItemType."""
    facts = xbrl.FactSet()

    type_textBlockItemType = instance.dts.schema.resolve_type_definition(xml.QName('textBlockItemType','http://www.xbrl.org/dtr/type/non-numeric'))
    if type_textBlockItemType:

        is_textblock_cache = {}
        for fact in instance.facts:
            is_textblock = is_textblock_cache.get(fact.concept,None)
            if is_textblock is None:
                is_textblock = fact.concept.type_definition.is_derived_from(type_textBlockItemType)
                is_textblock_cache[fact.concept] = is_textblock

            if is_textblock:
                facts.add(fact)

    return facts

def facts_in_namespace(instance,namespace,ignored):
    """Returns an xbrl.FactSet object with facts whose concept is in the given namespace."""

    facts = xbrl.FactSet()
    for fact in instance.facts:
        qname = fact.qname
        if qname.namespace_name == namespace and qname.local_name not in ignored:
            facts.add(fact)
    return facts

def is_extension(namespace):
    """Returns True if the given namespace is not a standard US-GAAP or SEC taxonomy namespace."""
    return re_standard_ns.match(namespace) is None

def _subtree_children_iterate(network,concept,children):
    for rel in network.relationships_from(concept):
        children.append(rel)
        _subtree_children_iterate(network,rel.target,children)
    
def _subtree_children(network,concept):
    children = []
    _subtree_children_iterate(network,concept,children)
    return children

def _get_dimension_values_iterate(network,concept,dims):
    if isinstance(concept,xbrl.xdt.Dimension):
        dims[concept] = list(_subtree_children(network,concept))
        return
    for rel in network.relationships_from(concept):
        _get_dimension_values_iterate(network,rel.target,dims)

def _get_dimension_values(network):
    dims = {}
    for root in network.roots:
        _get_dimension_values_iterate(network,root,dims)
    return dims

def dqc_0001(instance,error_log,suppress_errors,namespaces):
    """DQC_0001 Axis with Inappropriate Members"""

    handled = set()
    for role in instance.dts.presentation_link_roles():
        for dim, rels in _get_dimension_values(instance.dts.presentation_network(role)).items():
            rule = dqc_0001_axis_members.get(dim.target_namespace,{}).get(dim.name)
            if rule:
                for rel in rels:
                    member = rel.target
                    if dim.default_member == member:
                        continue

                    ext = is_extension(member.target_namespace)
                    if ext:
                        valid = rule['extensions'] if isinstance(rule['extensions'], bool) else member.name in rule['extensions']
                    elif rule['disallowed']:
                        valid = member.name not in rule['disallowed']
                    else:
                        valid = member.name in rule['allowed']
                    if not valid and (dim,member) not in handled:
                        # Mimick Arelle's behaviour of only reporting the first occurrence of each type of error
                        handled.add((dim,member))
                        rule_id = 'DQC.US.0001.'+rule['id'].split('.')[-1]
                        cs = xbrl.ConstraintSet()
                        cs[dim] = member
                        facts = instance.facts.filter(cs)
                        for fact in facts:
                            report_error(error_log,suppress_errors,rule_id,rel.arc,'ext' if ext else 'std',Rule={'axis':dim,'member':member},fact1=fact)
                        if len(facts) == 0:
                            report_error(error_log,suppress_errors,rule_id,rel.arc,'nofact',Rule={'axis':dim,'member':member},group=xbrl.Error.Param(instance.dts.role_definition(role),tooltip=role))
    
def _dqc_0004(instance,error_log,suppress_errors,rule_id,concept1,concept2):
    for fact1 in instance.facts.filter(concept1,allow_nil=False):
        # All comparisons between fact values occur between facts of equivalent dimensions. A rule will produce a message for each occurrence of the compared facts in equivalent dimensions.
        cs = xbrl.ConstraintSet(fact1)
        cs[xbrl.Aspect.CONCEPT] = concept2
        for fact2 in instance.facts.filter(cs,allow_nil=False,allow_additional_dimensions=False):
            if not decimal_comparison(fact1,fact2,equal_within_tolerance):
                report_error(error_log,suppress_errors,rule_id,fact1=fact1,fact2=fact2)

def dqc_0004_16(instance,error_log,suppress_errors,namespaces):
    """DQC_0004 Element Values Are Equal"""

    concept_Assets = instance.dts.resolve_concept(xml.QName('Assets',namespaces.get('us-gaap')))
    concept_LiabilitiesAndStockholdersEquity = instance.dts.resolve_concept(xml.QName('LiabilitiesAndStockholdersEquity',namespaces.get('us-gaap')))
    if concept_Assets and concept_LiabilitiesAndStockholdersEquity:
        _dqc_0004(instance,error_log,suppress_errors,'DQC.US.0004.16',concept_Assets,concept_LiabilitiesAndStockholdersEquity)

def dqc_0004(instance,error_log,suppress_errors,namespaces):
    """DQC_0004 Element Values Are Equal"""

    dqc_0004_16(instance,error_log,suppress_errors,namespaces)

def _dqc_0005(instance,error_log,suppress_errors,rule_id,namespaces,facts,reporting_period_ends,cmp,additional_params={}):
    dim_LegalEntityAxis = instance.dts.resolve_concept(xml.QName('LegalEntityAxis',namespaces['dei']))
    concept_EntityCommonStockSharesOutstanding = instance.dts.resolve_concept(xml.QName('EntityCommonStockSharesOutstanding',namespaces['dei']))
    for fact1 in facts:

        reporting_period_end = reporting_period_ends.get(dimension_value(fact1,dim_LegalEntityAxis))
        if not reporting_period_end:
            reporting_period_end = reporting_period_ends.get(dim_LegalEntityAxis.default_member)

        if reporting_period_end and not cmp(period_end(fact1),reporting_period_end[1]):
            params = {'fact1':fact1,'dei:DocumentPeriodEndDate':reporting_period_end[0]}
            params.update(additional_params)
            report_error(error_log,suppress_errors,rule_id,**params)

def dqc_0005_17(instance,error_log,suppress_errors,namespaces,reporting_period_ends):
    """DQC_0005.17 Entity Common Stock, Shares Outstanding"""

    concept_EntityCommonStockSharesOutstanding = instance.dts.resolve_concept(xml.QName('EntityCommonStockSharesOutstanding',namespaces['dei']))

    facts = instance.facts.filter(concept_EntityCommonStockSharesOutstanding)
    _dqc_0005(instance,error_log,suppress_errors,'DQC.US.0005.17',namespaces,facts,reporting_period_ends,operator.ge)

def dqc_0005_48(instance,error_log,suppress_errors,namespaces,reporting_period_ends):
    """DQC_0005.48 Subsequent events"""

    dim_SubsequentEventTypeAxis = instance.dts.resolve_concept(xml.QName('SubsequentEventTypeAxis',namespaces.get('us-gaap')))
    if dim_SubsequentEventTypeAxis:

        cs = xbrl.ConstraintSet()
        cs[dim_SubsequentEventTypeAxis] = xbrl.ExplicitDimensionAspectValue(dim_SubsequentEventTypeAxis,None)
        facts = instance.facts - instance.facts.filter(cs)
        _dqc_0005(instance,error_log,suppress_errors,'DQC.US.0005.48',namespaces,facts,reporting_period_ends,operator.gt,{'us-gaap:SubsequentEventTypeAxis':dim_SubsequentEventTypeAxis})

def dqc_0005_49(instance,error_log,suppress_errors,namespaces,reporting_period_ends):
    """DQC_0005.49 Subsequent events"""

    dim_StatementScenarioAxis = instance.dts.resolve_concept(xml.QName('StatementScenarioAxis',namespaces.get('us-gaap')))
    if dim_StatementScenarioAxis:
        member_ScenarioForecastMember = instance.dts.resolve_concept(xml.QName('ScenarioForecastMember',namespaces.get('us-gaap')))

        cs = xbrl.ConstraintSet()
        cs[dim_StatementScenarioAxis] = member_ScenarioForecastMember
        facts = instance.facts.filter(cs)
        _dqc_0005(instance,error_log,suppress_errors,'DQC.US.0005.49',namespaces,facts,reporting_period_ends,operator.gt,{'us-gaap:StatementScenarioAxis':dim_StatementScenarioAxis,'us-gaap:ScenarioForecastMember':member_ScenarioForecastMember})

def dqc_0005(instance,error_log,suppress_errors,namespaces):
    """DQC_0005 Context Dates After Period End Date"""

    concept_DocumentType = instance.dts.resolve_concept(xml.QName('DocumentType',namespaces['dei']))
    facts_DocumentType = instance.facts.filter(concept_DocumentType)
    if len(facts_DocumentType) != 1 or facts_DocumentType[0].normalized_value in ('S-1', 'S-3', 'S-4', 'S-6', 'S-8', 'S-11', 'S-20', 'S-1/A', 'S-3/A', 'S-4/A', 'S-6/A', 'S-8/A', 'S-11/A', 'S-20/A'):
        # Appendix A
        # Exclusions from the rule: S-1, S-3, S-4, S-6, S-8, S-11, S-20, S-1/A, S-3/A, S-4/A, S-6/A, S-8/A, S-11/A and S-20/A
        return
    
    reporting_periods = reporting_period_ends(instance,namespaces['dei'])
    dqc_0005_17(instance,error_log,suppress_errors,namespaces,reporting_periods)
    dqc_0005_48(instance,error_log,suppress_errors,namespaces,reporting_periods)
    dqc_0005_49(instance,error_log,suppress_errors,namespaces,reporting_periods)

def _dqc_0006(instance,error_log,suppress_errors,dim_LegalEntityAxis,period_focus_for_legal_entity,facts):
    for fact1 in facts:

        period_focus = period_focus_for_legal_entity.get(dimension_value(fact1,dim_LegalEntityAxis))
        if not period_focus:
            period_focus = period_focus_for_legal_entity.get(dim_LegalEntityAxis.default_member)
        if period_focus and period_focus.normalized_value in dqc_0006_period_focus_durations:

            duration = dqc_0006_period_focus_durations.get(period_focus.normalized_value)
            if not duration[0] <= period_duration(fact1) <= duration[1]:
                report_error(error_log,suppress_errors,'DQC.US.0006.14',**{'fact1':fact1,'dei:DocumentFiscalPeriodFocus':period_focus})

def dqc_0006(instance,error_log,suppress_errors,namespaces):
    """DQC_0006 DEI and Block Tag Date Contexts"""

    concept_DocumentType = instance.dts.resolve_concept(xml.QName('DocumentType',namespaces['dei']))
    facts_DocumentType = instance.facts.filter(concept_DocumentType)
    if len(facts_DocumentType) != 1 or facts_DocumentType[0].normalized_value.endswith('T') or facts_DocumentType[0].normalized_value.endswith('T/A'):
        # This rule also does not test any transition period filings, which are identified by the letter "T" in the form name.
        # Transition period filings are submitted when a filer changes their fiscal year.
        # Transition period filings may cover periods which are different from the general quarter or annual length.
        return

    dim_LegalEntityAxis = instance.dts.resolve_concept(xml.QName('LegalEntityAxis',namespaces['dei']))
    concept_DocumentFiscalPeriodFocus = instance.dts.resolve_concept(xml.QName('DocumentFiscalPeriodFocus',namespaces['dei']))

    period_focus_for_legal_entity = {}
    for fact in instance.facts.filter(concept_DocumentFiscalPeriodFocus):
        period_focus_for_legal_entity[dimension_value(fact,dim_LegalEntityAxis)] = fact

    fact_names = [
        'AmendmentDescription',
        'AmendmentFlag',
        'CurrentFiscalYearEndDate',
        'DocumentPeriodEndDate',
        'DocumentFiscalYearFocus',
        'DocumentFiscalPeriodFocus',
        'DocumentType',
        'EntityRegistrantName',
        'EntityCentralIndexKey',
        'EntityFilerCategory',
    ]

    for name in fact_names:
        concept = instance.dts.resolve_concept(xml.QName(name,namespaces['dei']))
        if concept:
            _dqc_0006(instance,error_log,suppress_errors,dim_LegalEntityAxis,period_focus_for_legal_entity,instance.facts.filter(concept))

    _dqc_0006(instance,error_log,suppress_errors,dim_LegalEntityAxis,period_focus_for_legal_entity,textblock_facts(instance))
    
    
def dqc_0008(instance,error_log,suppress_errors,namespaces):
    """DQC_0008 Reversed Calculation"""
    dts = instance.dts
    ns = namespaces.get('us-gaap')
    us_gaap_calc = dqc_0008_calculations.get(ns)
    if us_gaap_calc:
        for linkrole in dts.calculation_link_roles(arcrole_summation_item):
            nw = dts.calculation_network(linkrole, arcrole_summation_item)
            for rel in nw.relationships:
                us_gaap_items = us_gaap_calc.get(rel.target_concept.name, [])
                if rel.source_concept.name in us_gaap_items:
                    report_error(error_log,suppress_errors,'DQC.US.0008.6819',extCalcTarget=rel.target_concept,extCalcSource=rel.source_concept)
    

def dqc_0009(instance,error_log,suppress_errors,namespaces):
    """DQC_0009 Element A must be less than or equal to Element B"""

    for rule_id, prefix1, name1, prefix2, name2 in dqc_0009_facts:
        concept1 = instance.dts.resolve_concept(xml.QName(name1,namespaces.get(prefix1)))
        concept2 = instance.dts.resolve_concept(xml.QName(name2,namespaces.get(prefix2)))
        if concept1 and concept2:
            for fact1 in instance.facts.filter(concept1,allow_nil=False):
                # All comparisons between fact values occur between facts of equivalent dimensions.  A rule will produce a message for each occurrence of the compared facts in equivalent dimensions.
                cs = xbrl.ConstraintSet(fact1)
                cs[xbrl.Aspect.CONCEPT] = concept2
                for fact2 in instance.facts.filter(cs,allow_nil=False,allow_additional_dimensions=False):
                    if not decimal_comparison(fact1,fact2,less_or_equal):
                        report_error(error_log,suppress_errors,rule_id,fact1=fact1,fact2=fact2)


def dqc_0011(instance,error_log,suppress_errors,namespaces):
    """DQC_0011 Dimensional Equivalents """

    for rule_id, lineItemPrefix, lineItemName, dimItemPrefix, dimItemName, axisPrefix, axisName, memberPrefix, memberName, weight in dqc_0011_facts:
        lineConcept = instance.dts.resolve_concept(xml.QName(lineItemName, namespaces.get(lineItemPrefix)))
        dimConcept = instance.dts.resolve_concept(xml.QName(dimItemName,namespaces.get(dimItemPrefix)))
        axisConcept = instance.dts.resolve_concept(xml.QName(axisName,namespaces.get(axisPrefix)))
        memberConcept = instance.dts.resolve_concept(xml.QName(memberName,namespaces.get(memberPrefix)))
        # select all facts with name lineItemName and no value for explicit dimension axisName
        lineItemConstraintSet = xbrl.ConstraintSet()
        lineItemConstraintSet.add(xbrl.ConceptAspectValue(lineConcept))
        lineItemConstraintSet.add(xbrl.ExplicitDimensionAspectValue(axisConcept))
        lineFacts = instance.facts.filter(lineItemConstraintSet)
        for lineFact in lineFacts:
            if not isinstance(lineFact, xbrl.Item) or lineFact.xsi_nil:
                continue
            # select all facts with name dimItemName and explicit dimension axisName=memberName and all other aspect values equal to their respective value of lineFact
            dimItemConstraintSet = lineFact.aspect_values
            dimItemConstraintSet.add(xbrl.ConceptAspectValue(dimConcept))
            dimItemConstraintSet.add(xbrl.ExplicitDimensionAspectValue(axisConcept, memberConcept))
            dimFacts = instance.facts.filter(dimItemConstraintSet)
            lineValue = lineFact.effective_numeric_value
            for dimFact in dimFacts:
                if not isinstance(dimFact, xbrl.Item) or dimFact.xsi_nil:
                    continue
                dimValue = dimFact.effective_numeric_value
                if dimValue * weight != lineValue:
                    report_error(error_log,suppress_errors,rule_id,fact1=lineFact,fact2=dimFact,weight=weight)

        
def _dqc_0013_precondition_check(instance,namespaces,context):
    cs = xbrl.ConstraintSet(context)
    
    for name, summation in dqc_0013_preconditions.items():
        cs[xbrl.Aspect.CONCEPT] = instance.dts.resolve_concept(xml.QName(name,namespaces['us-gaap']))
        precondition_facts = instance.facts.filter(cs)
        if precondition_facts:
            val = 0
            for name in summation:
                cs[xbrl.Aspect.CONCEPT] = instance.dts.resolve_concept(xml.QName(name,namespaces['us-gaap']))
                for fact in instance.facts.filter(cs):
                    val += fact.numeric_value
            if val > 0:
                return precondition_facts[0]
    
    return None

def dqc_0013(instance,error_log,suppress_errors,namespaces):
    """DQC_0013 Negative Values with Dependence"""

    cache = {}
    for rule_id, perfix, name in dqc_0013_facts:
        concept = instance.dts.resolve_concept(xml.QName(name,namespaces.get(perfix)))
        if concept:
            for fact1 in instance.facts.filter(concept,allow_nil=False):
                if fact1.numeric_value < 0 and not _dqc_0015_member_exclusions_check(fact1):
                    if fact1.context in cache:
                        precondition_fact = cache[fact1.context]
                    else:
                        precondition_fact = _dqc_0013_precondition_check(instance,namespaces,fact1.context)
                        cache[fact1.context] = precondition_fact
                    if precondition_fact:
                        report_error(error_log,suppress_errors,rule_id,fact1=fact1,preconditionfact=precondition_fact)

def has_dimensions(context):
    try:
        next(context.dimension_aspect_values)
        return True
    except StopIteration:
        return False

def dqc_0014(instance,error_log,suppress_errors,namespaces):
    """DQC_0014 Negative Values with No Dimensions"""
    
    for rule_id, perfix, name in dqc_0014_facts:
        concept = instance.dts.resolve_concept(xml.QName(name,namespaces.get(perfix)))
        if concept:
            for fact1 in instance.facts.filter(concept,allow_nil=False):
                if fact1.numeric_value < 0 and not has_dimensions(fact1.context):
                    report_error(error_log,suppress_errors,rule_id,fact1=fact1) 

def _dqc_0015_member_exclusions_test_contains(rule,dim_aspect):
    name = dim_aspect.value.name if rule['dim'] == 'member' else dim_aspect.dimension.name
    return re.search(rule['text'],name,re.IGNORECASE)

def _dqc_0015_member_exclusions_test_equals(rule,dim_aspect):
    name = dim_aspect.value.name if rule['dim'] == 'member' else dim_aspect.dimension.name
    return name == rule['name']

def _dqc_0015_member_exclusions_test(rule,dim_aspect):
    if rule['test'] == 'contains':
        return _dqc_0015_member_exclusions_test_contains(rule,dim_aspect)
    elif rule['test'] == 'equals':
        return _dqc_0015_member_exclusions_test_equals(rule,dim_aspect)
    elif rule['test'] == 'and':
        return _dqc_0015_member_exclusions_test(rule['arg1'],dim_aspect) and _dqc_0015_member_exclusions_test(rule['arg2'],dim_aspect)
    elif rule['test'] == 'or':
        return _dqc_0015_member_exclusions_test(rule['arg1'],dim_aspect) or _dqc_0015_member_exclusions_test(rule['arg2'],dim_aspect)
    raise RuntimeError('Unknown member exclusion test '+rule['test'])

def _dqc_0015_member_exclusions_check(fact):
    for dim_aspect in fact.context.dimension_aspect_values:
        for rule in dqc_0015_member_exclusions:
            if _dqc_0015_member_exclusions_test(rule,dim_aspect):
                return True
    return False

def dqc_0015(instance,error_log,suppress_errors,namespaces):
    """DQC_0015 Negative Values"""

    for rule_id, perfix, name in dqc_0015_facts:
        concept = instance.dts.resolve_concept(xml.QName(name,namespaces.get(perfix)))
        if concept:
            for fact1 in instance.facts.filter(concept,allow_nil=False):
                if fact1.numeric_value < 0 and not _dqc_0015_member_exclusions_check(fact1):
                    report_error(error_log,suppress_errors,rule_id,fact1=fact1)

def _dqc_0018(error_log,suppress_errors,us_gaap,deprecated_concepts,network,rels):
    for rel in rels:
        if rel.target.target_namespace == us_gaap and rel.target.name in deprecated_concepts:
            report_error(error_log,suppress_errors,'DQC.US.0018.34',rel.arc,element=rel.target,deprecatedlabel=deprecated_concepts[rel.target.name])
        _dqc_0018(error_log,suppress_errors,us_gaap,deprecated_concepts,network,network.relationships_from(rel.target))

def dqc_0018(instance,error_log,suppress_errors,namespaces):
    """DQC_0018 Deprecated Element is Used in the Filing"""

    us_gaap = namespaces['us-gaap']
    deprecated_concepts = dqc_0018_concepts.get(us_gaap)
    if deprecated_concepts:
        for role in instance.dts.presentation_link_roles():
            network = instance.dts.presentation_network(role)
            for root in network.roots:
                if root.target_namespace == us_gaap and root.name in deprecated_concepts:
                    report_error(error_log,suppress_errors,'DQC.US.0018.34',element=root,deprecatedlabel=deprecated_concepts[root.name])
                _dqc_0018(error_log,suppress_errors,us_gaap,deprecated_concepts,network,network.relationships_from(root))
                    
def dqc_0033(instance,error_log,suppress_errors,namespaces):
    """DQC_0033 Document Period End Date Context"""

    dei_namespace = namespaces['dei']
    dim_LegalEntityAxis = instance.dts.resolve_concept(xml.QName('LegalEntityAxis',dei_namespace))

    reporting_periods = {}
    concept_DocumentPeriodEndDate = instance.dts.resolve_concept(xml.QName('DocumentPeriodEndDate',dei_namespace))
    for fact1 in instance.facts.filter(concept_DocumentPeriodEndDate):
        end_date = datetime.datetime.combine(fact1.element.schema_actual_value.value,datetime.time()) + datetime.timedelta(days=1)
        is_valid = abs((end_date - fact1.period_aspect_value.end).days) <= 3
        legal_entity = dimension_value(fact1,dim_LegalEntityAxis)
        reporting_periods[legal_entity] = (fact1,is_valid)

    for fact1 in facts_in_namespace(instance,dei_namespace,('EntityCommonStockSharesOutstanding','EntityPublicFloat','DocumentPeriodEndDate','EntityNumberOfEmployees','EntityListingDepositoryReceiptRatio')):

        reporting_period = reporting_periods.get(dimension_value(fact1,dim_LegalEntityAxis))
        if not reporting_period:
            reporting_period = reporting_periods.get(dim_LegalEntityAxis.default_member)

        if reporting_period and reporting_period[1] and period_end(fact1) != period_end(reporting_period[0]):
            report_error(error_log,suppress_errors,'DQC.US.0033.2',**{'fact1':fact1,'dei:DocumentPeriodEndDate':reporting_period[0]})

def dqc_0036(instance,error_log,suppress_errors,namespaces):
    """DQC_0036 Document Period End Date Context / Fact Value Check"""

    concept_DocumentPeriodEndDate = instance.dts.resolve_concept(xml.QName('DocumentPeriodEndDate',namespaces['dei']))
    for fact1 in instance.facts.filter(concept_DocumentPeriodEndDate):
        end_date = datetime.datetime.combine(fact1.element.schema_actual_value.value,datetime.time()) + datetime.timedelta(days=1)
        if abs((end_date - fact1.period_aspect_value.end).days) > 3:
            report_error(error_log,suppress_errors,'DQC.US.0036.1',fact1=fact1)

def dqc_0041(instance,error_log,suppress_errors,namespaces):
    """DQC_0041 Axis with a Default Member that Differs from the US GAAP Taxonomy"""

    for dim in instance.dts.dimensions:
        if dim.is_explicit():
            default_member = dim.default_member
            if not default_member:
                continue
            usgaap_default_member = dqc_0041_default_members.get(dim.target_namespace,{}).get(dim.name)
            if usgaap_default_member and default_member.name != usgaap_default_member:
                report_error(error_log,suppress_errors,'DQC.US.0041.73',axis=dim,axis_default=instance.dts.resolve_concept(xml.QName(usgaap_default_member,dim.target_namespace)),default=default_member)

def standard_namespaces(dts):
    """Returns a dict of prefix and namespace key/value pairs for standard namespaces."""
    namespaces = {}
    for taxonomy in dts.taxonomy_schemas:
        if taxonomy.target_namespace:
            for prefix, re in re_namespaces.items():
                if re.fullmatch(taxonomy.target_namespace):
                    namespaces[prefix] = taxonomy.target_namespace
    return namespaces

def parse_suppress_errors(params):
    """Returns a list with suppressed error codes."""
    val = params.get('suppressErrors', None)
    if not val:
        return []
    return val.split('|')

def validate(instance,error_log,params={}):
    """Performs additional validation of xBRL instance according to DQC rules."""
    if instance:
        error_log.report(xbrl.Error.create(
            'Verified {DQC} with Altova RaptorXML+XBRL',
            severity=xml.ErrorSeverity.INFO,
            location=instance,
            DQC=xbrl.Error.ExternalLinkParam('http://xbrl.us/data-quality/rules-guidance/',title='DQC validation rules',quotes=False)
        ))
        suppress_errors = set(code.strip() for code in parse_suppress_errors(params))
        namespaces = standard_namespaces(instance.dts)
        if 'dei' in namespaces:
            dqc_0001(instance,error_log,suppress_errors,namespaces)
            dqc_0004(instance,error_log,suppress_errors,namespaces)
            dqc_0005(instance,error_log,suppress_errors,namespaces)
            dqc_0006(instance,error_log,suppress_errors,namespaces)
            dqc_0008(instance,error_log,suppress_errors,namespaces)
            dqc_0009(instance,error_log,suppress_errors,namespaces)
            dqc_0011(instance,error_log,suppress_errors,namespaces)
            dqc_0013(instance,error_log,suppress_errors,namespaces)
            dqc_0014(instance,error_log,suppress_errors,namespaces)
            dqc_0015(instance,error_log,suppress_errors,namespaces)
            dqc_0018(instance,error_log,suppress_errors,namespaces)
            dqc_0033(instance,error_log,suppress_errors,namespaces)
            dqc_0036(instance,error_log,suppress_errors,namespaces)
            dqc_0041(instance,error_log,suppress_errors,namespaces)

# Main script callback entry points. These functions will be called by RaptorXML after the XBRL instance validation job has finished.

def on_xbrl_finished_dts(job, dts):
    pass

def on_xbrl_finished(job, instance):
    # instance object will be None if XBRL 2.1 validation was not successful.
    validate(instance,job.error_log,job.script_params)
