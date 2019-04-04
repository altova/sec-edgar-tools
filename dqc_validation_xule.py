# Copyright 2019 Altova GmbH
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
__copyright__ = "Copyright 2019 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'
__version__ = '8.0'

# This script executes additional DQC v8 validation rules published as XULE rulesets by the XBRL US Data Quality Committee (http://xbrl.us/data-quality/rules-guidance/).
#
# The following script parameters can be additionally specified:
#
#   dqcRepositoryPath               The path to the DQC XULE rules repository.
#   suppressErrors                  A list of DQC.US.nnnn.mmm error codes separated by | characters.
#
# Example invocations
#
# Validate a single filing
#   raptorxmlxbrl valxbrl --script=dqc_validation_xule.py instance.xml
#   raptorxmlxbrl valxbrl --script=dqc_validation_xule.py --script-param=dqcRepositoryPath:/path/to/dqc_us_rules-8.0.0/ instance.xml
# Suppress a specific error
#   raptorxmlxbrl valxbrl --script=dqc_validation_xule.py --script-param=suppressErrors:DQC.US.0004.16 instance.xml
#
# Using Altova RaptorXML+XBRL Server with XMLSpy client:
#
# 0.    Download the DQC v8 release from https://github.com/DataQualityCommittee/dqc_us_rules/releases
# 1a.   Copy dqc_validation_xule.py file to the Altova RaptorXML Server script directory /etc/scripts/sec-edgar-tools/ (default C:\Program Files\Altova\RaptorXMLXBRLServer2019\etc\scripts\sec-edgar-tools\) or
# 1b.   Edit the <server.script-root-dir> tag in /etc/server_config.xml
# 2.    Start Altova RaptorXML+XBRL server.
# 3.    Start Altova XMLSpy, open Tools|Manage Raptor Servers... and connect to the running server
# 4.    Create a new configuration and rename it to e.g. "DQC CHECKS"
# 5.    Select the XBRL Instance property page and then set the script property to sec-edgar-tools/dqc_validation_xule.py
# 6.    Select the new "DQC CHECKS" configuration in Tools|Raptor Servers and Configurations
# 7.    Open a SEC instance file
# 8.    Validate instance file with XML|Validate XML on Server (Ctrl+F8)

import json
from altova_api.v2 import xml, xsd, xbrl, beta, open, ProductInfo
xbrl.xule = beta.xbrl.xule

def location(result):
    return result.rule_focus

def is_part_location_value(part):
    if part.location:
        return part.location.is_value
    return False
    
def rule_id(rule_name):
    for part in rule_name.split('.'):
        if part not in ('DQC','US','IFRS'):
            return part
            
def severity(result):
    return {
        xbrl.xule.Severity.ERROR: xml.ErrorSeverity.ERROR,
        xbrl.xule.Severity.WARNING: xml.ErrorSeverity.WARNING,
        xbrl.xule.Severity.OK: xml.ErrorSeverity.INFO,
    }[result.severity]

def create_params(part):
    location = None
    if part.location:
        location = part.location.attribute if part.location.attribute else part.location.element
    deflocation = None
    if location and location.qname == xml.QName('element','http://www.w3.org/2001/XMLSchema'):
        deflocation = location
        location = None
        
    lines = str(part).split('\n')
    yield (xbrl.Error.Param(lines[0], location=location, deflocation=deflocation, quotes=False), part)
    for line in lines[1:]:
        if line:
            yield None
            yield (xbrl.Error.Param(line, location=location, deflocation=deflocation, quotes=False), part)
    
def create_child_error(params):
    return xbrl.Error.create(
        ''.join('{param%d%s}'%(i,':value' if is_part_location_value(part) else '')  for i, (param, part) in enumerate(params)),        
        **{f'param{i}': param for i, (param, part) in enumerate(params)},
        severity=xml.ErrorSeverity.OTHER
    )
    
def create_main_error(result, params, children):
    return xbrl.Error.create(
        '[{rule}] ' + ''.join('{param%d%s}'%(i,':value' if is_part_location_value(part) else '')  for i, (param, part) in enumerate(params)),        
        **{f'param{i}': param for i, (param, part) in enumerate(params)},
        rule=xbrl.Error.Param(result.effective_rule_name, external_url='https://xbrl.us/data-rule/dqc_%s/'%rule_id(result.rule_name), quotes=False),
        severity=severity(result),
        location=location(result),
        children=children
    )    

def create_error(result):
    lines = [[]]
    for part in result.value.parts:
        for param in create_params(part):
            if param is None:
                lines.append([])
            else:
                lines[-1].append(param)

    return create_main_error(result, lines[0], [create_child_error(line) for line in lines[1:]])
   
def create_catalog(dqcRepositoryPath):
    if dqcRepositoryPath is None:
        return xml.Catalog.root_catalog()
    return xml.Catalog.create_from_buffer("""<?xml version="1.0" encoding="UTF-8"?>
<catalog xmlns="urn:oasis:names:tc:entity:xmlns:xml:catalog">
	<rewriteURI uriStartString="https://github.com/DataQualityCommittee/dqc_us_rules/blob/v8/" rewritePrefix="{dqcRepositoryPath}"/>
	<rewriteURI uriStartString="https://raw.githubusercontent.com/DataQualityCommittee/dqc_us_rules/v8/" rewritePrefix="{dqcRepositoryPath}"/>
</catalog>
""".format(dqcRepositoryPath=dqcRepositoryPath).encode()).result

def setup_xule_processor(dqcRepositoryPath):
    catalog = create_catalog(dqcRepositoryPath)
    with open('https://github.com/DataQualityCommittee/dqc_us_rules/blob/v8/plugin/xule/rulesetMap.json?raw=true', catalog=catalog, mode='r') as f:
        ruleset_map = json.load(f)

    return xbrl.xule.Processor(ruleset_map, catalog=catalog)

def parse_suppress_errors(params):
    """Returns a list with suppressed error codes."""
    val = params.get('suppressErrors', None)
    if not val:
        return []
    return val.split('|')
    
def validate(instance, error_log, **params):
    """Performs additional validation checks using the given XULE rules."""
    error_log.report(xbrl.Error.create(
        'Verified {dqc} with {processor}',
        severity=xml.ErrorSeverity.INFO,
        location=instance,
        dqc=xbrl.Error.ExternalLinkParam('https://xbrl.us/data-quality/rules-guidance/', title='DQC v8 validation rules', quotes=False),
        processor=xbrl.Error.ExternalLinkParam('https://www.altova.com/raptorxml', title=ProductInfo.full_product_name, quotes=False)
    ))
    
    suppress_errors = parse_suppress_errors(params)
    xp = setup_xule_processor(params.get('dqcRepositoryPath', None)) #'file:///C:/Projects/trunk/test/dqc_us_rules-8.0.0/'))
    for result in xp.execute(instance):
        if not len(suppress_errors) or result.effective_rule_name in suppress_errors:
            error_log.report(create_error(result))

def on_xbrl_finished_dts(job, dts):
    pass

def on_xbrl_finished(job, instance):
    # instance object will be None if XBRL 2.1 validation was not successful.
    if instance:
        validate(instance, job.error_log, **job.script_params)