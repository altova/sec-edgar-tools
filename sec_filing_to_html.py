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
__copyright__ = "Copyright 2015 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# This script generates HTML reports from a SEC EDGAR filing.
#
# Example invocation:
#   raptorxmlxbrl valxbrl --script=sec_filing_to_html.py nanonull.xbrl

import os, datetime, itertools, builtins
from altova import *

lang='en-US'

def isPeriodStart(role):
    return role in (
        'http://www.xbrl.org/2003/role/periodStartLabel',
        'http://xbrl.us/us-gaap/role/label/negatedPeriodStart',
        'http://www.xbrl.org/2009/role/negatedPeriodStartLabel'
    )
def isPeriodEnd(role):
    return role in (
        'http://www.xbrl.org/2003/role/periodEndLabel',
        'http://xbrl.us/us-gaap/role/label/negatedPeriodEnd',
        'http://www.xbrl.org/2009/role/negatedPeriodEndLabel'
    )
def isTotal(role):
    return role in (
        'http://www.xbrl.org/2003/role/totalLabel',
        'http://xbrl.us/us-gaap/role/label/negatedTotal',
        'http://www.xbrl.org/2009/role/negatedTotalLabel'
    )
def isNegated(role):
    return role in (
        'http://xbrl.us/us-gaap/role/label/negated',
        'http://www.xbrl.org/2009/role/negatedLabel',
        'http://www.xbrl.org/2009/role/negatedNetLabel',
        'http://xbrl.us/us-gaap/role/label/negatedPeriodEnd',
        'http://www.xbrl.org/2009/role/negatedPeriodEndLabel',
        'http://xbrl.us/us-gaap/role/label/negatedPeriodStart',
        'http://www.xbrl.org/2009/role/negatedPeriodStartLabel',
        'http://www.xbrl.org/2009/role/negatedTerseLabel',
        'http://xbrl.us/us-gaap/role/label/negatedTotal',
        'http://www.xbrl.org/2009/role/negatedTotalLabel'
    )

def domainMembersFromPresentationTreeRecursive(network,parent,domain_members):
    for rel in network.relationships_from(parent):
        domain_members.append(rel.target)
        domainMembersFromPresentationTreeRecursive(network,rel.target,domain_members)

def conceptsFromPresentationTreeRecursive(network,parent,concepts):
    for rel in network.relationships_from(parent):
        if not rel.target.abstract:
            concepts.append((rel.target,rel.preferred_label))
        conceptsFromPresentationTreeRecursive(network,rel.target,concepts)

def analyzePresentationTree(network,roots):
    concepts = []
    dimensions = {}
    for rel in network.relationships_from(roots[0]):
        if isinstance(rel.target,xbrl.xdt.Hypercube):
            for rel2 in network.relationships_from(rel.target):
                if isinstance(rel2.target,xbrl.xdt.Dimension):
                    domainMembersFromPresentationTreeRecursive(network,rel2.target,dimensions.setdefault(rel2.target,[]))
                else:
                    conceptsFromPresentationTreeRecursive(network,rel2.target,concepts)
        else:
            conceptsFromPresentationTreeRecursive(network,rel.target,concepts)
    return concepts, dimensions

def calcTableData(instance,role,contexts,concepts,dimensions):
    table = {'columns': [], 'height': len(concepts)}

    bIsCashFlow = 'cash' in role[1].lower() and 'flow' in role[1].lower()

    for context in contexts:
        cs = xbrl.ConstraintSet(context)
        period = cs[xbrl.Aspect.PERIOD]
        dimension_aspects = [value for aspect,value in cs.items() if isinstance(aspect,xbrl.xdt.Dimension)]
        bEliminate = False
        for val in dimension_aspects:
            domain = dimensions.get(val.dimension,None)
            if not domain or val.value not in domain:
                bEliminate = True
        for dim in set(dimensions.keys())-set([value.dimension for value in dimension_aspects]):
            if dim.default_member and dim.default_member not in dimensions[dim]:
                bEliminate = True
        if bEliminate:
            continue

        bEmpty = True
        bHasCash = False
        column = {'period': period, 'dimensions': dimension_aspects, 'rows': []}
        for concept in concepts:
            cs[xbrl.Aspect.CONCEPT] = concept[0]
            if isPeriodStart(concept[1]):
                if period.period_type == xbrl.PeriodType.START_END:
                    cs[xbrl.Aspect.PERIOD] = xbrl.PeriodAspectValue.from_instant(period.start)
                else:
                    column['rows'].append({'concept': concept, 'facts': xbrl.FactSet()})
                    continue
            elif isPeriodEnd(concept[1]):
                if period.period_type == xbrl.PeriodType.START_END:
                    cs[xbrl.Aspect.PERIOD] = xbrl.PeriodAspectValue.from_instant(period.end)
                else:
                    column['rows'].append({'concept': concept, 'facts': xbrl.FactSet()})
                    continue
            else:
                cs[xbrl.Aspect.PERIOD] = period

            facts = instance.facts.filter(cs,allow_additional_dimensions=False)
            if len(facts):
                bEmpty = False
                if bIsCashFlow and not bHasCash and concept[0].is_duration():
                    bHasCash = 'cash' in next(iter(concept[0].labels(label_role=concept[1],lang=lang))).text.lower()
            column['rows'].append({'concept': concept, 'facts': facts})

        if not bEmpty and (not bIsCashFlow or bHasCash):
            table['columns'].append(column)

    return table

def formatConcept(concept):
    preferredLabel = concept[1] if concept[1] else 'http://www.xbrl.org/2003/role/label'
    labels = list(concept[0].labels(label_role=preferredLabel,lang=lang))
    if labels:
        return labels[0].text
    return str(concept[0].qname)

def formatUnit(unit):
    numerators = list(unit.numerator_measures)
    denumerators = list(unit.denominator_measures)
    if len(numerators) == 1 and len(denumerators) == 0:
        if numerators[0] == xml.QName('USD','http://www.xbrl.org/2003/iso4217'):
            return '$'
        elif numerators[0] == xml.QName('EUR','http://www.xbrl.org/2003/iso4217'):
            return '€'
    numerator = ','.join([qname.local_name for qname in numerators])
    denominator = ','.join([qname.local_name for qname in denominators])
    if denominator:
        return numerator+'/'+denominator
    return numerator

def formatDimensionValue(dimValue):
    return formatConcept((dimValue.value,'http://www.xbrl.org/2003/role/terseLabel'))

def formatFact(fact,preferredLabel):
    if fact.xsi_nil:
        return 'nil'
    elif fact.concept.is_numeric():
        val = fact.effective_numeric_value
        if preferredLabel and 'negated' in preferredLabel:
            val *= -1
        if val < 0:
            return '(%s)' % str(abs(val))
        return str(val)
    elif fact.concept.is_fraction():
        return str(fact.fraction_value)
    else:
        return fact.normalized_value

def formatDate(date):
    return date.strftime('%b. %d, %Y')

def getDuration(column):
    p = column['period']
    if p.period_type == xbrl.PeriodType.INSTANT:
        return 0
    return (p.end.year - p.start.year) * 12 + p.end.month - p.start.month

def getEndDate(column):
    p = column['period']
    if p.period_type == xbrl.PeriodType.INSTANT:
        return p.instant
    return p.end

def generateTable(file, role, table):
    columns = sorted(table['columns'],key=lambda x: (-getDuration(x),getEndDate(x)),reverse=True)

    file.write('<hr/>\n')
    file.write('<a name="table_%s"/>\n' % role[1].split(' - ')[0])
    file.write('<table>\n')

    file.write('<caption>')
    file.write(role[1])
    file.write('</caption>\n')

    file.write('<thead>\n')

    bHasDurations = False
    for duration, group in itertools.groupby(columns,key=getDuration):
        if duration > 0:
            bHasDurations = True

    file.write('<tr>\n')
    file.write('<th rowspan="%d"></th>\n' % (2 if bHasDurations else 1))
    if bHasDurations:
        for duration, group in itertools.groupby(columns,key=getDuration):
            cols = list(group)
            file.write('<th colspan="%d">\n' % len(cols))
            if duration > 0:
                file.write('<p class="label">%d Months Ended</p>\n' % getDuration(cols[0]))
            file.write('</th>\n')
        file.write('</tr>\n')
        file.write('<tr>\n')
    for column in columns:
        file.write('<th>\n')
        file.write('<p class="label">%s</p>\n' % formatDate(getEndDate(column)-datetime.timedelta(days=1)))
        for dimValue in column['dimensions']:
            dimLabel = formatDimensionValue(dimValue)
            if '[Domain]' not in dimLabel:
                file.write('<p class="label">%s</p>\n' % dimLabel)
        file.write('</th>\n')
    file.write('</tr>\n')
    file.write('</thead>\n')

    file.write('<tbody>\n')
    footnotes = {}
    for row in range(table['height']):
        concept = columns[0]['rows'][row]['concept']
        file.write('<tr>\n')
        file.write('<th>%s</th>\n' % formatConcept(concept))
        for column in columns:
            file.write('<td>')
            for fact in column['rows'][row]['facts']:
                file.write('<p class="fact">%s' % formatFact(fact,concept[1]))
                for footnote in fact.footnotes(lang=lang):
                    index = footnotes.setdefault(footnote, len(footnotes)+1)
                    file.write('<a href="#table_%s_footnote_%d"><span class="footnoteRef">[%d]</span></a>' % (role[1].split(' - ')[0],index,index))
                file.write('</p>\n')
            file.write('</td>\n')
        file.write('</tr>\n')
    file.write('</tbody>\n')

    file.write('</table>\n')

    for (footnote,index) in sorted(footnotes.items(),key=lambda footnote: footnote[1]):
        file.write('<a name="table_%s_footnote_%d"><p class="footnote">[%d] %s</p></a>\n' % (role[1].split(' - ')[0],index,index,footnote.text))

def generateTables(file, dts, instance):
    file.write("""<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8"/>
<style type="text/css">
.error { color: red }
.footnoteRef { font-size: 70%; vertical-align: top;}
table { border-collapse:collapse; border: 0.22em solid black; background-color: white; color: black;}
caption {font-size: 150%}
td, th { border-left: 0.1em solid black; border-left: 0.1em solid black; border-top: 0.1em solid black; padding: 0.5em; text-align: center; }
thead tr th.rollup { border-top-style: none; }
tbody tr th.rollup { border-left-style: none; }
tbody tr:nth-of-type(even) { background-color: #EAEFFF; }
thead, tbody tr th { background-color: #C6D8FF; }
thead { border-bottom: 0.19em solid black; }
thead tr:first-of-type th:first-of-type, tbody tr th:last-of-type { border-right: 0.18em solid black; }
</style>
</head>
<body>
""")

    # Calculate table data
    tables = {}
    contexts = list(instance.contexts)
    roles = [(role, dts.role_type(role).definition.value) for role in dts.presentation_link_roles()]
    roles = sorted(roles, key=lambda role: role[1].split(' - ')[0])
    for role in roles:
        presentation_network = dts.presentation_base_set(role[0]).network_of_relationships()
        roots = list(presentation_network.roots)
        tables[role] = calcTableData(instance,role,contexts,*analyzePresentationTree(presentation_network,roots))

    # Generate table index
    for role in roles:
        if tables[role]['columns']:
            file.write('<h4><a href="#table_%s">%s</a></h4>\n' % (role[1].split(' - ')[0], role[1]))

    # Generate html rendering of each non-empty table
    for role in roles:
        if tables[role]['columns']:
            generateTable(file, role, tables[role])

# Main entry point, will be called by RaptorXML after the XBRL instance validation job has finished
def on_xbrl_finished(job, instance):
    # instance object will be None if XBRL 2.1 validation was not successful
    if instance:
        path = os.path.join(job.output_dir,'table.html')
        with builtins.open(path,mode='w',newline='') as file:
            generateTables(file, instance.dts, instance)
        # Register new output file with RaptorXML engine
        job.append_output_filename(path)
