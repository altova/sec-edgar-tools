# Copyright 2015 Altova GmbH
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#	  http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
__copyright__ = "Copyright 2015 Altova GmbH"
__license__ = 'http://www.apache.org/licenses/LICENSE-2.0'

# This script generates Excel reports from a SEC EDGAR filing.
# NOTE: You must first download the source code of the 3rd party Python module xlsxwriter from https://pypi.python.org/pypi/XlsxWriter
# and extract the xslxwriter folder in the archive to the lib/python3.4 subfolder of the RaptorXML server installation directory.
#
# Example invocation:
#	raptorxmlxbrl valxbrl --script=sec_filing_to_xlsx.py nanonull.xbrl

import os, datetime, itertools
from altova import *

try:
	import xlsxwriter
except:
	raise ImportError('Please install the 3rd party python module xlsxwrite from https://pypi.python.org/pypi/XlsxWriter')

lang='en-US'

formats = {}

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
		
def dimensionsFromPresentationTree(network,roots):
	dimensions = {}
	table = next(network.relationships_from(roots[0])).target
	for rel in network.relationships_from(table):
		if isinstance(rel.target,xbrl.xdt.Dimension):
			domainMembersFromPresentationTreeRecursive(network,rel.target,dimensions.setdefault(rel.target,[]))		
	return dimensions

def conceptsFromPresentationTreeRecursive(network,parent,concepts):
	for rel in network.relationships_from(parent):
		if not rel.target.abstract:
			concepts.append((rel.target,rel.preferred_label))
		conceptsFromPresentationTreeRecursive(network,rel.target,concepts)

def conceptsFromPresentationTree(network,roots):
	concepts = []
	table = next(network.relationships_from(roots[0])).target
	for rel in network.relationships_from(table):
		if isinstance(rel.target,xbrl.taxonomy.Item) and not isinstance(rel.target,xbrl.xdt.Dimension) and not isinstance(rel.target,xbrl.xdt.Hypercube):
			conceptsFromPresentationTreeRecursive(network,rel.target,concepts)
	return concepts

def calcTableData(instance,role,concepts,contexts,dimensions):
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

def formatPeriod(period):
	if period.period_type == xbrl.PeriodType.INSTANT:
		return period.instant.strftime('%d. %B %Y')
	elif period.period_type == xbrl.PeriodType.START_END:
		return '%s to %s' % (period.start.strftime('%d. %B %Y'), period.end.strftime('%d. %B %Y'))
	elif period.period_type == xbrl.PeriodType.FOREVER:
		return 'Forever'
	return ''

def formatDimensionValue(dimValue):
	return formatConcept((dimValue.value,'http://www.xbrl.org/2003/role/terseLabel'))
	
def formatFact(dts,fact,preferredLabel=None):
	if fact.xsi_nil:
		return ('#N/A',None)
	elif fact.concept.is_numeric():
		if fact.concept.is_fraction():
			val = fact.effective_fraction_value
		else:
			val = fact.effective_numeric_value
		if isNegated(preferredLabel):
			val *= -1
		if fact.concept.is_monetary():
			if isTotal(preferredLabel):
				return (val,formats['monetary_total'])
			return (val,formats['monetary'])
		return (val,None)
	elif fact.concept.is_qname():
		concept = dts.resolve_concept(fact.qname_value)
		if concept:
			for label in concept.labels():
				return (label.text,None)
		return (str(fact.qname_value),None)
	else:
		return (fact.normalized_value,None)
		
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
	
def generateTable(workbook, dts, role, table):
	columns = sorted(table['columns'],key=lambda x: (-getDuration(x),getEndDate(x)),reverse=True)
	
	worksheet = workbook.add_worksheet(role[1].split(' - ')[0])

	worksheet.set_column(0,0,70)
	worksheet.set_column(1,1+len(table['columns']),20)
	worksheet.write(0,0,role[1].split(' - ')[2],formats['caption'])
	
	col = 1
	row_start = 1
	for duration, group in itertools.groupby(columns,key=getDuration):
		cols = list(group)
		if duration > 0:
			if len(cols) > 1:
				worksheet.merge_range(0,col,0,col+len(cols)-1,'%d Months Ended' % getDuration(cols[0]),formats['center'])
			else:
				worksheet.write(0,col,'%d Months Ended' % getDuration(cols[0]),formats['center'])
			row = 1
		else:
			row = 0
		for column in cols:
			worksheet.write(row,col,getEndDate(column)-datetime.timedelta(days=1),formats['date'])
			for i, dimValue in enumerate(column['dimensions']):
				dimLabel = formatDimensionValue(dimValue)
				if '[Domain]' not in dimLabel:
					worksheet.write(row+1+i,col,dimLabel)
			col += 1
			row_start = max(row_start,row+2+len(column['dimensions']))
	
	for row in range(table['height']):
		concept = columns[0]['rows'][row]['concept']
		worksheet.write(row_start+row,0,formatConcept(concept),formats['header'])
		for col, column in enumerate(columns):
			for fact in column['rows'][row]['facts']:
				worksheet.write(row_start+row,1+col,*formatFact(dts,fact,concept[1]))
				footnotes = [footnote.text for footnote in fact.footnotes(lang=lang)]
				if footnotes:
					worksheet.write_comment(row_start+row,1+col,'\n'.join(footnotes),{'x_scale':5,'y_scale':2})	

def generateTables(path, dts, instance):
	global formats

	workbook = xlsxwriter.Workbook(path)

	formats['center'] = workbook.add_format({'align':'center'})
	formats['caption'] = workbook.add_format({'text_wrap':True,'bold':True})
	formats['header'] = workbook.add_format({'text_wrap':True})
	formats['date'] = workbook.add_format({'num_format':'mmm. d, yyyy','bold':True})
	formats['monetary'] = workbook.add_format({'num_format': '#,##0_);[Red](#,##0)'})
	formats['monetary_total'] = workbook.add_format({'num_format': '#,##0_);[Red](#,##0)', 'underline':33})

	# Calculate table data
	tables = {}
	contexts = list(instance.contexts)
	roles = [(role, dts.role_type(role).definition.value) for role in dts.presentation_link_roles()]
	roles = sorted(roles, key=lambda role: role[1].split(' - ')[0])
	for role in roles:
		presentation_network = dts.presentation_base_set(role[0]).network_of_relationships()
		roots = list(presentation_network.roots)
		tables[role] = calcTableData(instance,role,conceptsFromPresentationTree(presentation_network,roots),contexts,dimensionsFromPresentationTree(presentation_network,roots))
	
	# Generate excel sheet for each non-empty table
	for role in roles:
		if tables[role]['columns']:
			generateTable(workbook, dts, role, tables[role])

	workbook.close()

# Main entry point, will be called by RaptorXML after the XBRL instance validation job has finished
def on_xbrl_finished(job, instance):
	# instance object will be None if XBRL 2.1 validation was not successful
	if instance:
		path = os.path.join(job.output_dir,'table.xlsx')
		generateTables(path, instance.dts, instance)
		# Register new output file with RaptorXML engine
		job.append_output_filename(path)