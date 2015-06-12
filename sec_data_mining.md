
# SEC filing analysis with RaptorXML

Starting with version 2015r3 [RaptorXML](http://www.altova.com/raptorxml.html) features a new improved and more powerful Python API. The Python API allows the user to access the complete data model of the XBRL instance and DTS.
This brief primer will demonstrate how to use the new API to retrieve the reported value for 'Net Income' from SEC filings.

To retrieve all facts that are contained in the instance use the `facts` property of [`xbrl.Instance`](http://manual.altova.com/RaptorXML/pyapiv2/html/xbrl.Instance.html):

```python
# Iterate over all facts (items and tuples)
for fact in instance.facts:
	print('Fact {0} in context {1} has value {2}'.format(fact.qname, fact.context.id, fact.normalized_value))
```

`xbrl.Instance.facts` returns an [`xbrl.FactSet`](http://manual.altova.com/RaptorXML/pyapiv2/html/xbrl.FactSet.html) object which supports iteration, indexing, set operations and more advanced filtering.
E.g. to print out only the first 10 facts can be easily done with slices:

```python
# Iterate over the first 10 facts
for fact in instance.facts[:10]:
	print('Fact {0} in context {1} has value {2}'.format(fact.qname, fact.context.id, fact.normalized_value))
```

To retrieve only facts for a particular concept, use the `xbrl.FactSet.filter()` method with an [`xml.QName`](http://manual.altova.com/RaptorXML/pyapiv2/html/xml.QName.html). E.g. to print out all net income facts:

```python
# Create a qname for the us-gaap:NetIncomeLoss concept
qname_NetIncomeLoss = xml.QName('NetIncomeLoss', 'http://fasb.org/us-gaap/2013-01-31')
# Iterate only over all us-gaap:NetIncomeLoss facts
for fact in instance.facts.filter(qname_NetIncomeLoss):
	print('Fact {0} in context {1} has value {2}'.format(fact.qname, fact.context.id, fact.normalized_value))
```

Three things should be noted here. First, the US-GAAP taxonomy namespace is hardcoded in the code above.
The SEC regularly updates and publishes new versions of the US-GAAP taxonomy. Each new version has a distinct target namespace.
Thus, the actual us-gaap namespace can vary from filing to filing depending on the version of the US-GAAP taxonomy used.
One way to automatically determine the correct us-gaap namespace can be achieved by going over all taxonomy schemas in the DTS and analysing their target namespace URLs.

```python
# Create a regular expression to match different us-gaap namespace versions
re_us_gaap = re.compile('^http://[^/]+/us-gaap/')
# Iterate over all taxonomy schemas in the DTS
for taxonomy in instance.dts.taxonomy_schemas:
	# Check if the taxonomy schema's target namespace is an us-gaap namespace
	if re_us_gaap.match(taxonomy.target_namespace):
		# If yes, store it in the variable 'us_gaap'
		us_gaap = taxonomy.target_namespace
```

Using a Python generator expression this can be rewritten as an one-liner:

```python
	us_gaap = next(taxonomy.target_namespace for taxonomy in instance.dts.taxonomy_schemas if re_us_gaap.match(taxonomy.target_namespace))
```

Second, depending on the document type, filings may contain additional facts from other reporting periods.
The script above will simply output all us-gaap:NetIncomeLoss facts that are present in the instance.
To find the us-gaap:NetIncomeLoss fact for the current reporting period we need to additionally filter by the *required context*.
All SEC filings must have a required context which represents the main reporting period of that filing.
One simple approach to determine the required context is to find the mandatory dei:DocumentType fact which by definition must be reported in the required context.
	
```python
# First we determine the dei namespace (used for reporting Document Information elements) using the same method as the us-gaap namespace:
re_dei = re.compile('^http://xbrl.us/dei/|^http://xbrl.sec.gov/dei/')
dei = next(taxonomy.target_namespace for taxonomy in instance.dts.taxonomy_schemas if re_dei.match(taxonomy.target_namespace))

# Create a qname for the dei:DocumentType concept
qname_DocumentType = xml.QName('DocumentType', dei)
# Find the first dei:DocumentType fact
documentType = instance.facts.filter(qname_DocumentType)[0]
# Set required_context to be the XBRL context with which dei:DocumentType was reported
required_context = documentType.context
```

Now we can use the required_context in the `xbrl.FactSet.filter()` method to additionally restrict the returned facts by qname and by XBRL context.

```python
# Iterate only over us-gaap:NetIncomeLoss facts in the required context
for fact in instance.facts.filter(qname_NetIncomeLoss, required_context):
	print('Fact {0} in context {1} has value {2}'.format(fact.qname, fact.context.id, fact.normalized_value))
```

Finally, numeric XBRL facts can be specified with a certain precision using the `@decimals` and `@precision` attributes. To retrieve the actual fact value after taking the numeric precision into account, use the `xbrl.Item.effective_numeric_value` property.

```python
# Iterate only over us-gaap:NetIncomeLoss facts in the required context
for fact in instance.facts.filter(qname_NetIncomeLoss, required_context):
	print('Fact {0} in context {1} has effective value {2}'.format(fact.qname, fact.context.id, fact.effective_numeric_value))
```

The complete script now looks as following:

```python
# Main entry point, will be called by RaptorXML after the XBRL instance validation job has finished
def on_xbrl_finished(job, instance):
	
  # instance object will be None if XBRL 2.1 validation was not successful
  if instance:

    # Determine dei namespace used in the filing
    re_dei = re.compile('^http://xbrl.us/dei/|^http://xbrl.sec.gov/dei/')
    dei = next(taxonomy.target_namespace for taxonomy in instance.dts.taxonomy_schemas if re_dei.match(taxonomy.target_namespace))
    # Determine us-gaap namespace used in the filing
    re_us_gaap = re.compile('^http://[^/]+/us-gaap/')
    us_gaap = next(taxonomy.target_namespace for taxonomy in instance.dts.taxonomy_schemas if re_us_gaap.match(taxonomy.target_namespace))
  
    # Find the mandatory dei:DocumentType fact
    qname_DocumentType = xml.QName('DocumentType', dei)
    documentType = instance.facts.filter(qname_DocumentType)[0]
    # Determine the required context from the dei:DocumentType fact
    required_context = documentType.context
  	
    # Print us-gaap:NetIncomeLoss facts that are reported with the required context
    qname_NetIncomeLoss = xml.QName('NetIncomeLoss', us_gaap)
    for fact in instance.facts.filter(qname_NetIncomeLoss, required_context):
      print('Fact {0} in context {1} has effective value {2}'.format(fact.qname, fact.context.id, fact.effective_numeric_value))
```
