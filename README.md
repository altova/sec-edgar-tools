# sec-edgar-tools

This example demonstrates how the [Python v2.1 API in RaptorXML+XBRL Server](http://manual.altova.com/RaptorXML/pyapiv2/html/) can be used to perform custom validation.

The `efm-validation.py` script performs the extra checks specified in the EDGAR Filer Manual (Volume II) EDGAR Filing (Version 31) (http://www.sec.gov/info/edgar/edmanuals.htm) in [RaptorXML+XBRL Server](http://www.altova.com/raptorxml.html).

The `sec_*.py` scripts demonstrate how to generate reports from a SEC EDGAR filing.

The following parameters can be additionally specified during invocation of `efm-validation.py`

Parameter 					| Description
---                         | ---
CIK							| The CIK of the registrant
submissionType				| The EDGAR submission type, e.g. `10-K`
cikList						| A list of CIKs separated with a comma `,`
cikNameList					| A list of official registrant names for each CIK in cikList separated by &vert;`Edgar`&vert;
forceUtrValidation			| Set to true to force-enable UTR validation
edbody-url					| The path to the `edbody.dtd` used to validate the embedded HTML fragments
edgar-taxonomies-url		| The path to the `edgartaxonomies.xml` which contains a list of taxonomy files that are allowed to be referenced from the company extension taxonomy


Example invocation:

```
raptorxmlxbrl valxbrl --script=efm-validation.py --script-param="CIK:1234567890" nanonull.xbrl
```
