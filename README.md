# sec-edgar-tools

This example demonstrates how the [Python v2.1 API in RaptorXML+XBRL Server](http://manual.altova.com/RaptorXML/pyapiv2/html/) can be used to perform custom validation.

##### efm_validation.py
The script performs the extra checks specified in the EDGAR Filer Manual (Volume II) EDGAR Filing (Version 31) (http://www.sec.gov/info/edgar/edmanuals.htm) in [RaptorXML+XBRL Server](http://www.altova.com/raptorxml.html).

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
enableDqcValidation | Enable DQC rule validation

###### Example invocations

Validate a single filing
```
  raptorxmlxbrl valxbrl --script=efm_validation.py instance.xml
```
Validate a single filing with additional options
```
  raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=CIK:1234567890 instance.xml
```
Validate a single filing using EFM and DQC rules
```
  raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=enableDqcValidation:true instance.xml
```

##### dqc_validation.py

This script implements additional data quality validation rules as specified by the [XBRL US Data Quality Committee] (https://xbrl.us/home/data-quality/rules-guidance/).
This script is designed to be used standalone or in conjunction with the EDGAR Filer Manual (EFM) rules implemented in script `efm_validation.py`. When using the efm_validation.py script, the DQC validation rules can be enabled with the `enableDqcValidation` option.

The following script parameters can be additionally specified:

paramerter | description
--- | ---
`suppressErrors` |                  A list of DQC.US.nnnn.mmm error codes separated by `|` characters.

###### Example invocations

Validate a single filing

```
  raptorxmlxbrl valxbrl --script=dqc_validation.py instance.xml
```

Suppress a specific error
```
  raptorxmlxbrl valxbrl --script=dqc_validation.py --script-param=suppressErrors:DQC.US.0004.16 instance.xml
```
Validate a single filing using EFM and DQC rules
```
  raptorxmlxbrl valxbrl --script=efm_validation.py --script-param=enableDqcValidation:true instance.xml
```

###### Using Altova RaptorXML+XBRL Server with XMLSpy client:

1.   do one of
  - Copy `efm_validation.py` and all `dqc_*` files to the Altova RaptorXML Server script directory `etc/scripts/sec-edgar-tools/` (default `C:\Program Files\Altova\RaptorXMLXBRLServer2016\etc\scripts\sec-edgar-tools\` on windows)
  - Edit the <server.script-root-dir> tag in Altova RaptorXML+XBRL server configuration file `etc/server_config.xml`
2.    Start Altova RaptorXML+XBRL server.
3.    Start Altova XMLSpy, open `Tools|Manage Raptor Servers...` and connect to the running server
4.    Create a new configuration and rename it to e.g. "DQC CHECKS"
5.    Select the XBRL Instance property page and then set the script property to `sec-edgar-tools/dqc_validation.py`
6.    Select the new "DQC CHECKS" configuration in `Tools|Raptor Servers and Configurations`
7.    Open a SEC instance file
8.    Validate instance file with `XML|Validate XML on Server (Ctrl+F8)`
