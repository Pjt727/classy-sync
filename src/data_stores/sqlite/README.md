# Overview
- sqlite is file based database
- connect to it using the file path
- mutating any of the tables made by these migrations will result in undefined behavior
- relying on rows with enforced FK relationships will also produce undefined behavior
- this data should be treated as an source of truth you have no control over
## constraints
- there is limited support for going in between granularity level
- 
