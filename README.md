# Overview
This is a WIP syncing tool for [classy](https://github.com/Pjt727/classy.git).
It provides a simple interface to sync data from **classy** into the data store of your choice.

# Supported Datastores
Each datastore may support several granularity options for getting data from classy:
- **all** - gets every exposed data point at each sync
- **school** - choose which school(s) to sync
- **term** -  choose which term(s) to sync
  
| Datastore | all | school | term |
|-----------|:----------:|:------:|:----:|
| sqlite    |     ✅      |   ❌    |  ❌   |
