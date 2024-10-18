# workflow-based-information-management
A nimble, reusable, and dependable data infrastructure for organizations to be able to quickly execute research studies. Further abstraction is underway and will further simplify current scripts into smaller, more compact scripts.

## Setup and Installation

### Configuration
The program refers to the [configuration file](./scripts/config.yml) for all of its processes so that any changes demanded of the program (e.g., changing column names in the dataset, changing data pull method, etc) can be done through the configuration file or the scheduleR and without modifying the code. An example configuration file is included in the repository, though any project-specific details have been removed from the file.

### Installation of dependencies

#### Required Packages
Run the following code in R to install all of the packages required. Note that ***getwd() is only to be used as an argument if you cloned the repository and have the directory set as the project's directory***, otherwise specify where the **deps.yaml** file is located.
```
install.packages("automagic")
library(automagic)
install_deps_file(getwd())
```

#### Database drivers
If using the database accessioning workflow, a database driver will be needed. There are many database drivers to choose from, depending on the database engine used, organization requirements/restrictions, etc. Howerver, some notable ones are [FreeTDS]("https://www.freetds.org") and [Microsoft ODBC Driver 18 for SQL Server]("https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16") for Microsoft SQL Server users.

#### sFTP connections
If relying on sFTP connections for the data pull workflow, [Stenevang/sftp]("https://github.com/stenevang/sftp") has a package on Github to make those connections. Also, it's worth noting that on Macs the sftp module for cURL isn't built-in like it is on a Windows machine and would have to be installed prior. Andrew Berl has a great [guide]("https://andrewberls.com/blog/post/adding-sftp-support-to-curl") on how to do so

## Workflows
### 1. Data pulls
- Ingesting data from an sFTP: [sFTP scraper]("./scripts/arc_sftp_scraper.R")
- Pulling data from a Microsoft Teams environment: [Vitros ETL]("./scripts/cil_vitros_etl.R")

### 2. Quality control
- Fully abstracted quality control based on config entries: [Abstracted QC]("./scripts/helper_scripts/abs_qc.R")
- Adapted quality control script for a specific use case: [ARC manifest QC]("./scripts/helper_scripts/arc_manifest_qc.R")

### 3. Database management
- Streamlining the database connection process: [Db connect]("./scripts/helper_scripts/db_connect.R")
- Accessioning: [Red cross samples]("./scripts/arc_sample_mssql_accessioning.R"), [Vitalant test results]("./scripts/vtl_result_mssql_accessioning.R")
- Notification scripts for data stakeholders: [Db notify]("./scripts/helper_scripts/db_notification.R")
- Update records in a database based on key-pair values: [Db update]("./scripts/helper_scripts/update_mssql_db.R")
- Record removal: [Remove records](./scripts/helper_scripts/remov_records_mssql_db.R)
- Backups: [Db backup]("./scripts/helper_scripts/backup_db.R")

### 4. Reporting
- Examples of reporting scripts: [Report test results]("./scripts/report_test_results_v2.R"), [Donation data reporting]("./scripts/donation_report_prep.R")
