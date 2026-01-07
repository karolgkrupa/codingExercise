# Design document

## Security features
First element of security is private networking. All the services used in this solution will use full private networking communication and will not be accessible from public internet.  
Second is network separation between environments, we don't want developers being able to copy data between environments.  
Third is Access Control, no developer should have access to change any of the resources, as they are created and managed by the IaaC. In addition to that we should have strict rules about what groups have access to what environment.

## Data access controls (i.e. RBAC).

Depending on what is the classification of the data, we might want to add UAT/preprod environment for Business users to test and validate changes. 
 
| Environment | Developers | Business users | Testers | Support           |
|-------------|------------|----------------|---------|-------------------|
| Dev         | Write      |                |         |                   |
| Test        | Read       | Read           | Write    | Write |
| Prod        |       | Read           |         | Conditional Write |

## Ways-of-working.
Developers write changes, write tests for it. Creation of PR to main triggers running of all tests, checks styling. After merge the solution is deployed on test. There it can be checked with testers/business users. During release window, it will be deployed to production with everything else on the main branch. 
## Process to deploy changes to production.
Releases every week on Wednesdays, assuming there are changes. Main gets deployed. Untested changes will not be enabled, unless they touch common files, then have to be reverted, or release postponed till next week.
## Alerting and monitoring.
Alerts on failed pipelines. Log analytics for the whole platform, getting relevant diagnostic settings.
## How data consumers interact with the platform.
Data consumers, based on the need, can use serverless warehouse (connected privately using NCC), interact with PowerBi or use tables in shared Catalog.
## Onboarding of new engineers to the platform.
Adding new engineers should be as easy as adding them to the relevant Entra groups. These have RBAC permissions assigned, as well as they are represented in Databricks Unity Catalog.
## Adding a new data source.
Adding new data source is a multi-step process:
1. Make firewall exception if needed. If the source is not part of already whitelisted network, this is required
2. Create pipeline in ADF that will pull the data
3. Write notebook that will transform the data
4. Make sure the notebook is triggered by the pipeline