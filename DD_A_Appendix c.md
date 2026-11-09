OVMT_Appendix_A_New_Entries.docx[OVMT_Appendix_A_New_Entries.docx](sandbox:/workspace/scratch/42d3a13f3eec/OVMT_Appendix_A_New_Entries.docx) contains only the **15 newly added glossary entries**. AWS and PDP were excluded because they already appear in the original template.

**Appendix C explains the runtime inputs passed between services**, followed by the structure of the load-status data. These are actual code references, although the naming formulas use explanatory placeholders.

The five inputs originate in `infra/templates/nested/eventbridge-cfn.yaml`, lines 52–56:

| Input | Purpose | Where it is consumed |
|---|---|---|
| `source_database` | Identifies the ADLH source catalogue database | Step Functions passes it to Glue as `--source_database`; the script assigns it to `hub_database`. |
| `target_database` | Identifies the PDP destination catalogue database | Passed to Glue as `--target_database`; assigned to `spoke_database`. Also used in notifications and the reporting Lambda. |
| `spoke_path` | Specifies the destination S3 prefix | Passed to Glue for storage locations and summary output; passed to Lambda to locate the summary JSON. |
| `max_parallel` | Controls how many tables the script processes concurrently | Passed to Glue and converted to `MAX_PARALLEL` for `ThreadPoolExecutor`. |
| `SNStopic` | Identifies the notification topic ARN | Read by Step Functions when publishing start, completion and failure notifications. It is not passed to Glue. |

The relevant references are:

- **`statemachine-cfn.yaml`, lines 49–52:** passes the four Glue arguments.
- **`glue-job-cfn.yaml`, lines 77–91:** defines default values for those arguments. Execution-specific arguments supplied by Step Functions override these defaults.
- **`kg01-pdp-bronze-active-view-entire-db-multithreads.py`, lines 22–31:** resolves and assigns the arguments.
- **`statemachine-cfn.yaml`, lines 83–84:** passes the target database and S3 prefix to the reporting Lambda.
- **`table_summary_end_of_load.py`, lines 12–15:** reads those values and constructs the summary-object path.

The names `{hubEnv}`, `{hubChar}`, `{version}` and `{hubProjectId}` in Appendix C are **documentation placeholders**, not literal variable names in the code. They summarize the CloudFormation substitutions and SSM lookup used to construct the source database name.

The last paragraph describes **outputs**, rather than input variables: the columns in `table_load_status` and the fields in the summary JSON. Those structures are defined in the Glue script.

**I recommend removing Appendix C as a separate appendix and distributing its content into the relevant sections.** It is useful information, but its current placement partly repeats the component descriptions.

| Information | Recommended location |
|---|---|
| Source/target database names, S3 paths and naming formulas | Section 2.6, System Environment, under Cloud Environment |
| Five runtime inputs and how they pass between services | Scheduling and orchestration component, under a “Runtime inputs” subsection |
| Status-table columns and summary JSON fields | Load reporting and operational notifications component |

Section 2.6 is therefore appropriate for the **environment names and locations**, but the execution arguments and reporting structures fit more naturally alongside the components that use them.