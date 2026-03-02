# dbt-python-imports

This is a [dbt plugin][dbt-plugin] to allow importing arbitrary Python modules from Jinja templates.

[dbt-plugin]: https://github.com/dbt-labs/dbt-core/blob/fa96acb15f79ae4f10b1d78f311f5ef2f4ed645e/core/dbt/plugins/manager.py

## Usage

1. Install `dbt-python-imports` via PyPI into the same Python environment as dbt itself.

2. Call `modules.import("<module_name>")` to import the given module from a dbt Jinja string.

   Examples:

   * [`modules.import("os.path").dirname`](https://docs.python.org/3/library/os.path.html#os.path.dirname)
   * [`modules.import("requests").get`](https://docs.python-requests.org/en/latest/api/#requests.get) (already a dependency of dbt-core)

3. Profit!



> [!warning]
> **This allows arbitrary code execution.** (That's kindof the point.)
>
> Jinja's sandboxing is not foolproof (e.g. some adapters allow arbitrary reads/writes to disk), so you already shouldn't be running untrusted dbt code/packages. However the limited Jinja context available by default does currently make it *harder* to run arbitrary code.
>
> Make sure you're not installing any packages (dbt or Python) whose source you haven't inspected.



### Available dbt Jinja Contexts

| Context                                                      | Available? |
| ------------------------------------------------------------ | ---------- |
| [model & hook SQL](https://docs.getdbt.com/reference/dbt-jinja-functions-context-variables) | ✅          |
| [model properties.yml](https://docs.getdbt.com/reference/dbt-jinja-functions/dbt-properties-yml-context) | ✅          |
| [dbt_project.yml](https://docs.getdbt.com/reference/dbt-jinja-functions/dbt-project-yml-context) | ✅          |
| [profiles.yml](https://docs.getdbt.com/reference/dbt-jinja-functions/profiles-yml-context) | ❌          |

See [unit tests](tests/test_plugin.py) for more details on context availability.



## Example

```console
# download the artifacts from your latest Databricks dbt job run

$ echo '
{%- macro fetch_dbt_artifacts(job_name='dbt build', extract_to=flags.TARGET_PATH ~ '/remote-state') %}
  {#- https://databricks-sdk-py.readthedocs.io/en/latest/workspace/jobs/jobs.html #}
  {%- set jobs_api = adapter.config.credentials.authenticate().api_client.jobs %}
  {%- set job = jobs_api.list(name=job_name, limit=1) | first %}
  {%- set job_run = jobs_api.list_runs(job_id=job["job_id"], limit=1, completed_only=true, expand_tasks=true) | first %}
  {%- set task_run = job_run["tasks"] | first %}
  {%- set dbt_output = jobs_api.get_run_output(run_id=task_run["run_id"]).dbt_output %}
  {%- set download_url = dbt_output.artifacts_link %}

  {%- set system = modules.import("dbt_common.clients.system") %}
  {%- set tar_path = extract_to ~ '/dbt-artifacts.tar.gz' %}
  {%- do system.download(download_url, tar_path) %}
  {%- do system.untar_package(tar_path, extract_to) %}
{%- endmacro %}
' > macros/fetch_dbt_artifacts.sql

$ dbt run-operation fetch_dbt_artifacts

```
