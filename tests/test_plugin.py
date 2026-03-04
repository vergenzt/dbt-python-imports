import json
import os
import re
import sys
from pathlib import Path
from subprocess import check_call
from textwrap import dedent
from urllib.parse import urlparse

import psycopg2
import pytest
import tinypg
from pytest import fixture

DBT_PROFILES_DIR = Path(__file__).parent
DBT_PROFILE = "test_profile"
DBT_PROJECT_NAME = "test_" + re.sub(
    r"[^\w]", "_", os.getenv("ENV_NAME", DBT_PROFILES_DIR.parent.name).lower()
)
DBT_PROJECT_YML: str = json.dumps(
    {
        "name": DBT_PROJECT_NAME,
        "flags": {
            "send_anonymous_usage_stats": False,
        },
        "version": "0.0.1",
        "profile": DBT_PROFILE,
        "model-paths": ["models"],
        "target-path": "target",
    }
)


@fixture
def temp_postgres_db_url():
    with tinypg.database() as dburl_str:
        yield dburl_str


@fixture
def dbt_project(tmp_path_factory, temp_postgres_db_url, monkeypatch):
    dbt_project_path = tmp_path_factory.mktemp(DBT_PROJECT_NAME)
    dbt_project_path.joinpath("dbt_project.yml").write_text(DBT_PROJECT_YML)
    dbt_project_path.joinpath("models").mkdir()
    dbt_project_path.joinpath("target").mkdir()

    monkeypatch.setenv("DBT_PROFILES_DIR", str(DBT_PROFILES_DIR))
    monkeypatch.setenv("DBT_PROJECT_DIR", str(dbt_project_path))

    dburl = urlparse(temp_postgres_db_url)
    monkeypatch.setenv("DB_HOST", dburl.hostname or "")
    monkeypatch.setenv("DB_USERNAME", dburl.username or "")
    monkeypatch.setenv("DB_PASSWORD", dburl.password or "")
    monkeypatch.setenv("DB_PORT", str(dburl.port))
    monkeypatch.setenv("DB_NAME", dburl.path[1:] or "")
    monkeypatch.setenv("DB_SCHEMA", "public")
    monkeypatch.chdir(dbt_project_path)

    return dbt_project_path


def test_model_context(dbt_project: Path):
    model_path = dbt_project / "models" / "my_model.sql"
    model_path.write_text("""
    {%- set os_path = modules.import("os.path") %}
    select '{{ os_path.dirname("foo/bar/baz") }}'
    """)

    check_call([sys.executable, "-m", "dbt.cli.main", "compile"])

    model_compiled_path = (
        Path("target/compiled") / DBT_PROJECT_NAME / model_path.relative_to(dbt_project)
    )
    assert model_compiled_path.read_text().strip() == "select 'foo/bar'"


def test_properties_context(dbt_project: Path):
    model_path = dbt_project / "models" / "my_model.sql"
    model_path.write_text("select 1 as my_column")

    yml_path = dbt_project / "models" / "my_model.yml"
    yml_path.write_text(
        dedent(
            """
            version: 2
            models:
            - name: my_model
              description: |-
                {{ modules.import("calendar").TextCalendar().formatyear(2026) }}
            """
        )
    )

    check_call([sys.executable, "-m", "dbt.cli.main", "compile"])

    manifest_path = dbt_project / "target" / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    assert (
        manifest["nodes"][f"model.{DBT_PROJECT_NAME}.my_model"]["description"].strip()
        == """
                                  2026

      January                   February                   March
Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su
          1  2  3  4                         1                         1
 5  6  7  8  9 10 11       2  3  4  5  6  7  8       2  3  4  5  6  7  8
12 13 14 15 16 17 18       9 10 11 12 13 14 15       9 10 11 12 13 14 15
19 20 21 22 23 24 25      16 17 18 19 20 21 22      16 17 18 19 20 21 22
26 27 28 29 30 31         23 24 25 26 27 28         23 24 25 26 27 28 29
                                                    30 31

       April                      May                       June
Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su
       1  2  3  4  5                   1  2  3       1  2  3  4  5  6  7
 6  7  8  9 10 11 12       4  5  6  7  8  9 10       8  9 10 11 12 13 14
13 14 15 16 17 18 19      11 12 13 14 15 16 17      15 16 17 18 19 20 21
20 21 22 23 24 25 26      18 19 20 21 22 23 24      22 23 24 25 26 27 28
27 28 29 30               25 26 27 28 29 30 31      29 30

        July                     August                  September
Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su
       1  2  3  4  5                      1  2          1  2  3  4  5  6
 6  7  8  9 10 11 12       3  4  5  6  7  8  9       7  8  9 10 11 12 13
13 14 15 16 17 18 19      10 11 12 13 14 15 16      14 15 16 17 18 19 20
20 21 22 23 24 25 26      17 18 19 20 21 22 23      21 22 23 24 25 26 27
27 28 29 30 31            24 25 26 27 28 29 30      28 29 30
                          31

      October                   November                  December
Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su      Mo Tu We Th Fr Sa Su
          1  2  3  4                         1          1  2  3  4  5  6
 5  6  7  8  9 10 11       2  3  4  5  6  7  8       7  8  9 10 11 12 13
12 13 14 15 16 17 18       9 10 11 12 13 14 15      14 15 16 17 18 19 20
19 20 21 22 23 24 25      16 17 18 19 20 21 22      21 22 23 24 25 26 27
26 27 28 29 30 31         23 24 25 26 27 28 29      28 29 30 31
                          30
""".strip()
    )


def test_project_yml_var_context(dbt_project: Path, monkeypatch):
    monkeypatch.setenv("EXAMPLE_URL", "https://example.org:8080/foo/bar?baz")

    yml_path = dbt_project / "dbt_project.yml"
    yml_path.write_text(
        json.dumps(
            {
                **json.loads(yml_path.read_text()),
                "vars": {
                    DBT_PROJECT_NAME: {
                        "example_domain": "{{- modules.import('urllib.parse').urlparse(env_var('EXAMPLE_URL')).hostname }}"
                    },
                },
            }
        )
    )

    model_path = dbt_project / "models" / "my_model.sql"
    model_path.write_text("select '{{ var('example_domain') }}'")

    check_call([sys.executable, "-m", "dbt.cli.main", "compile"])

    model_compiled_path = (
        Path("target/compiled") / DBT_PROJECT_NAME / model_path.relative_to(dbt_project)
    )
    assert model_compiled_path.read_text().strip() == "select 'example.org'"


@pytest.mark.xfail(
    reason="dbt doesn't call set_up_plugin_manager until after config from dbt_project.yml is rendered"
)
def test_project_yml_config_context(dbt_project: Path, temp_postgres_db_url: str):
    yml_path = dbt_project / "dbt_project.yml"
    yml_path.write_text(
        json.dumps(
            {
                **json.loads(yml_path.read_text()),
                "models": {
                    DBT_PROJECT_NAME: {
                        "my_model": {
                            "schema": "{{ modules.import('subprocess').check_output(['echo', 'extra_schema'], text=True).strip() }}"
                        }
                    },
                },
            }
        )
    )

    model_path = dbt_project / "models" / "my_model.sql"
    model_path.write_text("select 1 as my_column")

    check_call([sys.executable, "-m", "dbt.cli.main", "run"])

    curs = psycopg2.connect(temp_postgres_db_url).cursor()
    curs.execute("select * from public_extra_schema.my_model")


@pytest.mark.xfail(
    reason="dbt doesn't call set_up_plugin_manager until after profiles.yml is rendered"
)
def test_profiles_yml_context(
    dbt_project: Path, temp_postgres_db_url: str, monkeypatch
):
    monkeypatch.undo()

    monkeypatch.setenv("DBT_PROJECT_DIR", str(dbt_project))
    monkeypatch.setenv("DBT_PROFILES_DIR", str(dbt_project))
    monkeypatch.setenv("DB_URI", temp_postgres_db_url + "?schema=my_schema")

    yml_path = dbt_project / "profiles.yml"
    yml_path.write_text(
        dedent(
            """
            test_profile:
              outputs:
                default:
                  type: postgres
                  host:     '{%- set parse = modules.import("urllib.parse") %}{{ parse.urlparse(env_var("DB_URI")).hostname }}'
                  user:     '{%- set parse = modules.import("urllib.parse") %}{{ parse.urlparse(env_var("DB_URI")).username }}'
                  password: '{%- set parse = modules.import("urllib.parse") %}{{ parse.urlparse(env_var("DB_URI")).password }}'
                  port:     '{%- set parse = modules.import("urllib.parse") %}{{ parse.urlparse(env_var("DB_URI")).port }}'
                  dbname:   '{%- set parse = modules.import("urllib.parse") %}{{ parse.urlparse(env_var("DB_URI")).path.split("/")[1] }}'
                  schema:   '{%- set parse = modules.import("urllib.parse") %}{{ parse.parse_qs(parse.urlparse(env_var("DB_URI")).query)["schema"] }}'
            """
        )
    )

    model_path = dbt_project / "models" / "my_model.sql"
    model_path.write_text("select 1 as my_column")

    check_call([sys.executable, "-m", "dbt.cli.main", "run"])
