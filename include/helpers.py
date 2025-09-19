"""
helpers.py

Author: Jake Roach
Date: 2025-09-16
"""

from airflow.sdk import Variable, task
from airflow.exceptions import AirflowSkipException

from datetime import datetime
from urllib.parse import quote

import requests


def _check_liveliness(organization_id: str, deployment_id: str, api_token, **context):
    """
    _check_liveliness

    Checks that a Deployment is in a HEALTHY state. Branches based on the response:

        - If HEALTHY, then pull a list of DAGs from the primary Deployment and update their state in DR.
        - Otherwise, wait for the list of DR DAGs to be retrieved.
    """
    response = requests.get(
        url=f"https://api.astronomer.io/platform/v1beta1/organizations/{organization_id}/deployments/{deployment_id}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}"
        }
    )

    return "pull_list_of_primary_dags" if response.json().get("status") == "HEALTHY" else "wait_for_pull_list_of_dr_dags"


def _retrieve_all_dags(api_token: str, base_url: str):
    """
    _retrieve_all_dags

    Pulls a list of all DAGs from a Deployment.
    """
    response: requests.Response = requests.get(
        url=f"{base_url}/api/v2/dags",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        },
    )

    dags = response.json().get("dags")

    if isinstance(dags, list):
        return dags

    raise Exception(f"Unexpected response type. `dags` is `{dags}` when it should be a `list`.")


def _update_is_paused_in_primary(dag_response, **context):
    """
    _update_is_paused_in_primary

    Updates a Variable in the DR Deployment for a specific DAG. If that DAG is active in the primary Deployment, then
    <dag_id>_is_paused is set to False. If that DAG is NOT active in the primary Deployment, then <dag_id>_is_paused is
    set to True.

    If the variable does not already exist, then it will be set with this operation.
    """
    dag_id = dag_response.get("dag_id")
    is_paused = dag_response.get("is_paused")

    if "backup" in dag_id:
        raise AirflowSkipException(f"Will not perform operations for DR dag with ID: {dag_id}")

    print(f"{dag_id} is paused: {is_paused}")

    Variable.set(f"{dag_id}_is_paused_in_primary", str(is_paused))


def __flip_dag_switch(dag_id: str, api_token: str, base_url: str, is_paused: bool):
    """
    __flip_dag_switch

    Pauses or unpauses a DAG.
    """
    return requests.patch(
        url=f"{base_url}/api/v2/dags/{dag_id}",
        json={
            "is_paused": is_paused
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        }
    )

def _toggle_dr_dags(dag_response,  api_token_name: str, base_url_name: str, failover=False, **context):
    """
    _toggle_dr_dags

    Two scenarios:

        1. Primary Deployment is NOT healthy.
              - Check to see if a DAG should be on in DR via the <dag_id>_is_paused Variable.
              - If it should be on, then turn it on.

        2. Primary Deployment IS healthy.
              - If there is a DAG on in DR, then it off.
    """
    dag_id = dag_response.get("dag_id")

    if "backup" in dag_id:
        raise AirflowSkipException(f"Will not perform operations for DR dag with ID: {dag_id}")

    is_paused_in_primary = True if Variable.get(f"{dag_id}_is_paused_in_primary") == "True" else False
    is_paused_in_dr = dag_response.get("is_paused")

    print(f"failover: {failover}")
    print(f"is_paused_in_dr: {is_paused_in_dr}")

    # Only start if it SHOULD be started and is already paused
    if (is_paused_in_dr and failover) and not is_paused_in_primary:
        _ = __flip_dag_switch(
            dag_id=dag_id,
            api_token=Variable.get(api_token_name),
            base_url=Variable.get(base_url_name),
            is_paused=False
        )

    elif not failover and not is_paused_in_dr:
        _ = __flip_dag_switch(
            dag_id=dag_id,
            api_token=Variable.get(api_token_name),
            base_url=Variable.get(base_url_name),
            is_paused=True
        )


def __retrieve_dag_runs(dag_id: str, api_token: str, base_url: str):
    """
    __retrieve_dag_runs


    """
    if "backup" in dag_id:
        raise AirflowSkipException(f"Will not perform operations for DR dag with ID: {dag_id}")

    try:
        last_logical_date = Variable.get(f"{dag_id}_last_logical_date")
        last_logical_date = last_logical_date if not None else "2025-01-01T00:00:00"
    except Exception as _:
        # If the Variable is NOT available, then an exception will be thrown. If that's the case, we'll use 2025-01-01
        # as the `last_logical_date`
        last_logical_date = "2025-01-01T00:00:00"

    print(f"BASE_URL: {base_url}")

    response = requests.get(
        url=f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
        json={
            "logical_date_gte": last_logical_date,
        },
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        }
    ).json()

    return response.get("dag_runs", [])


def __update_dag_run(dag_id, dag_run, api_token: str, base_url: str):
    """
    _update_dag_run

    Helper that only creates a new DAG run in the DR Deployment, then update the status to match what is in the primary
    Deployment.
    """
    dag_run_id = dag_run.get("dag_run_id")
    dag_run_id__scheduled = dag_run_id.split('scheduled__')[-1]
    dag_run_state = dag_run.get("state")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}",
    }

    # Create a DAG run
    _ = requests.post(
        url=f"{base_url}/api/v2/dags/{dag_id}/dagRuns",
        json={
            "dag_run_id": f"dr_created__{dag_run_id__scheduled}",
            "data_interval_start": dag_run.get("data_interval_start"),
            "data_interval_end": dag_run.get("data_interval_end"),
            "logical_date": dag_run.get("logical_date"),
            "run_after": "2030-01-01T00:00:00",
            "note": "DAG run created by DR DAG."
        },
        headers=headers,
    ).json()

    _ = requests.patch(
        url=f"{base_url}/api/v2/dags/{dag_id}/dagRuns/dr_created__{quote(dag_run_id__scheduled)}",
        json={
            "state": dag_run_state,
            "note": "State updated by DR DAG."
        },
        headers=headers,
    )


def _update_dag_runs(
    dag_response,
    primary_api_token_name: str,
    disaster_recovery_api_token_name: str,
    primary_base_url_name: str,
    disaster_recovery_base_url_name: str,
    **context
):
    """
    _update_dag_runs

    Retrieve DAG runs from primary AND upsert those DAG runs in DR. This is used to keep DR in-sync with Primary.
    """
    dag_id: str = dag_response.get("dag_id")

    if "backup" in dag_id:
        raise AirflowSkipException(f"Will not perform operations for DR dag with ID: {dag_id}")

    dag_runs = __retrieve_dag_runs(
        dag_id,
        Variable.get(primary_api_token_name),
        Variable.get(primary_base_url_name)
    )
    logical_dates = []

    for dag_run in dag_runs:
        __update_dag_run(
            dag_id=dag_id,
            dag_run=dag_run,
            api_token=Variable.get(disaster_recovery_api_token_name),
            base_url=Variable.get(disaster_recovery_base_url_name)
        )
        logical_dates.append(datetime.strptime(dag_run.get("logical_date"), "%Y-%m-%dT%H:%M:%S.%fZ"))

    Variable.set(
        key=f"{dag_id}_last_logical_date",
        value=max(logical_dates).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )

