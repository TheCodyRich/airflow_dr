from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

from include.helpers import (
    _check_liveliness,
    _retrieve_all_dags,
    _reconcile_dag_runs,
    _update_in_failover,
    _update_is_paused_in_primary,
    _toggle_dr_dags
)


with DAG(
    dag_id="backup",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=10)
) as dag:

    # First, check if it is down. If so, immediately kickoff all DAGs in the existing instance that were on
    check_liveliness = BranchPythonOperator(
        task_id="check_liveliness",
        python_callable=_check_liveliness,
        op_kwargs={
            "organization_id": "{{ var.value.ASTRO_ORGANIZATION_ID }}",
            "deployment_id": "{{ var.value.PRIMARY_DEPLOYMENT_ID }}",
            "api_token": "{{ var.value.PRIMARY_ASTRO_API_TOKEN }}",
        }
    )

    # This is always going to be the first Task run in parallel with `check_liveliness`. It pulls a list of the DR
    # DAGs and turns them off if their corresponding DAGs are running in the primary Deployment
    pull_list_of_dr_dags = PythonOperator(
        task_id="pull_list_of_dr_dags",
        python_callable=_retrieve_all_dags,
        op_kwargs={
            "api_token": "{{ var.value.DISASTER_RECOVERY_ASTRO_API_TOKEN }}",
            "base_url": "{{ var.value.DISASTER_RECOVERY_BASE_URL }}",
        }
    )

    primary_is_alive = EmptyOperator(
        task_id="GOOD__primary_is_alive",
    )

    primary_is_down = EmptyOperator(
        task_id="BAD__primary_is_down",
    )

    check_liveliness >> [primary_is_alive, primary_is_down]

    # PATH A (Failure)
    update_in_failover_true = PythonOperator(
        task_id="update_in_failover_true",
        python_callable=_update_in_failover,
        op_kwargs={
            "in_failover": True,
        }
    )

    primary_is_down >> update_in_failover_true

    # Turn on the DAGs
    turn_on_dags_in_dr = PythonOperator.partial(
        task_id="turn_on_dags_in_dr",
        python_callable=_toggle_dr_dags,
    ).expand(
        op_kwargs=pull_list_of_dr_dags.output.map(
            lambda dag_response: {
                "dag_response": dag_response,
                "api_token_name": "DISASTER_RECOVERY_ASTRO_API_TOKEN",
                "base_url_name": "DISASTER_RECOVERY_BASE_URL",
                "in_failover": True,
            }
        ),
    )

    pull_list_of_dr_dags >> turn_on_dags_in_dr
    update_in_failover_true >> turn_on_dags_in_dr


    # PATH B (No Failure)
    update_in_failover_false = PythonOperator(
        task_id="update_in_failover_false",
        python_callable=_update_in_failover,
        op_kwargs={
            "in_failover": False,
        }
    )

    primary_is_alive >> update_in_failover_false

    pull_list_of_primary_dags = PythonOperator(
        task_id="pull_list_of_primary_dags",
        python_callable=_retrieve_all_dags,
        op_kwargs={
            "api_token": "{{ var.value.PRIMARY_ASTRO_API_TOKEN }}",
            "base_url": "{{ var.value.PRIMARY_BASE_URL }}",
        }
    )

    primary_is_alive >> pull_list_of_primary_dags

    # Turn off DAGs
    turn_off_dags_in_dr = PythonOperator.partial(
        task_id="turn_off_dags_in_dr",
        python_callable=_toggle_dr_dags,
    ).expand(
        op_kwargs=pull_list_of_dr_dags.output.map(  # Assumption is that primary DAGs == DR DAGs
            lambda dag_response: {
                "dag_response": dag_response,
                "api_token_name": "DISASTER_RECOVERY_ASTRO_API_TOKEN",
                "base_url_name": "DISASTER_RECOVERY_BASE_URL",
                "failover": False,
            }
        ),
    )

    primary_is_alive >> turn_off_dags_in_dr

    # TODO: Update DAG runs in primary

    update_is_paused_in_primary = PythonOperator.partial(
        task_id="update_is_paused_in_primary",
        python_callable=_update_is_paused_in_primary,
    ).expand(
        op_kwargs=pull_list_of_primary_dags.output.map(
            lambda dag_response: {"dag_response": dag_response}
        ),
    )

    pull_list_of_primary_dags >> update_is_paused_in_primary

    reconcile_dag_runs = PythonOperator.partial(
        task_id="reconcile_dag_runs",
        python_callable=_reconcile_dag_runs,
    ).expand(
        op_kwargs=pull_list_of_primary_dags.output.map(
            lambda dag_response: {
                "dag_response": dag_response,
                "primary_api_token_name": "PRIMARY_ASTRO_API_TOKEN",  # "{{ var.value.PRIMARY_ASTRO_API_TOKEN }}",
                "disaster_recovery_api_token_name": "DISASTER_RECOVERY_ASTRO_API_TOKEN",  # "{{ var.value.DISASTER_RECOVERY_ASTRO_API_TOKEN }}",
                "primary_base_url_name": "PRIMARY_BASE_URL",  # "{{ var.value.PRIMARY_BASE_URL }}",
                "disaster_recovery_base_url_name": "DISASTER_RECOVERY_BASE_URL",  # "{{ var.value.DISASTER_RECOVERY_BASE_URL }}",
                "dag_run_id_prefix": "dr_created"
            }
        ),
    )

    pull_list_of_primary_dags >> reconcile_dag_runs
