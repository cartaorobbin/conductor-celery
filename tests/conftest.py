import json

import pytest


@pytest.fixture
def task_poll_response():
    def inner(input_data):
        response = json.dumps(
            {
                "callback_after_seconds": 0,
                "callback_from_worker": True,
                "correlation_id": None,
                "domain": None,
                "end_time": 0,
                "executed": False,
                "execution_name_space": None,
                "external_input_payload_storage_path": None,
                "external_output_payload_storage_path": None,
                "inputData": input_data,
                "isolation_group_id": None,
                "iteration": 0,
                "loop_over_task": False,
                "output_data": {},
                "poll_count": 1,
                "queue_wait_time": 63552,
                "rate_limit_frequency_in_seconds": 1,
                "rate_limit_per_frequency": 0,
                "reason_for_incompletion": None,
                "reference_task_name": "celery_test_task_ref",
                "response_timeout_seconds": 3600,
                "retried": False,
                "retried_task_id": None,
                "retry_count": 0,
                "scheduled_time": 1691936100498,
                "seq": 1,
                "start_delay_in_seconds": 0,
                "start_time": 1691936164050,
                "status": "IN_PROGRESS",
                "sub_workflow_id": None,
                "subworkflow_changed": False,
                "task_def_name": "celery_test_task",
                "task_definition": {
                    "backoff_scale_factor": 1,
                    "concurrent_exec_limit": None,
                    "create_time": 1691935582212,
                    "created_by": "",
                    "description": "shipping Workflow",
                    "execution_name_space": None,
                    "input_keys": ["a", "b"],
                    "input_template": {},
                    "isolation_group_id": None,
                    "name": "celery_test_task",
                    "output_keys": [],
                    "owner_app": None,
                    "owner_email": "tomas.correa@gmail.com",
                    "poll_timeout_seconds": None,
                    "rate_limit_frequency_in_seconds": 1,
                    "rate_limit_per_frequency": 0,
                    "response_timeout_seconds": 3600,
                    "retry_count": 3,
                    "retry_delay_seconds": 60,
                    "retry_logic": "FIXED",
                    "timeout_policy": "ALERT_ONLY",
                    "timeout_seconds": 0,
                    "update_time": None,
                    "updated_by": None,
                },
                "task_id": "ca08872c-39e3-11ee-868b-06fd3bd0ae8b",
                "task_type": "celery_test_task",
                "update_time": 1691936164050,
                "worker_id": "6b72840d72b0",
                "workflow_instance_id": "ca0775bb-39e3-11ee-868b-06fd3bd0ae8b",
                "workflow_priority": 0,
                "workflow_task": {
                    "async_complete": False,
                    "case_expression": None,
                    "case_value_param": None,
                    "decision_cases": None,
                    "default_case": None,
                    "default_exclusive_join_task": None,
                    "description": None,
                    "dynamic_fork_join_tasks_param": None,
                    "dynamic_fork_tasks_input_param_name": None,
                    "dynamic_fork_tasks_param": None,
                    "dynamic_task_name_param": None,
                    "evaluator_type": None,
                    "expression": None,
                    "fork_tasks": None,
                    "input_parameters": {
                        "http_request": {
                            "method": "GET",
                            "uri": "https://datausa.io/api/data?drilldowns=Nation&measures=Population",
                        }
                    },
                    "join_on": None,
                    "loop_condition": None,
                    "loop_over": None,
                    "name": "celery_test_task",
                    "optional": False,
                    "rate_limited": None,
                    "retry_count": None,
                    "script_expression": None,
                    "sink": None,
                    "start_delay": 0,
                    "sub_workflow_param": None,
                    "task_definition": {
                        "backoff_scale_factor": 1,
                        "concurrent_exec_limit": None,
                        "create_time": 1691935582212,
                        "created_by": "",
                        "description": "shipping Workflow",
                        "execution_name_space": None,
                        "input_keys": [],
                        "input_template": {},
                        "isolation_group_id": None,
                        "name": "celery_test_task",
                        "output_keys": [],
                        "owner_app": None,
                        "owner_email": "tomas.correa@gmail.com",
                        "poll_timeout_seconds": None,
                        "rate_limit_frequency_in_seconds": 1,
                        "rate_limit_per_frequency": 0,
                        "response_timeout_seconds": 3600,
                        "retry_count": 3,
                        "retry_delay_seconds": 60,
                        "retry_logic": "FIXED",
                        "timeout_policy": "ALERT_ONLY",
                        "timeout_seconds": 0,
                        "update_time": None,
                        "updated_by": None,
                    },
                    "task_reference_name": "celery_test_task_ref",
                    "type": "SIMPLE",
                    "workflow_task_type": None,
                },
                "workflow_type": "celery_test_workflow",
            }
        )

        return response

    return inner
