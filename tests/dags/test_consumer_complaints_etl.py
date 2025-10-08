"""
Unit tests for the consumer_complaints_etl DAG.

These tests verify the DAG structure, task dependencies, and basic functionality.
"""

import os
import sys
from datetime import datetime, timedelta

import pytest
from airflow.models import DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

# Add include directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "include"))


class TestConsumerComplaintsETLDAG:
    """Test suite for consumer_complaints_etl DAG."""

    @pytest.fixture(scope="class")
    def dagbag(self):
        """Load the DAG bag."""
        return DagBag(dag_folder="dags/", include_examples=False)

    @pytest.fixture(scope="class")
    def dag(self, dagbag):
        """Get the consumer_complaints_etl DAG."""
        dag_id = "consumer_complaints_etl"
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found in DagBag"
        return dagbag.dags[dag_id]

    def test_dag_loaded(self, dag):
        """Test that the DAG is loaded correctly."""
        assert dag is not None
        assert dag.dag_id == "consumer_complaints_etl"

    def test_dag_has_no_import_errors(self, dagbag):
        """Test that there are no import errors in the DAG."""
        assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

    def test_dag_properties(self, dag):
        """Test DAG configuration properties."""
        assert dag.schedule_interval == "@daily"
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert "etl" in dag.tags
        assert "cfpb" in dag.tags
        assert "snowflake" in dag.tags

    def test_dag_default_args(self, dag):
        """Test DAG default arguments."""
        default_args = dag.default_args
        assert default_args["owner"] == "data_engineering"
        assert default_args["retries"] == 3
        assert default_args["retry_delay"] == 300

    def test_dag_tasks_exist(self, dag):
        """Test that all expected tasks exist in the DAG."""
        expected_tasks = [
            "extract_complaints",
            "transform_complaints",
            "create_snowflake_table",
            "load_to_snowflake",
            "validate_data_quality",
        ]

        task_ids = [task.task_id for task in dag.tasks]

        for expected_task in expected_tasks:
            assert expected_task in task_ids, f"Task {expected_task} not found in DAG"

    def test_task_count(self, dag):
        """Test that the DAG has the correct number of tasks."""
        assert len(dag.tasks) == 5, f"Expected 5 tasks, found {len(dag.tasks)}"

    def test_task_dependencies(self, dag):
        """Test task dependencies are correctly set."""
        # Get tasks
        extract_task = dag.get_task("extract_complaints")
        transform_task = dag.get_task("transform_complaints")
        create_table_task = dag.get_task("create_snowflake_table")
        load_task = dag.get_task("load_to_snowflake")
        validate_task = dag.get_task("validate_data_quality")

        # Check upstream dependencies
        assert (
            len(extract_task.upstream_task_ids) == 0
        ), "extract_complaints should have no upstream tasks"
        assert "extract_complaints" in transform_task.upstream_task_ids
        assert (
            len(create_table_task.upstream_task_ids) == 0
        ), "create_snowflake_table should have no upstream tasks"
        assert "transform_complaints" in load_task.upstream_task_ids
        assert "create_snowflake_table" in load_task.upstream_task_ids
        assert "load_to_snowflake" in validate_task.upstream_task_ids

    def test_task_retry_configuration(self, dag):
        """Test that tasks have retry configuration."""
        for task in dag.tasks:
            # Tasks should inherit default_args
            assert (
                task.retries == 3 or task.retries is None
            ), f"Task {task.task_id} has unexpected retry count"


class TestCFPBAPIClient:
    """Test suite for CFPB API client."""

    def test_api_client_initialization(self):
        """Test that the API client can be initialized."""
        from cfpb_api_client import CFPBAPIClient

        client = CFPBAPIClient()
        assert client is not None
        assert (
            client.BASE_URL
            == "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
        )
        assert client.session is not None
        client.close()

    def test_api_client_has_required_methods(self):
        """Test that the API client has all required methods."""
        from cfpb_api_client import CFPBAPIClient

        client = CFPBAPIClient()

        assert hasattr(client, "get_complaints")
        assert hasattr(client, "get_complaints_paginated")
        assert hasattr(client, "get_complaints_for_date_range")
        assert hasattr(client, "get_complaints_last_n_days")
        assert hasattr(client, "close")

        client.close()


class TestSnowflakeLoader:
    """Test suite for Snowflake loader."""

    def test_snowflake_loader_initialization(self):
        """Test that the Snowflake loader requires a hook."""
        from snowflake_loader import SnowflakeLoader

        # Should work with any object (we're not connecting)
        class MockHook:
            pass

        loader = SnowflakeLoader(MockHook())
        assert loader is not None
        assert loader.hook is not None

    def test_snowflake_loader_has_required_methods(self):
        """Test that the Snowflake loader has all required methods."""
        from snowflake_loader import SnowflakeLoader

        class MockHook:
            pass

        loader = SnowflakeLoader(MockHook())

        assert hasattr(loader, "create_table_if_not_exists")
        assert hasattr(loader, "load_dataframe")
        assert hasattr(loader, "load_json_data")
        assert hasattr(loader, "execute_query")
        assert hasattr(loader, "truncate_table")
        assert hasattr(loader, "get_table_row_count")


class TestDataTransformation:
    """Test suite for data transformation logic."""

    def test_transform_empty_list(self):
        """Test transformation of empty complaint list."""
        # This would require importing the task function
        # For now, this is a placeholder for future implementation
        pass

    def test_transform_valid_complaints(self):
        """Test transformation of valid complaints."""
        # This would test the actual transformation logic
        # Placeholder for future implementation
        pass

    def test_transform_handles_missing_fields(self):
        """Test that transformation handles missing fields gracefully."""
        # Placeholder for future implementation
        pass


class TestDAGIntegrity:
    """Test DAG integrity and structure."""

    def test_dag_has_documentation(self, dagbag):
        """Test that the DAG has documentation."""
        dag = dagbag.dags.get("consumer_complaints_etl")
        assert dag.doc_md is not None
        assert len(dag.doc_md) > 0
        assert "CFPB" in dag.doc_md
        assert "Snowflake" in dag.doc_md

    def test_all_tasks_have_descriptions(self, dagbag):
        """Test that all tasks have docstrings."""
        dag = dagbag.dags.get("consumer_complaints_etl")

        for task in dag.tasks:
            # TaskFlow tasks should have documentation
            assert (
                task.doc is not None or task.doc_md is not None or task.doc_rst is not None
            ), f"Task {task.task_id} should have documentation"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
