from airflow.models import BaseOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.apache.livy.hooks.livy import LivyHook, BatchState
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricHook
import uuid
import time
from typing import Any
from airflow.exceptions import AirflowException

class MyCustomOperator(LivyOperator):
    def __init__(self, param1, param2, fabric_conn_id: str, *args, **kwargs):
        super(MyCustomOperator, self).__init__(*args, **kwargs)
        self.param1 = param1
        self.param2 = param2
        self.fabric_conn_id = fabric_conn_id
        self._extra_headers = {}
        self._add_access_token_to_headers()

    def _add_access_token_to_headers(self):
        """Fetch the access token using FabricHook and add it to the headers."""
        access_token = FabricHook(fabric_conn_id=self.fabric_conn_id)._get_token()
        self._extra_headers['Authorization'] = f"Bearer {access_token}"

    @staticmethod
    def _validate_session_ids(session_id: int | str) -> None:
        """Override to validate session IDs as UUID instead of int."""
        try:
            uuid.UUID(session_id, version=4)  # Validate as UUID
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be a uuid")

    def get_batch_state_fabric(self, session_id: int | str, retry_args: dict[str, Any] | None = None) -> BatchState:
        """Override to fetch batch state without using `/state` in the endpoint."""
        self.hook._validate_session_id(session_id)
        self.log.debug("Fetching info for batch session %s", session_id)
        response = self.hook.run_method(
            endpoint=f"{self.hook.endpoint_prefix}/batches/{session_id}",  # URL changed to remove `/state`
            retry_args=retry_args,
            headers=self._extra_headers,
        )
        try:
            response.raise_for_status()
        except Exception as err:
            self.log.warning("Got status code %d for session %s", err.response.status_code, session_id)
            raise AirflowException(
                f"Unable to fetch batch with id: {session_id}. Message: {err.response.text}"
            )
        jresp = response.json()
        self.log.info("Batch with state: %s", jresp["state"])
        if "state" not in jresp:
            raise AirflowException(f"Unable to get state for batch with id: {session_id}")
        return BatchState(jresp["state"])

    def poll_for_termination_fabric(self, batch_id: int | str) -> None:
        """Override to poll for termination without dumping logs."""
        state = self.hook.get_batch_state(batch_id, retry_args=self.retry_args)
        while state not in self.hook.TERMINAL_STATES:
            self.log.debug("Batch with id %s is in state: %s", batch_id, state.value)
            time.sleep(self._polling_interval)
            state = self.hook.get_batch_state(batch_id, retry_args=self.retry_args)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state.value)
        if state != BatchState.SUCCESS:
            raise AirflowException(f"Batch {batch_id} did not succeed")

    def execute(self, context):
        """Override execute to include custom logic."""
        # Example logic using param1 and param2
        result = self.param1 + self.param2
        return result