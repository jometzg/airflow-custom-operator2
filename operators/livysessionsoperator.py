from airflow.models import BaseOperator
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.providers.apache.livy.hooks.livy import LivyHook, BatchState
from apache_airflow_microsoft_fabric_plugin.operators.fabric import FabricHook
import uuid
import time
from enum import Enum
import requests
from typing import Any
from airflow.exceptions import AirflowException

class SessionState(Enum):
    NOT_STARTED = "not_started"
    STARTING = "starting"
    RUNNING = "running"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"

TERMINAL_STATES = {
        SessionState.IDLE,
        SessionState.DEAD,
        SessionState.KILLED,
        SessionState.ERROR,
    }

class CustomSessionLivyOperator(BaseOperator):
    def __init__(self, fabric_conn_id: str, workspace_id: str, item_id: str, command: str, *args, **kwargs):
        super(CustomSessionLivyOperator, self).__init__(*args, **kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.workspace_id = workspace_id
        self.item_id = item_id
        self.command = command
        self._extra_headers = {}
        self.base_url = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{item_id}/livyapi/versions/2023-12-01'
        self.access_token = ""
        
        # Validate required properties
        self._validate_properties()

        # Log initialization
        self.log.info("_init with base_url %s", self.base_url)

        # get access token
        self._add_access_token_to_headers()

    def _validate_properties(self):
        """Validate required properties."""
        if not self.fabric_conn_id:
            raise ValueError("The 'fabric_conn_id' parameter is required.")
        if not self.workspace_id:
            raise ValueError("The 'workspace_id' parameter is required.")
        if not self.item_id:
            raise ValueError("The 'item_id' parameter is required.")
        if not self.command:
            raise ValueError("The 'command' parameter is required.")

    def execute(self, context):
        
        self.log.info(f"Executing LivySesssionOperator: {self.command}")
        retval = ""
        session_id = ""
        try:
           # create a session
           session_id = self._create_session()
           self.log.info("Session created %s", session_id)

           # check session is ready for use
           self._poll_for_session_idle(session_id )
           self.log.info("returned from poll_for_session_idle with session %s", session_id)

           # submit a job to the session
           statement_id = self._submit_statement(session_id, self.command)
           self.log.info("Statement submitted")

           # check the job status
           statement_status = self._poll_for_statement_completed(session_id, statement_id)

           # update the result with the response from the job
           self.log.info("Session %s now %s", session_id, statement_status['state'])
           if statement_status['state'] == "available":
               retval = statement_status['output']['data']
               self.log.info("Statement result %s", retval)

        except Exception as e:
           self.log.error("An error occurred: %s", e)    
           
        finally:   
           # delete the session
           self._delete_session(session_id)

        return retval

    # get access token from Fabric
    def _add_access_token_to_headers(self):
        """Fetch the access token using FabricHook and add it to the headers."""
        self.access_token = FabricHook(fabric_conn_id=self.fabric_conn_id)._get_token()

    # Post to create a session
    def _create_session(self ):
        resp = requests.post(
            f'{self.base_url}/sessions', 
            headers={ "Authorization": f"Bearer {self.access_token}","Accept": "application/json"},
            json = {}
        )
        if resp.status_code >= 200 and resp.status_code < 300:
            jresp = resp.json()
            # get the id from the JSON object returned    
            return jresp["id"]
        else:
            self.log.error("Request failed with status code: %s", resp.status_code)

    # Determine the state of the session
    def _get_state(self, session_id: str ) -> SessionState:
        resp = requests.get(f'{self.base_url}/sessions/{session_id}', headers={ "Authorization": f"Bearer {self.access_token}","Accept": "application/json"})
        jresp = resp.json()
        return SessionState(jresp["state"])

    # Wait for the session to be in idle state. If state is not idle but is terminated then error.
    def _poll_for_session_idle(self, session_id: str): 
        state = self._get_state(session_id)
        while state not in TERMINAL_STATES:
           self.log.info("got %s waiting for poll duration", state)
           time.sleep(60) # maybe think about majking this configurable
           # check the state again       
           state = self._get_state(session_id)
    
        self.log.info("Session %s is now %s", session_id, state)
        if state != SessionState.IDLE:
           raise AirflowException(f"Session is {state.value} not idle")

    # Post a block of code to the Livy Session
    def _submit_statement(self, session_id: str, statement_block: str):
        self.log.info("Submitting %s", statement_block)
        resp = requests.post(
            f'{self.base_url}/sessions/{session_id}/statements', 
            headers={ "Authorization": f"Bearer {self.access_token}","Accept": "application/json"},
            json = {'kind':'spark', 'code':statement_block}
        )
        jresp = resp.json()
        # return the id of the statement    
        return jresp["id"]

    # Determine the state of the session
    def _get_statement_state(self, session_id: str, statement_id: int ) -> SessionState:
        self.log.info("get statement %s state", statement_id)
        resp = requests.get(f'{self.base_url}/sessions/{session_id}/statements/{statement_id}', headers={ "Authorization": f"Bearer {self.access_token}","Accept": "application/json"})
        jresp = resp.json()
        return jresp

    # poll for statement to be ready 
    def _poll_for_statement_completed(self, session_id: str, statement_id: int):    
       resp = self._get_statement_state(session_id, statement_id)
       while resp['state'] == "running":
           self.log.info("got statement %s with %s waiting for poll duration", statement_id, resp['state'])
           time.sleep(60) # maybe think about majking this configurable
           resp = self._get_statement_state(session_id, statement_id)
    
       self.log.info("State now %s", resp['state'])
       if resp['state'] != "available":
           raise AirflowException(f"Statement is {resp['state']}")
       return resp

    # Delete the session
    def _delete_session(self, session_id: str ):
        self.log.info("Deleting Session %s", session_id)
        resp = requests.delete(f'{self.base_url}/sessions/{session_id}', headers={ "Authorization": f"Bearer {self.access_token}","Accept": "application/json"})
