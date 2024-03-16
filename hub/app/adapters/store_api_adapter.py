import json
import logging
from typing import List
import requests
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url, buffer_size=10):
        self.api_base_url = api_base_url # API base URL
        self.buffer_size = buffer_size # Buffer size
        self.buffer: List[ProcessedAgentData] = [] # Buffer size

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch (dict): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        # Implement it
        try:
            # Add batch data to buffer
            self.buffer.extend(processed_agent_data_batch)
            if len(self.buffer) >= self.buffer_size:
                success = self.send_data(self.buffer) # If buffer size is reached, send data
                # Clear the buffer after sending
                self.buffer.clear()
                return success
            else:
                return True
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            return False

    def processed_agent_data_list_to_list_of_dict(self, data_list: List[ProcessedAgentData]):
        """
        Convert a list of ProcessedAgentData to a list of dictionaries.
        """
        processed_data = []
        for data in data_list:
            # Convert ProcessedAgentData to dictionary
            processed_data.append(data.model_dump())

        for data in processed_data:
            # Serialize timestamp to ISO format
            data['agent_data']['timestamp'] = data['agent_data']['timestamp'].isoformat()

        return processed_data
    
    def send_data(self, data: List[ProcessedAgentData]) -> bool:
        """
        Send the accumulated data to the Store API.
        """
        url = f"{self.api_base_url}/processed_agent_data/"
        try:
            json_data = self.processed_agent_data_list_to_list_of_dict(data)
            # Send POST request to API
            response = requests.post(url, json=json_data)
            if response.ok: # Check if request is successful
                return True
            else:
                logging.error(f"Failed to save data. Status code: {response.status_code}")
                return False
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            return False