import json
import logging
from typing import List

import pydantic_core
import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url, buffer_size=10):
        self.api_base_url = api_base_url
        self.buffer_size = buffer_size
        self.buffer = []

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
            # Add the batch of data to the buffer
            self.buffer.extend(processed_agent_data_batch)

            # Check if the buffer size has been reached
            if len(self.buffer) >= self.buffer_size:
                # If buffer size is reached, send the data
                success = self.send_data(self.buffer)
                # Clear the buffer after sending
                self.buffer.clear()
                return success
            else:
                return True
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            return False

    def __processed_agent_data_list_to_list_of_dict(
        self, processed_agent_data_list: List[ProcessedAgentData]
    ):
        """
        Convert a list of ProcessedAgentData objects into a list of dictionaries.
        Parameters:
            processed_agent_data_list (List[ProcessedAgentData]): List of ProcessedAgentData objects.
        Returns:
            List[dict]: List of dictionaries representing the processed agent data.
        """
        processed_data = []
        for data in processed_agent_data_list:
            # Convert each ProcessedAgentData object to a dictionary and append to the list
            processed_data.append(data.dict())

        # Convert the timestamp of each agent data to ISO format
        for data in processed_data:
            data["agent_data"]["timestamp"] = data["agent_data"][
                "timestamp"
            ].isoformat()

        return processed_data

    def send_data(self, data: List[ProcessedAgentData]) -> bool:
        """
        Send the processed agent data to the Store API.
        Parameters:
            data (List[ProcessedAgentData]): List of processed agent data.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        # Construct the URL for sending data
        url = f"{self.api_base_url}/processed_agent_data/"
        try:
            # Convert the data to a list of dictionaries
            json_data = self.__processed_agent_data_list_to_list_of_dict(data)
            # Convert the JSON data to a string
            json_data_str = str(json.dumps(json_data, indent=4))

            # Send a POST request to the API endpoint with JSON data
            response = requests.post(
                url, data=json_data_str, headers={"Content-Type": "application/json"}
            )

            # Check if the response is successful
            if response.status_code >= 200 and response.status_code < 300:
                return True
            else:
                # Log if the request fails
                logging.error(
                    f"Failed to save data. Status code: {response.status_code}, URL: {url}"
                )
                return False
        except Exception as e:
            # Log and handle exceptions
            logging.error(f"Error occurred: {e}")
            return False
