from services.sources.base import ExternalSourceService
from app.utils import extract_placeholders
from dataclasses import dataclass, field
import yaml
import requests
import logging


@dataclass
class SimpleAPIService(ExternalSourceService):
    """
    Service for interacting with external APIs that return data instantly upon request.
    Loads API request templates from config/api_templates.yaml.
    """
    template_key: str
    params: dict = field(default_factory=dict)
    template: dict = field(init=False)

    def __post_init__(self):
        """
        Loads API template from configuration file after initialization.
        Validates required parameters dynamically based on the template.
        """
        with open("config/api_templates.yaml", "r") as file:
            templates = yaml.safe_load(file)
        if self.template_key not in templates:
            raise ValueError(f"Template '{self.template_key}' not found in configuration.")
        self.template = templates[self.template_key]
        required_keys = extract_placeholders(self.template["url"])
        required_keys += extract_placeholders(self.template["headers"])
        required_keys += extract_placeholders(self.template["url"])
        if "body" in self.template:
            required_keys += extract_placeholders(self.template["body"])
        if not all(key in self.params for key in required_keys):
            raise ValueError(f"Missing required parameters: {set(required_keys) - set(self.params.keys())}")

    def extract(self):
        """
        Constructs the API request using the loaded template and retrieves data.
        """
        url = self.template["url"].format(**self.params)
        headers = {k: v.format(**self.params) for k, v in self.template["headers"].items()}
        method = self.template.get("method", "GET").upper()
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            body = self.template.get("body", {})
            if isinstance(body, dict):
                formatted_body = {k: v.format(**self.params) for k, v in body.items()}
            else:
                formatted_body = body
            response = requests.post(url, headers=headers, json=formatted_body)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        response.raise_for_status()
        if "application/json" in response.headers.get("Content-Type", ""):
            logging.info("Fetched data format: JSON")
            return response.json()
        else:
            logging.info("Fetched data format: CONTENT")
            return response.content
