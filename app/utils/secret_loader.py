import yaml
import os


class SecretLoader:
    """Utility for securely loading API tokens from config/secrets/api_tokens.yaml."""

    @staticmethod
    def load_token(service_name: str):
        """Loads API token for a given service from a YAML file."""
        secret_path = "config/secrets/api_tokens.yaml"

        if not os.path.exists(secret_path):
            raise FileNotFoundError(f"Secrets file not found: {secret_path}")

        with open(secret_path, "r") as file:
            secrets = yaml.safe_load(file) or {}

        if service_name not in secrets or "token" not in secrets[service_name]:
            raise ValueError(f"Token for {service_name} not found in secrets file")

        return secrets[service_name]["token"]
