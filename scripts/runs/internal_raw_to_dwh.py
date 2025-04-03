import json
import logging
import sys
import time
import services.pipelines.internal_raw_to_dwh as irs_to_dwh


def main(env, runner, logs_level, params):
    # Validate the 'env' parameter
    valid_envs = {"DEV", "TEST", "PROD"}
    if env not in valid_envs:
        raise ValueError(f"Invalid value for 'env': {env}. Must be one of {valid_envs}.")

    # Set up logging level
    log_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    log_level = log_levels.get(logs_level, None)
    if log_level is None:
        raise ValueError(f"Invalid value for 'logs_level': {logs_level}. Must be one of {list(log_levels.keys())}.")

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        handlers=[logging.StreamHandler()]
    )
    logging.Formatter.converter = time.gmtime
    logging.captureWarnings(True)
    logger = logging.getLogger(__name__)

    # Set debug flag
    debug = logs_level == "DEBUG"

    # Set the warning logging level for elasticsearch
    elasticsearch_logger = logging.getLogger("elasticsearch")
    elasticsearch_logger.setLevel(logging.WARNING)

    # Check if the runner exists in the module's __all__
    if runner not in irs_to_dwh.__all__:
        raise ValueError(
            f"Runner '{runner}' not found in the module 'services.pipelines.internal_raw_to_dwh'. "
            f"Available runners: {irs_to_dwh.__all__}"
        )

    # Dynamically invoke the runner
    try:
        # Retrieve the runner by its name
        runner_to_call = getattr(irs_to_dwh, runner)
        logger.info(f"Calling runner '{runner}' with params: {params}")
        # Pass params as keyword arguments (**kwargs)
        result = runner_to_call(env=env, debug=debug, **params)
        logger.info(f"Runner '{runner}' executed successfully.")
        return result
    except Exception as e:
        logger.error(f"Error while executing runner '{runner}': {e}")
        raise


if __name__ == "__main__":
    try:
        # Ensure that the script received JSON input as a command-line argument
        if len(sys.argv) < 2:
            raise ValueError("JSON input is required as an argument.")

        # Parse the JSON input
        with open(sys.argv[1], "r") as f:
            input_data = json.load(f)

        # Extract parameters from the JSON input
        env = input_data.get("env")  # Environment (e.g., DEV, TEST, PROD)
        runner = input_data.get("runner")  # Name of the runner to call
        logs_level = input_data.get("logs_level", "INFO")  # Logging level (default: INFO)
        params = input_data.get("params", {})  # Additional parameters for the runner

        # Execute the main runner
        output = main(env=env, runner=runner, logs_level=logs_level, params=params)

        # Print success response as JSON
        print(json.dumps({"status": "success", "result": output}))
    except Exception as e:
        # Print error response as JSON
        print(json.dumps({"status": "error", "message": str(e)}))
        sys.exit(1)
