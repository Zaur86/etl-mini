#!/usr/bin/env PYTHONPATH=. python

import os
import shutil
import argparse
from app.errors import TemplateNotFoundError

TEMPLATE_DIR = "tests/templates"
OUTPUT_DIR = "tests/cases"


def generate_config(test_path):
    """
    Generates a configuration file for a given test from a template if it doesn't already exist.

    Args:
        test_path (str): Path to the test file (e.g., tests/cases/test_case_1.py).
    """
    # Ensure the test file exists
    if not os.path.isfile(test_path):
        raise FileNotFoundError(f"Test file not found: {test_path}")

    # Determine the configuration file path
    test_dir = os.path.dirname(test_path)
    test_name = os.path.basename(test_path).replace(".py", "_config.yaml")
    config_path = os.path.join(test_dir, test_name)

    # Skip generation if the configuration file already exists
    if os.path.isfile(config_path):
        print(f"Configuration already exists: {config_path}")
        return

    # Locate the corresponding template, preserving the relative path
    relative_path = os.path.relpath(test_dir, OUTPUT_DIR)
    template_name = os.path.basename(test_path).replace(".py", "_config_template.yaml")
    template_path = os.path.join(TEMPLATE_DIR, relative_path, template_name)

    # Raise an error if the template file is missing
    if not os.path.isfile(template_path):
        raise TemplateNotFoundError(template_path)

    # Copy the template to the target configuration path
    # The template serves as a starting point for the test configuration
    os.makedirs(test_dir, exist_ok=True)
    shutil.copy(template_path, config_path)
    print(f"Generated configuration file from template: {config_path}")


if __name__ == "__main__":
    # Command-line interface
    parser = argparse.ArgumentParser(description="Generate a test configuration file from a template.")
    parser.add_argument("test_path", help="Path to the test file (e.g., tests/cases/test_case_1.py)")
    args = parser.parse_args()

    try:
        generate_config(args.test_path)
    except (FileNotFoundError, TemplateNotFoundError) as e:
        print(f"Error: {e}")
