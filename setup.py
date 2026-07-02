#!/usr/bin/env python3
"""
Setup script for pulp-tool package.
"""

from setuptools import setup, find_packages
import os


# Read the README file for long description
def read_readme():
    """Read the README file."""
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")
    if os.path.exists(readme_path):
        with open(readme_path, "r", encoding="utf-8") as f:
            return f.read()
    return "Pulp Tool - A Python client for Pulp API operations"


# Dependencies and version are managed in pyproject.toml (setuptools_scm).
# setup.py is provided for pip install compatibility.

setup(
    name="pulp-tool",
    author="Rok Artifact Storage Team",
    author_email="rokartifactstorage@redhat.com",
    description="A Python client for Pulp API operations including RPM, log, and SBOM file management",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/konflux-ci/pulp-tool",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving :: Packaging",
    ],
    python_requires=">=3.12",
    install_requires=[
        "httpx>=0.28.1",
        "pydantic>=2.13.4",
        "click>=8.3.2",
        "python-json-logger>=3.2.1,<5",
    ],
    extras_require={
        "dev": [
            "pytest>=9.0.3",
            "pytest-asyncio>=1.3.0",
            "pytest-cov>=7.1.0",
            "pytest-mock>=3.15.1",
            "hypothesis>=6.131.0",
            "respx>=0.23.1",
            "diff-cover>=10.2.0",
            "black>=26.3.1",
            "flake8>=7.3.0",
            "mypy>=2.0.0",
            "pylint>=4.0.5",
            "pre-commit>=4.6.0",
            "setuptools>=82.0.1",
            "wheel>=0.47.0",
            "setuptools-scm[toml]>=8.0",
            "pip-audit>=2.10.0",
            "pip-tools>=7.4.0",
            "radon>=6.0.1",
        ],
    },
    entry_points={
        "console_scripts": [
            "pulp-tool=pulp_tool.cli:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
