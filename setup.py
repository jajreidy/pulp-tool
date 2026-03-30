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


# Dependencies are managed in pyproject.toml
# setup.py is provided for pip install compatibility

setup(
    name="pulp-tool",
    version="1.0.0",
    author="Rok Artifact Storage Team",
    author_email="rokartifactstorage@redhat.com",
    description="A Python client for Pulp API operations including RPM, log, and SBOM file management",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/konflux/pulp-tool",
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
    # Dependencies are defined in pyproject.toml
    # Duplicated here for pip install compatibility
    install_requires=[
        "httpx>=0.27.0",
        "pydantic>=2.10.0",
        "click>=8.1.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0",
            "pytest-asyncio>=0.24.0",
            "pytest-cov>=6.0",
            "pytest-mock>=3.14.0",
            "respx>=0.22.0",
            "diff-cover>=7.0",
            "black>=25.1.0",
            "flake8>=7.0",
            "mypy>=1.11.0",
            "pylint>=3.3.0",
            "pre-commit>=4.0.0",
            "setuptools>=69.0",
            "wheel",
            "setuptools-scm[toml]>=8.0",
            "pip-audit>=2.7.0",
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
