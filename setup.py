#!/usr/bin/env python3
"""
Setup script for pulp-tool package.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    """Read the README file."""
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Pulp Tool - A Python client for Pulp API operations"

# Read requirements from requirements.txt
def read_requirements():
    """Read requirements from requirements.txt."""
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [
        'requests>=2.25.0',
        'tomli>=2.0.0; python_version < "3.11"',
    ]

setup(
    name="pulp-tool",
    version="1.0.0",
    author="Konflux Team",
    author_email="konflux@redhat.com",
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving :: Packaging",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.0",
            "black>=21.0",
            "flake8>=3.8",
            "mypy>=0.800",
            "pylint>=2.8",
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
