from setuptools import setup, find_packages

setup(
    name="spawn",
    version="0.1.0",
    description="Static Portal Automatic web indexer",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="SPAwn Team",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyyaml>=6.0",
        "click>=8.0.0",
        "python-dotenv>=0.19.0",
        "requests>=2.25.0",
        "jinja2>=3.0.0",
        "tqdm>=4.62.0",
        "gitpython>=3.1.0",
        "globus-sdk>=3.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "black>=21.5b2",
            "isort>=5.9.0",
            "flake8>=3.9.0",
            "mypy>=0.812",
        ],
        "compute": [
            "globus-compute-sdk>=2.0.0",
        ],
        "flow": [
            "globus-automate-client>=0.15.0",
        ],
    },
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "black>=21.5b2",
            "isort>=5.9.0",
            "flake8>=3.9.0",
            "mypy>=0.812",
        ],
    },
    entry_points={
        "console_scripts": [
            "spawn=spawn.cli:main",
        ],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)