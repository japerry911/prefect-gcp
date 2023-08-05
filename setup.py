from setuptools import find_packages, setup

import versioneer

with open("requirements.txt") as install_requires_file:
    install_requires = install_requires_file.read().strip().split("\n")

with open("requirements-dev.txt") as dev_requires_file:
    dev_requires = dev_requires_file.read().strip().split("\n")

with open("README.md") as readme_file:
    readme = readme_file.read()

extras_require = {
    "cloud_storage": ["google-cloud-storage"],
    "bigquery": ["google-cloud-bigquery", "google-cloud-bigquery-storage"],
    "secret_manager": ["google-cloud-secret-manager"],
    "aiplatform": ["google-cloud-aiplatform"],
}
extras_require["all_extras"] = sorted(
    {lib for key in extras_require.values() for lib in key}
)
extras_require["dev"] = dev_requires + extras_require["all_extras"]

setup(
    name="prefect-gcp",
    description="Prefect tasks and subflows for interacting with Google Cloud Platform.",
    license="Apache License 2.0",
    author="Prefect Technologies, Inc.",
    author_email="help@prefect.io",
    keywords="prefect",
    url="https://github.com/PrefectHQ/prefect-gcp",
    long_description=readme,
    long_description_content_type="text/markdown",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require=extras_require,
    entry_points={
        "prefect.collections": [
            "prefect_gcp = prefect_gcp",
            "prefect_gcp_worker_v2_test = prefect_gcp.worker_v2",
        ]
    },
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
    ],
)
