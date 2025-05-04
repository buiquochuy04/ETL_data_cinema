from setuptools import find_packages, setup

setup(
    name="ETL_cinema",
    packages=find_packages(exclude=["ETL_cinema_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
