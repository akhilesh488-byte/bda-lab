from setuptools import setup

setup(
    name="bda-lab",
    version="0.1.0",
    description="Big Data Laboratory Manual - Reference Code & Exam Practice",
    py_modules=["lab"],
    entry_points={
        "console_scripts": [
            "bda-lab=lab:main",
        ],
    },
    python_requires=">=3.6",
)
