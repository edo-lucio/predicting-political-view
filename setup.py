from setuptools import setup, find_packages

# Automatically parse requirements from requirements.txt
def parse_requirements(filename):
    with open(filename, "r") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="data-collector",
    version="0.1",
    packages=find_packages(where="src"),  # Finds packages in the 'src' directory
    package_dir={"": "src"},
    install_requires=parse_requirements("requirements.txt"),  # Automatically parsed
    entry_points={
        "console_scripts": [
            "collect=data_collection.collect:main",  # Maps the command to the main function
            "score=Moral_Foundation_FrameAxis.scorer:main"
        ],
    },
)
