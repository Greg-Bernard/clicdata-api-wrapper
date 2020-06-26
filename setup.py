import setuptools

VERSION = '0.0.1'

def readme():
    with open("README.md", "r") as fh:
        return fh.read()

def main():
    setuptools.setup(
        name="clicdata-api-wrapper-Greg-Bernard", # Replace with your own username
        version=VERSION,
        author="Greg Bernard",
        author_email="greg.t.bernard@gmail.com",
        description="A ClicData API wrapper for use in Jupyter notebooks.",
        long_description=readme(),
        long_description_content_type="text/markdown",
        url="https://github.com/Greg-Bernard/clicdata-api-wrapper",
        packages=setuptools.find_packages(),
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        python_requires='>=3.6',
        install_requires=[
            'pandas', 
            'requests'
        ],
        include_package_data=True,
        license='MIT'
    )

if __name__ == '__main__':
    main()