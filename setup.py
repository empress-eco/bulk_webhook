from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in bulkwebhook/__init__.py
from bulkwebhook import __version__ as version

setup(
	name="bulkwebhook",
	version=version,
	description="Bulk Webhook",
	author="Aakvatech",
	author_email="info@aakvatech.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
