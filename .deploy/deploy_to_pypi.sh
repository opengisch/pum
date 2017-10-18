cp .deploy/pypirc_template ~/.pypirc
sed -i ~/.pypirc -e "s/\[PYPI_USER\]/${PYPI_USER}/g"
sed -i ~/.pypirc -e "s/\[PYPI_PASSWORD\]/${PYPI_PASSWORD}/g"

python setup.py sdist upload -r pypi
