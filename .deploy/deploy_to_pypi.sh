cp .deploy/pypirc_template ~/.pypirc

sed -i pum/__main__.py -e "s/\[DEV\]/${GIT_TAG_NAME}/g"

sed -i ~/.pypirc -e "s/\[PYPI_USER\]/${PYPI_USER}/g"
sed -i ~/.pypirc -e "s/\[PYPI_PASSWORD\]/${PYPI_PASSWORD}/g"
sed -i setup.py -e "s/\[VERSION\]/${GIT_TAG_NAME}/g"

python setup.py sdist upload -r pypi
