cp .deploy/pypirc_template ~/.pypirc

sed -i scripts/pum -e "s/\[DEV\]/${TRAVIS_TAG}/g"

sed -i ~/.pypirc -e "s/\[PYPI_USER\]/${PYPI_USER}/g"
sed -i ~/.pypirc -e "s/\[PYPI_PASSWORD\]/${PYPI_PASSWORD}/g"
sed -i setup.py -e "s/\[VERSION\]/${TRAVIS_TAG}/g"

python setup.py sdist upload -r pypi
