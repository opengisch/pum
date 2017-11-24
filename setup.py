from setuptools import setup
import sys
if sys.version_info < (3,3):
    sys.exit('Pum requires at least Python version 3.3.\nYou are currently running this installation with\n\n{}'.format(sys.version))

setup(
    name = 'pum',
    packages = [
        'pum',
        'pum/core',
        'pum/utils'
    ],
    scripts = [
        'scripts/pum'
    ],
    version = '[VERSION]',
    description = 'Postgres upgrade manager',
    author = 'Mario Baranzini',
    author_email = 'mario@opengis.ch',
    url = 'https://github.com/opengisch/pum',
    download_url = 'https://github.com/opengisch/pum/archive/[VERSION].tar.gz', # I'll explain this in a second
    keywords = [
        'postgres',
        'migration',
        'upgrade'
    ],
    classifiers = [
        'Topic :: Database',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Information Technology',
        'Topic :: Software Development :: Version Control',
        'Development Status :: 5 - Production/Stable'
    ],
    install_requires = [
        'psycopg2>=2.7.3',
        'PyYAML'
    ],
    python_requires=">=3.3",
)
