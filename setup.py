from distutils.core import setup
setup(
    name = 'pum',
    packages = ['pum', 'pum/core', 'pum/utils'],
    scripts = ['scripts/pum'],
    version = '[VERSION]',
    description = 'Postgres upgrade manager',
    author = 'Mario Baranzini',
    author_email = 'mario@opengis.ch',
    url = 'https://github.com/opengisch/pum',
    download_url = 'https://github.com/opengisch/pum/archive/[VERSION].tar.gz', # I'll explain this in a second
    keywords = ['postgres', 'migration', 'upgrade'],
    classifiers = [],
)
