import nox.sessions


PYTHON_VERSIONS = ['3.6', '3.7', '3.8', '3.9']
SQLALCHEMY_VERIONS = [
    '1.2.19',
    *(f'1.3.{x}'
      for x in range(0, 1 + 20)),
]


nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ['tests', 'tests_sqlalchemy']


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.sessions.Session, sqlalchemy=None):
    """ Run the full tests suite """
    session.install('poetry')
    session.run('poetry', 'install')

    if sqlalchemy:
        session.install(f'sqlalchemy=={sqlalchemy}')

    session.run('pytest', 'tests/')


@nox.session(python='3.8')
@nox.parametrize('sqlalchemy', SQLALCHEMY_VERIONS)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, sqlalchemy)
