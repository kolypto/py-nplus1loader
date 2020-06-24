import nox.sessions


nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ['tests', 'tests_sqlalchemy']


@nox.session(python=['3.6', '3.7', '3.8'])
def tests(session: nox.sessions.Session, sqlalchemy=None):
    """ Run the full tests suite """

    session.install('.', '-r', 'requirements-dev.txt')
    if sqlalchemy:
        session.install(f'sqlalchemy=={sqlalchemy}')

    session.run('nosetests', 'tests/')


@nox.session(python='3.8')
@nox.parametrize(
    'sqlalchemy',
    [
        '1.2.19',
        *(f'1.3.{x}'
          for x in range(0, 1+17)),
    ]
)
def tests_sqlalchemy(session: nox.sessions.Session, sqlalchemy):
    """ Test against a specific SqlAlchemy version """
    tests(session, sqlalchemy)


@nox.session
def build(session: nox.sessions.Session):
    """ Build the package """
    session.run('python', 'setup.py', 'build', 'sdist', 'bdist_wheel')
