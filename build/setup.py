from distutils.core import setup

setup(name='Single connection RPC',
      version='VERSION',
      description='An RPC server/client pair that communicates over a single, persistent TCP connection.',
      author='Mads D. Kristensen',
      author_email='madsk@cs.au.dk',
      url='https://github.com/madsdk/python-single-connection-RPC',
      packages=['scrpc']
)
