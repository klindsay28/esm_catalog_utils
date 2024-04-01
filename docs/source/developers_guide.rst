=================
Developer's Guide
=================

Coding Style
------------

Code Formatting
~~~~~~~~~~~~~~~

The code of the package is formatted using the tools `black
<https://black.readthedocs.io/>`_ and `isort <https://pycqa.github.io/isort/>`_.
This ensures that the code across the package has a consistent appearance.

Documentation Strings
~~~~~~~~~~~~~~~~~~~~~

Documentation strings (docstrings) follow the Docstring Standard from the
`numpy Style guide <https://numpydoc.readthedocs.io/en/latest/format.html>`_.
This standard describes how the content of docstrings is organized.
Docstring are written using `reStructuredText
<http://docutils.sourceforge.net/rst.html>`_ markup syntax and are rendered
into documentation using `Sphinx <https://www.sphinx-doc.org/>`_.

Function Annotations/Type Hints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Functions annotations <https://peps.python.org/pep-3107/>`_ are used to
document the types of function’s parameters and return values.
This enables users of the package to use external tools like `mypy
<https://mypy.readthedocs.io/en/stable/>`_ to help ensure that they're
using the package properly.
Python's `typing module <https://peps.python.org/pep-0484/>`_ is used to
support the annotations.

Testing
-------

Testing is performed with continuous integration using `github actions
<https://github.com/features/actions>`_.
Testing is performed with python versions 3.8 through 3.12.
Testing consists of the following:

- Run the source code through `black <https://black.readthedocs.io/>`_ and
  `isort <https://pycqa.github.io/isort/>`_ to verify that the desired code
  formatting is adhered to.
- Run the source code through `flake8 <https://flake8.pycqa.org/>`_, which
  analyzes the code and detects various errors.
- Run the source code through `mypy
  <https://mypy.readthedocs.io/en/stable/>`_, to ensure that variable types
  are used appropriately throughout the package.
- Run unit tests, located in the `tests` subdirectory. The unit tests include
  creating catalogs from internally generated input files and verifying that
  the generated catalogs match baseline catalogs that are included in the
  repository.
