import zipfile
from pathlib import Path

import pytest

from ploomber.cloud import pkg


@pytest.fixture
def sample_project():
    Path('a').touch()
    Path('b').mkdir()
    Path('b', 'b1').touch()
    Path('c', 'c1').mkdir(parents=True)
    Path('c', 'c1', 'c2').touch()


def test_zip_project(tmp_directory, sample_project):
    pkg.zip_project()

    with zipfile.ZipFile('project.zip') as zip:
        files = zip.namelist()

    assert set(files) == {'a', 'c/', 'b/', 'c/c1/', 'c/c1/c2', 'b/b1'}


def test_get_presigned_link():
    pass
