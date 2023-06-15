#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
# Original repository: https://github.com/StardustDL/aexpy
# Copyright 2022 StardustDL <stardustdl@163.com>
#

import code
import logging
import pathlib

import click

from aexpy.models import ProduceMode
from aexpy.caching import FileProduceCache
from aexpy.services import ServiceProvider

from . import __version__, initializeLogging
from .models import ApiDescription, ApiDifference, Distribution, Release, ReleasePair


def getUnknownDistribution():
    return Distribution(release=Release.fromId("unknown@unknown"))


FLAG_interact = False

services = ServiceProvider()


class AliasedGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        matches = [x for x in self.list_commands(ctx) if x.startswith(cmd_name)]
        if not matches:
            return None
        elif len(matches) == 1:
            return click.Group.get_command(self, ctx, matches[0])
        ctx.fail(f"Too many matches: {', '.join(sorted(matches))}")

    def resolve_command(self, ctx, args):
        # always return the full command name
        _, cmd, args = super().resolve_command(ctx, args)
        assert cmd is not None, "Command is None."
        return cmd.name, cmd, args


@click.group(cls=AliasedGroup)
@click.pass_context
@click.version_option(
    __version__,
    package_name="aexpy",
    prog_name="aexpy",
    message="%(prog)s v%(version)s.",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    default=0,
    type=click.IntRange(0, 5),
    help="Increase verbosity.",
)
@click.option("-i", "--interact", is_flag=True, default=False, help="Interact mode.")
def main(ctx=None, verbose: int = 0, interact: bool = False) -> None:
    """
    AexPy /eɪkspaɪ/ is Api EXplorer in PYthon for detecting API breaking changes in Python packages. (ISSRE'22)

    Home page: https://aexpy.netlify.app/

    Repository: https://github.com/StardustDL/aexpy
    """
    global FLAG_interact
    FLAG_interact = interact

    loggingLevel = {
        0: logging.CRITICAL,
        1: logging.ERROR,
        2: logging.WARNING,
        3: logging.INFO,
        4: logging.DEBUG,
        5: logging.NOTSET,
    }[verbose]

    initializeLogging(loggingLevel)


@main.command()
@click.argument(
    "rootpath",
    type=click.Path(
        exists=True,
        file_okay=False,
        resolve_path=True,
        dir_okay=True,
        path_type=pathlib.Path,
    ),
    required=False,
)
@click.argument("modules", nargs=-1)
@click.option(
    "-r",
    "--release",
    default="unknown@unknown",
    help="Tag the release (project@version).",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(
        exists=False,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    help="Result file.",
    required=True,
)
@click.option("-v", "--view", is_flag=True)
def preprocess(
    rootpath: pathlib.Path,
    modules: "list[str]",
    release: str,
    output: pathlib.Path,
    view: bool,
):
    """Generate a release definition."""
    assert view or (
        rootpath and modules
    ), "Please give the input file or use the view mode."

    mode = ProduceMode.Read if view else ProduceMode.Write

    product = Distribution(release=Release.fromId(release))
    with product.produce(FileProduceCache("", output), mode) as product:
        product.pyversion = "3.11"
        product.rootPath = rootpath
        product.topModules = list(modules)
        product.producer = "aexpy"

    result = product
    print(result.overview())
    if FLAG_interact:
        code.interact(banner="", local=locals())


@main.command()
@click.argument(
    "file",
    type=click.Path(
        exists=True,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    required=False,
)
@click.option(
    "-o",
    "--output",
    type=click.Path(
        exists=False,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    help="Result file.",
    required=True,
)
@click.option("-v", "--view", is_flag=True)
def extract(file: pathlib.Path, output: pathlib.Path, view: bool):
    """Extract the API in a release."""
    assert view or file, "Please give the input file or use the view mode."

    mode = ProduceMode.Read if view else ProduceMode.Write

    product = getUnknownDistribution()
    if not view:
        with product.produce(FileProduceCache("", file), ProduceMode.Read) as product:
            pass

    result = services.extract(FileProduceCache("", output), product, mode)
    print(result.overview())

    if FLAG_interact:
        code.interact(banner="", local=locals())


@main.command()
@click.argument(
    "old",
    type=click.Path(
        exists=True,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    required=False,
)
@click.argument(
    "new",
    type=click.Path(
        exists=True,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    required=False,
)
@click.option(
    "-o",
    "--output",
    type=click.Path(
        exists=False,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    help="Result file.",
    required=True,
)
@click.option("-v", "--view", is_flag=True)
def diff(old: pathlib.Path, new: pathlib.Path, output: pathlib.Path, view: bool):
    """Diff two releases."""
    assert view or (old and new), "Please give the input file or use the view mode."

    mode = ProduceMode.Read if view else ProduceMode.Write

    oldData = (
        services.extract(
            FileProduceCache("", old), getUnknownDistribution(), ProduceMode.Read
        )
        if not view
        else ApiDescription(distribution=getUnknownDistribution())
    )
    newData = (
        services.extract(
            FileProduceCache("", new), getUnknownDistribution(), ProduceMode.Read
        )
        if not view
        else ApiDescription(distribution=getUnknownDistribution())
    )

    result = services.diff(FileProduceCache("", output), oldData, newData, mode)
    print(result.overview())

    if FLAG_interact:
        code.interact(banner="", local=locals())


@main.command()
@click.argument(
    "file",
    type=click.Path(
        exists=True,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    required=False,
)
@click.option(
    "-o",
    "--output",
    type=click.Path(
        exists=False,
        file_okay=True,
        resolve_path=True,
        dir_okay=False,
        path_type=pathlib.Path,
    ),
    help="Result file.",
    required=True,
)
@click.option("-v", "--view", is_flag=True)
def report(file: pathlib.Path, output: pathlib.Path, view: bool):
    """Report breaking changes between two releases."""
    assert view or file, "Please give the input file or use the view mode."

    mode = ProduceMode.Read if view else ProduceMode.Write

    diffData = (
        services.diff(
            FileProduceCache("", file),
            ApiDescription(distribution=getUnknownDistribution()),
            ApiDescription(distribution=getUnknownDistribution()),
            ProduceMode.Read,
        )
        if not view
        else ApiDifference(old=getUnknownDistribution(), new=getUnknownDistribution())
    )

    result = services.report(FileProduceCache("", output), diffData, mode)
    print(result.overview())

    if FLAG_interact:
        code.interact(banner="", local=locals())


if __name__ == "__main__":
    main()
