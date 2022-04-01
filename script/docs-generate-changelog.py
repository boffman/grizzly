import sys
import subprocess
import argparse

from os import path, getcwd
from pathlib import Path


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--from-directory',
        type=str,
        default=getcwd(),
        required=False,
    )

    return parser.parse_args()


def main() -> int:
    args = _parse_arguments()

    git_toplevel_dir = subprocess.check_output(
        ['git', 'rev-parse', '--show-toplevel'],
        cwd=args.from_directory,
    ).decode('utf-8').strip()

    github_project_name = path.basename(git_toplevel_dir)
    pypi_project_name = github_project_name.replace('grizzly', 'grizzly-loadtester')

    output = subprocess.check_output(
        ['git', 'tag'],
        cwd=args.from_directory,
    ).decode('utf-8').strip()

    base_directory = Path(path.realpath(path.join(path.dirname(__file__), '..'))) / 'docs' / 'changelog'

    tags = output.split('\n')
    tags.sort(reverse=True)

    if not base_directory.exists():
        base_directory.mkdir()

    with open(base_directory / f'{pypi_project_name}.md', 'w') as fd:
        fd.write('# Changelog\n\n')

        for index, previous_tag in enumerate(tags[1:], start=1):
            current_tag = tags[index - 1]
            print(f'{github_project_name}: generating changelog for {current_tag} <- {previous_tag}')

            output = subprocess.check_output([
                'git',
                'log',
                f"{previous_tag}...{current_tag}",
                '--oneline',
                '--no-abbrev',
                '--no-merges',
            ], cwd=args.from_directory).decode('utf-8').strip()

            fd.write(f'## {current_tag}\n\n')

            for line in output.split('\n'):
                commit = line[:40]
                commit_short = commit[:8]
                message = line[41:].strip()

                fd.write(f'* <a href="https://github.com/Biometria-se/{github_project_name}/commit/{commit}" target="_blank">`{commit_short}`</a>: {message}\n\n')

            fd.write('\n')

    return 0


if __name__ == '__main__':
    sys.exit(main())
