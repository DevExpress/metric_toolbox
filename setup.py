from setuptools import setup, find_packages

with open('readme.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setup(
    name='toolbox',
    version='0.0.1',
    author='Stanislav Shchelokovskiy',
    author_email='stanislav.shchelokovskiy@gmail.com',
    description='Base classes.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/Stanislav-Shchelokovskiy/toolbox',
    license='MIT',
    packages=find_packages(
        exclude=[
            'Tests.*',
            'Tests',
            'setuptools_git >= 0.3',
        ]
    ),
    install_requires=['pandas'],
)
