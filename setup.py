from setuptools import setup


def read_requirements():
    with open("requirements.txt") as f:
        return [i.strip() for i in f.readlines()]


setup(
    name="hts_neut_launcher",
    license="MIT",
    python_requires=">=3.6",
    install_requires=read_requirements(),
    zip_safe=True,
    packages=["launcher"],
)
