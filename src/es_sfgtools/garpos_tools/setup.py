from pathlib import Path
import subprocess
import platform
from setuptools import setup, Extension,find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.install import install 
import numpy 
from distutils.dist import Distribution
from setuptools.dist import Distribution as Distribution
import shutil
import tempfile
import glob 

class CustomBuildCommand(build_ext):

    # def __init__(self,**kw) -> None:
    #     super().__init__(dist=Distribution())
    """Custom build command to clone the repository and compile Fortran code."""

    description = "Clone the repository and compile Fortran code"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def find_dir(self,base_path,dir_name):
        for path in base_path.rglob('*'):
            if path.is_dir() and path.name == dir_name:
                return path
        return None

    def install_gfortran(self):
        try:
            # Check if gfortran is already installed
            subprocess.run(
                ["gfortran", "--version"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("gfortran is already installed.")
        except subprocess.CalledProcessError:
            # Determine the operating system
            os_name = platform.system()

            match os_name:

                case "Darwin":  # macOS
                    print("Installing gfortran on macOS using Homebrew...")
                    subprocess.run(["brew", "install", "gfortran"], check=True)
                case "Linux":
                    # Determine the Linux distribution
                    distro = platform.linux_distribution()[0].lower()
                    if "ubuntu" in distro or "debian" in distro:
                        print("Installing gfortran on Ubuntu/Debian...")
                        subprocess.run(["sudo", "apt-get", "update"], check=True)
                        subprocess.run(
                            ["sudo", "apt-get", "install", "-y", "gfortran"], check=True
                        )
                    elif "centos" in distro or "fedora" in distro or "redhat" in distro:
                        print("Installing gfortran on CentOS/Fedora/RedHat...")
                        subprocess.run(
                            ["sudo", "yum", "install", "-y", "gcc-gfortran"], check=True
                        )
                    else:
                        print(f"Unsupported Linux distribution: {distro}")

                case _:
                    print(f"Unsupported operating system: {os_name}")

    def run(self):
        # Check if gfortran is installed
        self.install_gfortran()

        # Define the target directory for cloning
        target_dir = Path(__file__).parent/"garpos"

        build_command = "gfortran -shared -fPIC -fopenmp -O3 -o lib_raytrace.so sub_raytrace.f90 lib_raytrace.f90"
        # Clone the repository
        repo_url = "https://github.com/s-watanabe-jhod/garpos.git"
        if not target_dir.exists():
            out = subprocess.run(["git", "clone", repo_url,str(target_dir)])

        # get the path of the directory that is named "f90lib"

        fortran_source = self.find_dir(target_dir,"f90lib")
        if fortran_source is None:
            raise FileNotFoundError("Directory 'f90lib' not found")

        # Compile the Fortran code
        build_command = "gfortran -shared -fPIC -fopenmp -O3 -o lib_raytrace.so sub_raytrace.f90 lib_raytrace.f90"
        subprocess.run(build_command.split(), cwd=fortran_source)

class CustomInstallCommand(install):
    def run(self):
        builder = CustomBuildCommand(self.distribution)
        builder.run()

        parent_dir = Path(__file__).parent

        build_dirs = ['build','dist','*.egg-info']
        for build_dir in build_dirs:
            for path in glob.glob(str(parent_dir / build_dir)):
                print(f"Removing {path}")
                shutil.rmtree(path, ignore_errors=True)

# setup(
#     name="garpos",
#     version="0.1",
#     # packages=find_packages(),
#     cmdclass = {
#         "install": CustomBuildCommand,
#     },
# )
setup(
    name="garpos",
    version="0.1",
    # packages=find_packages(),
    cmdclass={
        # "build_ext": CustomBuildCommand,
        "build_ext":CustomInstallCommand
    },
    package_data={
        "": ["f90lib/*"],  # Include all files in the f90lib subdirectory
    },
    include_package_data=True,
    # ext_modules=[
    #     Extension(
    #         "garpos",
    #         sources=["src/garpos.f90"],
    #         include_dirs=[numpy.get_include()],
    #     ),
    # ],
)

# if __name__ == "__main__":
#     dist = Distribution()

#     cbc = CustomBuildCommand(dist)

#     cbc.run()
#     from garpos.bin.garpos_v102.garpos_main import drive_garpos
