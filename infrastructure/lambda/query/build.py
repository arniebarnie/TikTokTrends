import subprocess
import shutil
import os

def build_lambda_package():
    # Create a temporary build directory
    if os.path.exists('build'):
        shutil.rmtree('build')
    os.makedirs('build')

    # Install dependencies into the build directory
    subprocess.check_call([
        'pip', 'install',
        '--platform', 'manylinux2014_x86_64',
        '--implementation', 'cp',
        '--python', '3.9',
        '--only-binary=:all:',
        '--target', 'build',
        '-r', 'requirements.txt'
    ])

    # Copy the Lambda function code
    shutil.copy('main.py', 'build/main.py')

    # Create a ZIP file
    shutil.make_archive('lambda_function', 'zip', 'build')

if __name__ == '__main__':
    build_lambda_package() 